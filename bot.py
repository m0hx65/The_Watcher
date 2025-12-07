"""
Single-file Telegram profile watcher bot.
Tech: aiogram v3, httpx, SQLAlchemy (async, SQLite), Pillow, APScheduler.
Set TELEGRAM_BOT_TOKEN in env before running.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Optional, Protocol
from urllib.parse import urlparse

import httpx
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from PIL import Image
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship, selectinload

# --------------------------------------------------------------------------- #
# Configuration and constants
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s %(name)s - %(message)s",
)
logger = logging.getLogger("watcher_bot")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
AVATAR_DIR = DATA_DIR / "avatars"
DATA_DIR.mkdir(parents=True, exist_ok=True)
AVATAR_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = f"sqlite+aiosqlite:///{(DATA_DIR / 'bot.db').as_posix()}"
CHECK_INTERVAL_MINUTES = 60
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class Settings:
    telegram_token: str

    @classmethod
    def load(cls) -> "Settings":
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required in environment variables.")
        return cls(telegram_token=token)


class PlatformType(str, Enum):
    INSTAGRAM = "INSTAGRAM"
    TWITTER = "TWITTER"
    SOUNDCLOUD = "SOUNDCLOUD"
    TUMBLR = "TUMBLR"
    TIKTOK = "TIKTOK"
    UNKNOWN = "UNKNOWN"


class VisibilityState(str, Enum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    UNKNOWN = "UNKNOWN"


@dataclass
class ProfileData:
    avatar_url: Optional[str]
    visibility_state: VisibilityState
    follower_count: Optional[int] = None
    following_count: Optional[int] = None


# --------------------------------------------------------------------------- #
# Database setup
# --------------------------------------------------------------------------- #

Base = declarative_base()
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_user_id = Column(BigInteger, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    accounts = relationship("TrackedAccount", back_populates="user", cascade="all, delete-orphan")


class TrackedAccount(Base):
    __tablename__ = "tracked_accounts"
    __table_args__ = (UniqueConstraint("user_id", "profile_url", name="uq_user_profile"),)

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    platform = Column(String(32), nullable=False)
    profile_url = Column(String(512), nullable=False)
    last_avatar_hash = Column(String(128))
    last_visibility_state = Column(String(16), default=VisibilityState.UNKNOWN.value)
    last_followers_count = Column(Integer)
    last_following_count = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    user = relationship("User", back_populates="accounts")
    snapshots = relationship(
        "AvatarSnapshot", back_populates="account", cascade="all, delete-orphan"
    )


class AvatarSnapshot(Base):
    __tablename__ = "avatar_snapshots"

    id = Column(Integer, primary_key=True)
    tracked_account_id = Column(
        Integer, ForeignKey("tracked_accounts.id", ondelete="CASCADE"), nullable=False
    )
    file_path = Column(String(1024), nullable=False)
    hash = Column(String(128), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    account = relationship("TrackedAccount", back_populates="snapshots")


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Ensure new columns exist in SQLite when upgrading without full migrations.
        await ensure_schema(conn)
    logger.info("Database ready at %s", DATABASE_URL)


async def ensure_schema(conn) -> None:
    async def column_exists(table: str, column: str) -> bool:
        result = await conn.execute(text(f"PRAGMA table_info('{table}')"))
        rows = result.fetchall()
        return any(row[1] == column for row in rows)

    if not await column_exists("tracked_accounts", "last_followers_count"):
        await conn.execute(text("ALTER TABLE tracked_accounts ADD COLUMN last_followers_count INTEGER"))
    if not await column_exists("tracked_accounts", "last_following_count"):
        await conn.execute(text("ALTER TABLE tracked_accounts ADD COLUMN last_following_count INTEGER"))


# --------------------------------------------------------------------------- #
# Utilities
# --------------------------------------------------------------------------- #

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_url(url: str) -> Optional[str]:
    url = (url or "").strip()
    if not url:
        return None
    if not re.match(r"https?://", url, re.IGNORECASE):
        url = f"https://{url}"
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return None
    normalized = parsed._replace(fragment="", query="").geturl().rstrip("/")
    return normalized


def detect_platform(url: str) -> PlatformType:
    host = urlparse(url).netloc.lower()
    if "instagram.com" in host:
        return PlatformType.INSTAGRAM
    if "twitter.com" in host or host == "x.com" or host.endswith(".x.com") or "x.com" in host:
        return PlatformType.TWITTER
    if "soundcloud.com" in host:
        return PlatformType.SOUNDCLOUD
    if "tumblr.com" in host:
        return PlatformType.TUMBLR
    if "tiktok.com" in host:
        return PlatformType.TIKTOK
    return PlatformType.UNKNOWN


def short_profile_name(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if parts:
        return parts[0]
    return parsed.netloc


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def extract_og_image(html: str) -> Optional[str]:
    match = re.search(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)
    match = re.search(r'"profile_image_url"\s*:\s*"([^"]+)"', html)
    if match:
        return match.group(1)
    return None


def detect_private_in_html(platform: PlatformType, html: str) -> bool:
    private_markers = [
        r'"is_private"\s*:\s*true',
        r'"isPrivate"\s*:\s*true',
        r'"privateAccount"\s*:\s*true',
        r"this account is private",
        r"account is private",
    ]
    for pattern in private_markers:
        if re.search(pattern, html, flags=re.IGNORECASE):
            return True
    # Instagram-specific text marker
    if platform == PlatformType.INSTAGRAM and "This Account is Private" in html:
        return True
    return False


def _parse_int(value: str) -> Optional[int]:
    try:
        cleaned = value.replace(",", "").replace(".", "")
        return int(cleaned)
    except Exception:
        return None


def extract_counts(html: str) -> tuple[Optional[int], Optional[int]]:
    """Best-effort follower/following extraction from public HTML/JSON snippets."""
    follower_patterns = [
        r'"followerCount"\s*:\s*(\d+)',
        r'"followers_count"\s*:\s*(\d+)',
        r'"edge_followed_by"\s*:\s*\{"count"\s*:\s*(\d+)',
        r'"fans"\s*:\s*(\d+)',
        r'"fans"\s*:\s*"([\d,\.]+)"',
        r'"followers"\s*:\s*"([\d,\.]+)"',
    ]
    following_patterns = [
        r'"followingCount"\s*:\s*(\d+)',
        r'"following"\s*:\s*(\d+)',
        r'"following"\s*:\s*"([\d,\.]+)"',
        r'"edge_follow"\s*:\s*\{"count"\s*:\s*(\d+)',
    ]
    follower_count = None
    following_count = None
    for pattern in follower_patterns:
        m = re.search(pattern, html, flags=re.IGNORECASE)
        if m:
            follower_count = _parse_int(m.group(1))
            break
    for pattern in following_patterns:
        m = re.search(pattern, html, flags=re.IGNORECASE)
        if m:
            following_count = _parse_int(m.group(1))
            break
    return follower_count, following_count


def downscale_for_telegram(data: bytes, max_bytes: int = 9 * 1024 * 1024) -> bytes:
    if len(data) <= max_bytes:
        return data
    with Image.open(BytesIO(data)) as img:
        img = img.convert("RGB")
        quality = 90
        while quality > 20:
            buffer = BytesIO()
            img.save(buffer, format="JPEG", optimize=True, quality=quality)
            if buffer.tell() <= max_bytes:
                return buffer.getvalue()
            quality -= 10
    return data


async def get_or_create_user(session: AsyncSession, telegram_user_id: int) -> User:
    result = await session.execute(select(User).where(User.telegram_user_id == telegram_user_id))
    user = result.scalar_one_or_none()
    if not user:
        user = User(telegram_user_id=telegram_user_id)
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


# --------------------------------------------------------------------------- #
# Profile providers
# --------------------------------------------------------------------------- #

class ProfileProvider(Protocol):
    async def fetch_profile(self, profile_url: str, client: httpx.AsyncClient) -> ProfileData:
        ...


class SimpleHTMLProvider:
    """Fetch avatar via public HTML; visibility inferred from HTTP status."""

    def __init__(self, platform: PlatformType):
        self.platform = platform

    async def fetch_profile(self, profile_url: str, client: httpx.AsyncClient) -> ProfileData:
        try:
            resp = await client.get(profile_url, headers={"User-Agent": USER_AGENT})
        except httpx.HTTPError as exc:
            logger.warning("HTTP error for %s: %s", profile_url, exc)
            return ProfileData(avatar_url=None, visibility_state=VisibilityState.UNKNOWN)

        # Start unknown until we confirm the page is reachable
        visibility = VisibilityState.UNKNOWN
        if resp.status_code in (401, 403):
            visibility = VisibilityState.PRIVATE
        elif resp.status_code >= 400:
            visibility = VisibilityState.UNKNOWN
        elif resp.status_code == 200:
            visibility = VisibilityState.PUBLIC

        avatar_url: Optional[str] = None
        follower_count: Optional[int] = None
        following_count: Optional[int] = None
        if resp.status_code == 200 and resp.text:
            avatar_url = extract_og_image(resp.text)
            follower_count, following_count = extract_counts(resp.text)

            # Detect login/private pages even when the status code is 200 (e.g., Instagram login wall).
            is_login_redirect = "login" in resp.url.path.lower() or "signin" in resp.url.path.lower()
            login_markers = ["log in to see", "login", "sign in", "private account"]
            if detect_private_in_html(self.platform, resp.text) or is_login_redirect or any(
                marker in resp.text.lower() for marker in login_markers
            ):
                visibility = VisibilityState.PRIVATE
        return ProfileData(
            avatar_url=avatar_url,
            visibility_state=visibility,
            follower_count=follower_count,
            following_count=following_count,
        )


PROVIDERS: dict[PlatformType, ProfileProvider] = {
    PlatformType.INSTAGRAM: SimpleHTMLProvider(PlatformType.INSTAGRAM),
    PlatformType.TWITTER: SimpleHTMLProvider(PlatformType.TWITTER),
    PlatformType.SOUNDCLOUD: SimpleHTMLProvider(PlatformType.SOUNDCLOUD),
    PlatformType.TUMBLR: SimpleHTMLProvider(PlatformType.TUMBLR),
    PlatformType.TIKTOK: SimpleHTMLProvider(PlatformType.TIKTOK),
}


def provider_for(platform: PlatformType) -> ProfileProvider:
    return PROVIDERS.get(platform, SimpleHTMLProvider(PlatformType.UNKNOWN))


# --------------------------------------------------------------------------- #
# Account checking logic
# --------------------------------------------------------------------------- #

http_client: Optional[httpx.AsyncClient] = None


async def get_http_client() -> httpx.AsyncClient:
    global http_client
    if http_client is None:
        http_client = httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers={"User-Agent": USER_AGENT})
    return http_client


async def close_http_client() -> None:
    global http_client
    if http_client:
        await http_client.aclose()
        http_client = None


async def download_image(url: str, client: httpx.AsyncClient) -> Optional[bytes]:
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content
    except httpx.HTTPError as exc:
        logger.warning("Failed to download image %s: %s", url, exc)
        return None


async def latest_snapshot(session: AsyncSession, account_id: int) -> Optional[AvatarSnapshot]:
    result = await session.execute(
        select(AvatarSnapshot)
        .where(AvatarSnapshot.tracked_account_id == account_id)
        .order_by(AvatarSnapshot.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def check_account(
    session: AsyncSession,
    bot: Bot,
    account: TrackedAccount,
    client: httpx.AsyncClient,
    notify: bool = True,
) -> None:
    """Fetch profile data, detect avatar/visibility changes, persist, and optionally notify."""
    await session.refresh(account, attribute_names=["user"])

    provider = provider_for(PlatformType(account.platform))
    try:
        profile = await provider.fetch_profile(account.profile_url, client)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Provider failure for %s: %s", account.profile_url, exc)
        if notify and account.user:
            await bot.send_message(
                account.user.telegram_user_id,
                "I couldn't check this profile right now, will retry later.",
            )
        return

    avatar_changed = False
    visibility_changed = False
    new_avatar_path: Optional[Path] = None
    prev_visibility_value = account.last_visibility_state or VisibilityState.UNKNOWN.value
    prev_followers = account.last_followers_count
    prev_following = account.last_following_count
    followers_changed = False
    following_changed = False

    if profile.avatar_url:
        image_bytes = await download_image(profile.avatar_url, client)
        if image_bytes:
            new_hash = compute_sha256(image_bytes)
            if new_hash != account.last_avatar_hash:
                avatar_changed = True
                ts = now_utc().strftime("%Y%m%dT%H%M%SZ")
                account_dir = AVATAR_DIR / str(account.id)
                account_dir.mkdir(parents=True, exist_ok=True)
                new_avatar_path = account_dir / f"{ts}.jpg"
                new_avatar_path.write_bytes(image_bytes)
                snapshot = AvatarSnapshot(
                    tracked_account_id=account.id, file_path=str(new_avatar_path), hash=new_hash
                )
                session.add(snapshot)
                account.last_avatar_hash = new_hash
        else:
            logger.info("No avatar bytes for %s", account.profile_url)
    else:
        logger.info("No avatar URL detected for %s", account.profile_url)

    new_visibility = profile.visibility_state
    if new_visibility != VisibilityState.UNKNOWN and new_visibility.value != account.last_visibility_state:
        visibility_changed = True
        account.last_visibility_state = new_visibility.value

    if profile.follower_count is not None and profile.follower_count != account.last_followers_count:
        followers_changed = True
        account.last_followers_count = profile.follower_count

    if profile.following_count is not None and profile.following_count != account.last_following_count:
        following_changed = True
        account.last_following_count = profile.following_count

    account.updated_at = now_utc()
    await session.commit()

    if notify and (avatar_changed or visibility_changed or followers_changed or following_changed):
        await notify_user_of_change(
            bot=bot,
            account=account,
            avatar_changed=avatar_changed,
            visibility_changed=visibility_changed,
            previous_visibility=prev_visibility_value,
            new_visibility=new_visibility,
            followers_changed=followers_changed,
            following_changed=following_changed,
            previous_followers=prev_followers,
            previous_following=prev_following,
            new_followers=profile.follower_count,
            new_following=profile.following_count,
            avatar_path=new_avatar_path,
        )


async def notify_user_of_change(
    bot: Bot,
    account: TrackedAccount,
    avatar_changed: bool,
    visibility_changed: bool,
    previous_visibility: str,
    new_visibility: VisibilityState,
    followers_changed: bool,
    following_changed: bool,
    previous_followers: Optional[int],
    previous_following: Optional[int],
    new_followers: Optional[int],
    new_following: Optional[int],
    avatar_path: Optional[Path],
) -> None:
    parts: list[str] = []
    if avatar_changed:
        parts.append(
            f"Profile picture changed for {account.platform.title()} {short_profile_name(account.profile_url)}."
        )
    if visibility_changed:
        parts.append(
            f"Visibility changed for {account.platform.title()} {short_profile_name(account.profile_url)}: "
            f"{previous_visibility} \u2192 {new_visibility.value}"
        )
    if followers_changed:
        parts.append(
            f"Followers changed for {account.platform.title()} {short_profile_name(account.profile_url)}: "
            f"{previous_followers} \u2192 {new_followers}"
        )
    if following_changed:
        parts.append(
            f"Following changed for {account.platform.title()} {short_profile_name(account.profile_url)}: "
            f"{previous_following} \u2192 {new_following}"
        )
    text = "\n".join(parts) if parts else "Change detected."

    if avatar_changed and avatar_path and avatar_path.exists():
        photo_bytes = avatar_path.read_bytes()
        photo_bytes = downscale_for_telegram(photo_bytes)
        await bot.send_photo(
            account.user.telegram_user_id,
            photo=InputFile(BytesIO(photo_bytes), filename=avatar_path.name),
            caption=text,
        )
    else:
        await bot.send_message(account.user.telegram_user_id, text)


async def run_scheduled_checks(bot: Bot) -> None:
    logger.info("Running scheduled check for all accounts")
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TrackedAccount).options(selectinload(TrackedAccount.user))
        )
        accounts = result.scalars().all()
        client = await get_http_client()
        for account in accounts:
            try:
                await check_account(session, bot, account, client, notify=True)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to check account %s: %s", account.profile_url, exc)


# --------------------------------------------------------------------------- #
# Telegram bot handlers
# --------------------------------------------------------------------------- #

router = Router()


class AddAccountStates(StatesGroup):
    waiting_for_url = State()


def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Add account", callback_data="add_account")
    builder.button(text="List accounts", callback_data="list_accounts:1")
    builder.button(text="Remove account", callback_data="remove_account")
    builder.adjust(1)
    return builder.as_markup()


@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    async with AsyncSessionLocal() as session:
        await get_or_create_user(session, message.from_user.id)
    await state.clear()
    welcome = (
        "Hi! I watch profile pictures and visibility for your tracked accounts "
        "(Instagram, X/Twitter, SoundCloud, Tumblr). "
        "Use the buttons below to add or manage accounts."
    )
    await message.answer(welcome, reply_markup=main_menu_keyboard())


async def safe_edit_text(message: types.Message, text: str, **kwargs) -> None:
    """
    Try editing the message; if it fails (e.g., message too old/not modified), send a new one.
    This prevents callback buttons from silently failing when Telegram rejects an edit.
    """
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as exc:
        logger.info("Edit failed (%s), sending new message instead", exc)
        await message.answer(text, **kwargs)


@router.callback_query(F.data == "add_account")
async def add_account_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AddAccountStates.waiting_for_url)
    await callback.message.answer("Send me the link (URL) of the profile you want me to watch.")
    await callback.answer()


@router.message(AddAccountStates.waiting_for_url)
async def handle_account_url(message: types.Message, state: FSMContext, bot: Bot) -> None:
    url_raw = message.text or ""
    normalized = normalize_url(url_raw)
    if not normalized:
        await message.answer(
            "I can't understand this link. Please send a direct link to a profile on Instagram, X (Twitter), SoundCloud, Tumblr, or similar."
        )
        return

    platform = detect_platform(normalized)
    if platform == PlatformType.UNKNOWN:
        await message.answer(
            "I can't understand this link. Please send a direct link to a profile on Instagram, X (Twitter), SoundCloud, Tumblr, or similar."
        )
        return

    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, message.from_user.id)
        existing = await session.execute(
            select(TrackedAccount).where(
                TrackedAccount.user_id == user.id, TrackedAccount.profile_url == normalized
            )
        )
        if existing.scalar_one_or_none():
            await message.answer("This account is already being watched.", reply_markup=main_menu_keyboard())
            await state.clear()
            return

        account = TrackedAccount(
            user_id=user.id,
            platform=platform.value,
            profile_url=normalized,
            last_visibility_state=VisibilityState.UNKNOWN.value,
        )
        account.user = user
        session.add(account)
        await session.commit()
        await session.refresh(account)

        await message.answer("Account saved. Running the first check now...")
        client = await get_http_client()
        try:
            await check_account(session, bot, account, client, notify=False)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Initial check failed: %s", exc)
            await message.answer("I couldn't check this profile right now, will retry later.")
        await session.commit()

    await message.answer("All set! I'll notify you if the avatar or visibility changes.", reply_markup=main_menu_keyboard())
    await state.clear()


async def render_accounts_list(
    chat_id: int,
    message: Optional[types.Message],
    page: int = 1,
    bot: Bot | None = None,
) -> None:
    page_size = 5
    page = max(page, 1)
    offset = (page - 1) * page_size

    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, chat_id)
        total_count = await session.scalar(
            select(func.count()).select_from(
                select(TrackedAccount).where(TrackedAccount.user_id == user.id).subquery()
            )
        )
        result = await session.execute(
            select(TrackedAccount)
            .where(TrackedAccount.user_id == user.id)
            .order_by(TrackedAccount.created_at.desc())
            .offset(offset)
            .limit(page_size)
        )
        accounts = result.scalars().all()

    if not accounts:
        text = "You have no tracked accounts yet."
        if message:
            await safe_edit_text(message, text, reply_markup=main_menu_keyboard())
        else:
            if bot is None:
                return
            await bot.send_message(chat_id, text, reply_markup=main_menu_keyboard())
        return

    lines = []
    builder = InlineKeyboardBuilder()
    for idx, acc in enumerate(accounts, start=offset + 1):
        vis = acc.last_visibility_state or VisibilityState.UNKNOWN.value
        follower_info = (
            f", followers: {acc.last_followers_count}" if acc.last_followers_count is not None else ""
        )
        following_info = (
            f", following: {acc.last_following_count}" if acc.last_following_count is not None else ""
        )
        lines.append(
            f"{idx}. {acc.platform.title()} - {short_profile_name(acc.profile_url)} "
            f"(visibility: {vis}{follower_info}{following_info})"
        )
        builder.button(
            text=f"Details #{idx}",
            callback_data=f"account_detail:{acc.id}:{page}",
        )
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="< Prev", callback_data=f"list_accounts:{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="Next >", callback_data=f"list_accounts:{page+1}"))
        if nav_buttons:
            builder.row(*nav_buttons)
    builder.button(text="Back", callback_data="back_to_menu")
    builder.adjust(1)

    text = "Your tracked accounts:\n" + "\n".join(lines)
    if message:
        await safe_edit_text(message, text, reply_markup=builder.as_markup())
    else:
        await bot.send_message(chat_id, text, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("list_accounts"))
async def handle_list_accounts(callback: CallbackQuery) -> None:
    _, page_str = (callback.data.split(":", maxsplit=1) + ["1"])[0:2]
    page = int(page_str) if page_str.isdigit() else 1
    await render_accounts_list(callback.from_user.id, callback.message, page=page, bot=callback.bot)
    await callback.answer()


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery) -> None:
    await safe_edit_text(callback.message, "Choose an action:", reply_markup=main_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data.startswith("account_detail:"))
async def account_detail(callback: CallbackQuery) -> None:
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("Invalid request.")
        return
    _, account_id_str, page_str = parts
    account_id = int(account_id_str)
    page = int(page_str) if page_str.isdigit() else 1

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TrackedAccount)
            .options(selectinload(TrackedAccount.snapshots))
            .where(TrackedAccount.id == account_id)
        )
        account = result.scalar_one_or_none()
        if not account or account.user.telegram_user_id != callback.from_user.id:
            await callback.answer("Not found.")
            return
        snapshot = await latest_snapshot(session, account.id)

    last_avatar = snapshot.created_at.isoformat() if snapshot else "Never"
    last_check = account.updated_at.isoformat() if account.updated_at else "Never"
    vis = account.last_visibility_state or VisibilityState.UNKNOWN.value
    followers = account.last_followers_count if account.last_followers_count is not None else "Unknown"
    following = account.last_following_count if account.last_following_count is not None else "Unknown"
    text = (
        f"Platform: {account.platform.title()}\n"
        f"Profile: {account.profile_url}\n"
        f"Last check: {last_check}\n"
        f"Visibility: {vis}\n"
        f"Followers: {followers}\n"
        f"Following: {following}\n"
        f"Last avatar change: {last_avatar}"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="Back to list", callback_data=f"list_accounts:{page}")
    builder.button(text="Remove this account", callback_data=f"remove_target:{account.id}")
    builder.adjust(1)
    await safe_edit_text(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data == "remove_account")
async def remove_account(callback: CallbackQuery) -> None:
    async with AsyncSessionLocal() as session:
        user = await get_or_create_user(session, callback.from_user.id)
        result = await session.execute(
            select(TrackedAccount).where(TrackedAccount.user_id == user.id).order_by(TrackedAccount.created_at.desc())
        )
        accounts = result.scalars().all()

    if not accounts:
        await safe_edit_text(callback.message, "You have no tracked accounts.", reply_markup=main_menu_keyboard())
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for acc in accounts:
        builder.button(
            text=f"Remove {short_profile_name(acc.profile_url)} ({acc.platform.title()})",
            callback_data=f"remove_target:{acc.id}",
        )
    builder.button(text="Cancel", callback_data="back_to_menu")
    builder.adjust(1)
    await safe_edit_text(callback.message, "Choose an account to remove:", reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("remove_target:"))
async def remove_target(callback: CallbackQuery) -> None:
    account_id = int(callback.data.split(":", maxsplit=1)[1])
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TrackedAccount).where(TrackedAccount.id == account_id).options(selectinload(TrackedAccount.user))
        )
        account = result.scalar_one_or_none()
        if not account or account.user.telegram_user_id != callback.from_user.id:
            await callback.answer("Not found.")
            return
        session.delete(account)
        await session.commit()

    await safe_edit_text(callback.message, "Account removed.", reply_markup=main_menu_keyboard())
    await callback.answer("Removed")


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #

scheduler = AsyncIOScheduler(timezone=timezone.utc)


async def on_startup(bot: Bot) -> None:
    await init_db()
    scheduler.add_job(run_scheduled_checks, "interval", minutes=CHECK_INTERVAL_MINUTES, args=(bot,))
    scheduler.start()
    logger.info("Scheduler started with %s minute interval", CHECK_INTERVAL_MINUTES)


async def on_shutdown(bot: Bot) -> None:
    scheduler.shutdown(wait=False)
    await close_http_client()
    await bot.session.close()
    logger.info("Bot shutdown complete")


async def main() -> None:
    settings = Settings.load()
    bot = Bot(settings.telegram_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")

# requirements.txt
# aiogram>=3.2.0
# httpx>=0.26.0
# SQLAlchemy>=2.0.0
# aiosqlite>=0.19.0
# Pillow>=10.0.0
# apscheduler>=3.10.0

