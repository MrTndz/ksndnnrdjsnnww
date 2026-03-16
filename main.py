# ============================================================
#  ДолгоМёт — Финансовый Telegram-бот
#  aiogram 3.26.0 | Python 3.11+ | BotHost-compatible
#  Автор: mrztn | Сборка: 2026-03-16
# ============================================================

# ─── 1. ИМПОРТЫ ─────────────────────────────────────────────
import asyncio
import json
import logging
import os
import random
import re
import string
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    SuccessfulPayment,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()

# ─── 2. КОНФИГУРАЦИЯ ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dolgomyet")

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
FERNET_KEY: bytes = os.getenv("FERNET_KEY", "").encode()
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
DB_PATH: str = os.getenv("DB_PATH", "dolgomyet.db")

# Проверяем обязательные переменные
_required = {"BOT_TOKEN": BOT_TOKEN, "ADMIN_ID": str(ADMIN_ID), "FERNET_KEY": FERNET_KEY.decode()}
for _k, _v in _required.items():
    if not _v or _v == "0":
        log.critical(f"[ENV] Отсутствует обязательная переменная: {_k}")
        raise SystemExit(f"Установите {_k} в .env")

log.info(f"[ENV] BOT_TOKEN=...{BOT_TOKEN[-6:]}")
log.info(f"[ENV] ADMIN_ID={ADMIN_ID}")
log.info(f"[ENV] GEMINI={'✅' if GEMINI_API_KEY else '❌ не задан'}")
log.info(f"[ENV] GROQ={'✅' if GROQ_API_KEY else '❌ не задан'}")
log.info(f"[ENV] DB_PATH={DB_PATH}")

# Fernet шифрование
try:
    _fernet = Fernet(FERNET_KEY)
except Exception as e:
    log.critical(f"[FERNET] Неверный ключ: {e}")
    raise SystemExit("Генерируйте ключ: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")

# Константы
FAMILY_PRICE_STARS = 500
FAMILY_MAX_MEMBERS = 5
FAMILY_DURATION_DAYS = 30
TRANSFER_TTL_MINUTES = 5
TRANSFER_HOURLY_LIMIT = 5
NOTIFY_TIMES = ["00:00", "16:00"]
SUPPORTED_BANKS = {
    "tbank": "T-Bank (Тинькофф)",
    "sber": "Сбербанк",
    "alfa": "Альфа-Банк",
    "vtb": "ВТБ",
    "raif": "Райффайзен",
    "gazprom": "Газпромбанк",
    "manual": "Ручной ввод",
}
TRANSFER_BANKS = {
    "TBANK": "T-Bank",
    "TINKOFF": "T-Bank",
    "SBER": "Сбербанк",
    "SBERBANK": "Сбербанк",
    "ALFA": "Альфа-Банк",
    "SBP": "СБП",
    "VTB": "ВТБ",
    "RAIF": "Райффайзен",
}
MSK = timezone(timedelta(hours=3))

# ─── 3. ЛОКАЛИЗАЦИЯ ─────────────────────────────────────────
TEXTS = {
    "ru": {
        "welcome": (
            "👋 <b>Привет, {name}!</b>\n\n"
            "Я <b>ДолгоМёт</b> — твой личный финансовый помощник.\n\n"
            "🔹 Подключи банк и я буду каждый день слать отчёты о долгах\n"
            "🔹 Получай AI-советы по погашению\n"
            "🔹 Переводи деньги прямо из чата\n"
            "🔹 Семейная подписка — смотри долги вместе с близкими\n\n"
            "Начни с кнопки ниже 👇"
        ),
        "menu_title": "📋 <b>Главное меню</b>\nВыберите действие:",
        "no_debts": "✅ У вас нет активных задолженностей.",
        "no_accounts": "🏦 Счета не подключены. Используйте /add_bank",
        "settings_title": "⚙️ <b>Настройки</b>\nВыберите параметр для изменения:",
        "enter_salary": "💰 Введите вашу зарплату в месяц (в рублях):\nПример: <code>45000</code>",
        "enter_utilities": "🏠 Введите расходы на коммуналку в месяц (в рублях):\nПример: <code>6500</code>",
        "enter_expenses": "🛒 Введите прочие ежемесячные расходы (в рублях):\nПример: <code>15000</code>",
        "saved": "✅ Сохранено!",
        "invalid_number": "❌ Введите корректное число (например: 45000)",
        "bank_select": "🏦 <b>Выберите банк для подключения:</b>",
        "bank_method": "🔑 <b>Выберите метод подключения для {bank}:</b>",
        "enter_token": "🔐 Введите <b>API-токен</b> для {bank}:\n\n<i>⚠️ Сообщение будет удалено через 30 сек из соображений безопасности</i>",
        "enter_phone": "📱 Введите <b>номер телефона</b> привязанный к {bank}:\nПример: <code>+79991234567</code>",
        "enter_sms": "📨 Введите <b>SMS-код</b> который пришёл на ваш номер:",
        "bank_connecting": "⏳ Подключаюсь к {bank}...",
        "bank_connected": "✅ <b>{bank}</b> успешно подключён!\nДанные синхронизированы.",
        "bank_error": "❌ Ошибка подключения к {bank}:\n<code>{error}</code>",
        "enter_account_label": "✏️ Введите название для этого счёта:\nПример: <code>Кредитка Тинькофф</code>",
        "manual_debt_bank": "🏦 Введите название банка:",
        "manual_debt_amount": "💳 Введите общую сумму долга (₽):\nПример: <code>150000</code>",
        "manual_debt_monthly": "📅 Введите ежемесячный платёж (₽):",
        "manual_debt_rate": "📊 Введите процентную ставку (% годовых):\nПример: <code>19.9</code>",
        "manual_debt_date": "📆 Введите дату следующего платежа (ДД.ММ.ГГГГ):\nПример: <code>25.04.2026</code>",
        "manual_debt_saved": "✅ Долг добавлен вручную.",
        "sub_info": (
            "⭐ <b>Семейная подписка — 500 Stars / 30 дней</b>\n\n"
            "Что входит:\n"
            "👨‍👩‍👧 До 5 человек в одной группе\n"
            "📊 Общий дашборд долгов\n"
            "🔔 Совместные уведомления\n"
            "💰 Общая сумма задолженности семьи\n\n"
            "Ваша текущая подписка: <b>{status}</b>"
        ),
        "sub_active": "✅ Активна до {date}",
        "sub_none": "❌ Не активна",
        "family_title": "👨‍👩‍👧 <b>Семейная группа</b>",
        "family_not_owner": "❌ Управление семьёй доступно только владельцу подписки.",
        "family_no_sub": "❌ Для семейной функции нужна подписка. /subscribe",
        "send_help": (
            "💸 <b>Перевод денег</b>\n\n"
            "Формат: <code>/send БАНК +7XXXXXXXXXX СУММА</code>\n\n"
            "Примеры:\n"
            "<code>/send TBANK +79991234567 3000</code>\n"
            "<code>/send SBER +79991234567 1500</code>\n"
            "<code>/send SBP +79991234567 500</code>\n\n"
            "Поддерживаемые банки: TBANK, SBER, ALFA, VTB, RAIF, SBP"
        ),
        "transfer_confirm": (
            "💸 <b>Подтверждение перевода</b>\n\n"
            "Банк: <b>{bank}</b>\n"
            "Получатель: <code>{phone}</code>\n"
            "Сумма: <b>{amount} ₽</b>\n\n"
            "🔐 Код подтверждения: <code>{code}</code>\n"
            "Введите: <code>/confirm {code}</code>\n\n"
            "⏱ Код действует <b>5 минут</b>"
        ),
        "transfer_success": "✅ Перевод <b>{amount} ₽</b> на {phone} выполнен успешно!",
        "transfer_failed": "❌ Ошибка перевода: {error}",
        "confirm_not_found": "❌ Код не найден или истёк. Создайте новый перевод.",
        "rate_limit": "⏰ Слишком много переводов. Подождите немного.",
        "advice_generating": "⏳ Генерирую персональный совет...",
        "sync_started": "🔄 Синхронизация запущена...",
        "sync_done": "✅ Синхронизация завершена. Обновлено счетов: {count}",
        "notify_changed": "🔔 Время уведомлений изменено на <b>{time}</b>",
        "notify_off": "🔕 Уведомления отключены",
        "lang_changed": "✅ Язык изменён",
        "data_deleted": "🗑 Все ваши данные удалены.",
        "admin_only": "❌ Только для администратора.",
        "help_text": (
            "📖 <b>Справка по командам:</b>\n\n"
            "/start — запуск / регистрация\n"
            "/menu — главное меню\n"
            "/debts — ваши долги\n"
            "/transactions — история операций\n"
            "/advice — AI-совет по погашению\n"
            "/status — полный дашборд\n"
            "/accounts — управление счетами\n"
            "/add_bank — добавить банк\n"
            "/settings — настройки\n"
            "/subscribe — купить Family подписку\n"
            "/family — семейная группа\n"
            "/send — перевод денег\n"
            "/confirm — подтвердить перевод\n"
            "/sync — синхронизация данных\n"
            "/help — эта справка\n\n"
            "🆘 Поддержка: @mrztn"
        ),
    },
    "en": {
        "welcome": (
            "👋 <b>Hello, {name}!</b>\n\n"
            "I'm <b>DebtBot</b> — your personal finance assistant.\n\n"
            "🔹 Connect your bank for daily debt reports\n"
            "🔹 Get AI-powered repayment advice\n"
            "🔹 Transfer money directly from chat\n"
            "🔹 Family subscription — track debts together\n\n"
            "Get started below 👇"
        ),
        "menu_title": "📋 <b>Main Menu</b>\nChoose an action:",
        "no_debts": "✅ You have no active debts.",
        "no_accounts": "🏦 No accounts connected. Use /add_bank",
        "settings_title": "⚙️ <b>Settings</b>\nSelect a parameter to change:",
        "enter_salary": "💰 Enter your monthly salary (RUB):\nExample: <code>45000</code>",
        "enter_utilities": "🏠 Enter monthly utility expenses (RUB):",
        "enter_expenses": "🛒 Enter other monthly expenses (RUB):",
        "saved": "✅ Saved!",
        "invalid_number": "❌ Please enter a valid number (e.g. 45000)",
        "bank_select": "🏦 <b>Select a bank to connect:</b>",
        "bank_method": "🔑 <b>Select connection method for {bank}:</b>",
        "enter_token": "🔐 Enter <b>API token</b> for {bank}:\n\n<i>⚠️ Message will be deleted in 30 sec for security</i>",
        "enter_phone": "📱 Enter <b>phone number</b> linked to {bank}:\nExample: <code>+79991234567</code>",
        "enter_sms": "📨 Enter the <b>SMS code</b> you received:",
        "bank_connecting": "⏳ Connecting to {bank}...",
        "bank_connected": "✅ <b>{bank}</b> connected successfully!\nData synchronized.",
        "bank_error": "❌ Connection error for {bank}:\n<code>{error}</code>",
        "enter_account_label": "✏️ Enter a name for this account:\nExample: <code>Tinkoff Credit Card</code>",
        "manual_debt_bank": "🏦 Enter bank name:",
        "manual_debt_amount": "💳 Enter total debt amount (RUB):",
        "manual_debt_monthly": "📅 Enter monthly payment (RUB):",
        "manual_debt_rate": "📊 Enter interest rate (% per year):",
        "manual_debt_date": "📆 Enter next payment date (DD.MM.YYYY):",
        "manual_debt_saved": "✅ Debt added manually.",
        "sub_info": (
            "⭐ <b>Family Subscription — 500 Stars / 30 days</b>\n\n"
            "Includes:\n"
            "👨‍👩‍👧 Up to 5 people in one group\n"
            "📊 Shared debt dashboard\n"
            "🔔 Joint notifications\n"
            "💰 Total family debt amount\n\n"
            "Your current subscription: <b>{status}</b>"
        ),
        "sub_active": "✅ Active until {date}",
        "sub_none": "❌ Not active",
        "family_title": "👨‍👩‍👧 <b>Family Group</b>",
        "family_not_owner": "❌ Family management is available to subscription owners only.",
        "family_no_sub": "❌ Family feature requires a subscription. /subscribe",
        "send_help": (
            "💸 <b>Money Transfer</b>\n\n"
            "Format: <code>/send BANK +7XXXXXXXXXX AMOUNT</code>\n\n"
            "Examples:\n"
            "<code>/send TBANK +79991234567 3000</code>\n"
            "<code>/send SBER +79991234567 1500</code>"
        ),
        "transfer_confirm": (
            "💸 <b>Transfer Confirmation</b>\n\n"
            "Bank: <b>{bank}</b>\n"
            "Recipient: <code>{phone}</code>\n"
            "Amount: <b>{amount} RUB</b>\n\n"
            "🔐 Confirmation code: <code>{code}</code>\n"
            "Enter: <code>/confirm {code}</code>\n\n"
            "⏱ Code valid for <b>5 minutes</b>"
        ),
        "transfer_success": "✅ Transfer of <b>{amount} RUB</b> to {phone} completed!",
        "transfer_failed": "❌ Transfer failed: {error}",
        "confirm_not_found": "❌ Code not found or expired. Create a new transfer.",
        "rate_limit": "⏰ Too many transfers. Please wait.",
        "advice_generating": "⏳ Generating personalized advice...",
        "sync_started": "🔄 Sync started...",
        "sync_done": "✅ Sync complete. Accounts updated: {count}",
        "notify_changed": "🔔 Notification time changed to <b>{time}</b>",
        "notify_off": "🔕 Notifications disabled",
        "lang_changed": "✅ Language changed",
        "data_deleted": "🗑 All your data has been deleted.",
        "admin_only": "❌ Admin only.",
        "help_text": (
            "📖 <b>Command Reference:</b>\n\n"
            "/start — start / register\n"
            "/menu — main menu\n"
            "/debts — your debts\n"
            "/transactions — transaction history\n"
            "/advice — AI repayment advice\n"
            "/status — full dashboard\n"
            "/accounts — manage accounts\n"
            "/add_bank — add bank\n"
            "/settings — settings\n"
            "/subscribe — buy Family subscription\n"
            "/family — family group\n"
            "/send — transfer money\n"
            "/confirm — confirm transfer\n"
            "/sync — sync data\n"
            "/help — this help"
        ),
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in TEXTS else "ru"
    text = TEXTS[lang].get(key, TEXTS["ru"].get(key, key))
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text


# ─── 4. УТИЛИТЫ ─────────────────────────────────────────────
def encrypt_token(token: str) -> str:
    return _fernet.encrypt(token.encode()).decode()


def decrypt_token(encrypted: str) -> str:
    try:
        return _fernet.decrypt(encrypted.encode()).decode()
    except Exception:
        return ""


def format_money(amount: float) -> str:
    if amount is None:
        return "0 ₽"
    return f"{amount:,.0f} ₽".replace(",", " ")


def format_date(dt_str: Optional[str]) -> str:
    if not dt_str:
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return dt_str


def format_date_full(dt_str: Optional[str]) -> str:
    if not dt_str:
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return dt_str


def now_msk() -> datetime:
    return datetime.now(MSK)


def now_iso() -> str:
    return now_msk().isoformat()


def gen_confirm_code(length: int = 6) -> str:
    return "".join(random.choices(string.digits, k=length))


def parse_phone(phone: str) -> str:
    phone = re.sub(r"[\s\-\(\)]", "", phone)
    if phone.startswith("8") and len(phone) == 11:
        phone = "+7" + phone[1:]
    elif phone.startswith("7") and len(phone) == 11:
        phone = "+" + phone
    return phone


def mask_phone(phone: str) -> str:
    if len(phone) >= 11:
        return phone[:4] + "***" + phone[-4:]
    return phone


def categorize_transaction(description: str) -> str:
    desc = description.lower()
    if any(w in desc for w in ["магазин", "супермаркет", "продукты", "азбука", "пятёрочка", "перекрёсток"]):
        return "🛒 Продукты"
    if any(w in desc for w in ["кафе", "ресторан", "доставка", "еда", "mcdonald", "burger"]):
        return "🍔 Еда"
    if any(w in desc for w in ["заправка", "автомоб", "парковка", "такси", "яндекс.такси"]):
        return "🚗 Авто"
    if any(w in desc for w in ["коммунал", "жкх", "электр", "газ", "вода", "интернет"]):
        return "🏠 ЖКУ"
    if any(w in desc for w in ["зарплат", "аванс", "доход", "поступлени"]):
        return "💰 Доход"
    if any(w in desc for w in ["кредит", "платёж", "погашени", "долг"]):
        return "💳 Кредиты"
    if any(w in desc for w in ["аптека", "медицин", "больниц", "клиник"]):
        return "💊 Здоровье"
    return "📦 Прочее"


# ─── 5. БАЗА ДАННЫХ ─────────────────────────────────────────
async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def create_tables(db: aiosqlite.Connection) -> None:
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id          INTEGER PRIMARY KEY,
            username         TEXT,
            full_name        TEXT,
            registered_at    TEXT,
            subscription     TEXT DEFAULT 'free',
            subscription_expires TEXT,
            notify_time      TEXT DEFAULT '16:00',
            salary           REAL DEFAULT 0,
            utilities        REAL DEFAULT 0,
            other_expenses   REAL DEFAULT 0,
            family_owner_id  INTEGER,
            language         TEXT DEFAULT 'ru',
            is_blocked       INTEGER DEFAULT 0,
            created_at       TEXT
        );

        CREATE TABLE IF NOT EXISTS bank_accounts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id          INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            bank_name        TEXT NOT NULL,
            bank_key         TEXT NOT NULL,
            connect_method   TEXT NOT NULL,
            encrypted_token  TEXT,
            phone            TEXT,
            account_label    TEXT,
            last_sync        TEXT,
            is_active        INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS debts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id       INTEGER REFERENCES bank_accounts(id) ON DELETE CASCADE,
            user_id          INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            debt_type        TEXT DEFAULT 'loan',
            total_amount     REAL DEFAULT 0,
            monthly_payment  REAL DEFAULT 0,
            interest_rate    REAL DEFAULT 0,
            next_payment_date TEXT,
            overdue_amount   REAL DEFAULT 0,
            bank_name        TEXT,
            updated_at       TEXT
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id       INTEGER REFERENCES bank_accounts(id) ON DELETE CASCADE,
            user_id          INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            amount           REAL NOT NULL,
            description      TEXT,
            category         TEXT,
            tx_date          TEXT,
            raw_data         TEXT
        );

        CREATE TABLE IF NOT EXISTS pending_transfers (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id          INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            target_bank      TEXT NOT NULL,
            target_phone     TEXT NOT NULL,
            amount           REAL NOT NULL,
            confirm_code     TEXT NOT NULL,
            status           TEXT DEFAULT 'pending',
            created_at       TEXT,
            expires_at       TEXT
        );

        CREATE TABLE IF NOT EXISTS family_members (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            member_id        INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            added_at         TEXT,
            UNIQUE(owner_id, member_id)
        );

        CREATE TABLE IF NOT EXISTS family_invites (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id         INTEGER NOT NULL,
            member_id        INTEGER NOT NULL,
            created_at       TEXT,
            status           TEXT DEFAULT 'pending'
        );

        CREATE TABLE IF NOT EXISTS transfer_hourly (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id          INTEGER NOT NULL,
            created_at       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_debts_user ON debts(user_id);
        CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);
        CREATE INDEX IF NOT EXISTS idx_accounts_user ON bank_accounts(user_id);
        CREATE INDEX IF NOT EXISTS idx_pending_user ON pending_transfers(user_id);
    """)
    await db.commit()
    log.info("[DB] Таблицы созданы/проверены")


# ── CRUD пользователи ──
async def db_get_user(db: aiosqlite.Connection, user_id: int) -> Optional[aiosqlite.Row]:
    async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cur:
        return await cur.fetchone()


async def db_upsert_user(db: aiosqlite.Connection, user_id: int, username: str, full_name: str) -> None:
    existing = await db_get_user(db, user_id)
    if existing:
        await db.execute(
            "UPDATE users SET username=?, full_name=? WHERE user_id=?",
            (username, full_name, user_id),
        )
    else:
        now = now_iso()
        await db.execute(
            "INSERT INTO users (user_id, username, full_name, registered_at, created_at) VALUES (?,?,?,?,?)",
            (user_id, username, full_name, now, now),
        )
    await db.commit()


async def db_get_user_lang(db: aiosqlite.Connection, user_id: int) -> str:
    async with db.execute("SELECT language FROM users WHERE user_id=?", (user_id,)) as cur:
        row = await cur.fetchone()
        return row["language"] if row else "ru"


async def db_set_field(db: aiosqlite.Connection, user_id: int, field: str, value) -> None:
    await db.execute(f"UPDATE users SET {field}=? WHERE user_id=?", (value, user_id))
    await db.commit()


async def db_check_subscription(db: aiosqlite.Connection, user_id: int) -> bool:
    async with db.execute(
        "SELECT subscription, subscription_expires FROM users WHERE user_id=?", (user_id,)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return False
    if row["subscription"] == "family" and row["subscription_expires"]:
        expires = datetime.fromisoformat(row["subscription_expires"])
        return expires.replace(tzinfo=MSK) > now_msk()
    return False


async def db_get_family_owner(db: aiosqlite.Connection, user_id: int) -> Optional[int]:
    """Возвращает owner_id если пользователь в семье, иначе None"""
    async with db.execute(
        "SELECT owner_id FROM family_members WHERE member_id=?", (user_id,)
    ) as cur:
        row = await cur.fetchone()
    if row:
        return row["owner_id"]
    # Проверяем: может сам владелец
    if await db_check_subscription(db, user_id):
        return user_id
    return None


# ── CRUD счета ──
async def db_get_accounts(db: aiosqlite.Connection, user_id: int) -> list:
    async with db.execute(
        "SELECT * FROM bank_accounts WHERE user_id=? AND is_active=1 ORDER BY id", (user_id,)
    ) as cur:
        return await cur.fetchall()


async def db_add_account(
    db: aiosqlite.Connection,
    user_id: int,
    bank_name: str,
    bank_key: str,
    connect_method: str,
    encrypted_token: Optional[str],
    phone: Optional[str],
    account_label: str,
) -> int:
    async with db.execute(
        """INSERT INTO bank_accounts
           (user_id, bank_name, bank_key, connect_method, encrypted_token, phone, account_label, last_sync)
           VALUES (?,?,?,?,?,?,?,?)""",
        (user_id, bank_name, bank_key, connect_method, encrypted_token, phone, account_label, now_iso()),
    ) as cur:
        account_id = cur.lastrowid
    await db.commit()
    return account_id


async def db_delete_account(db: aiosqlite.Connection, account_id: int, user_id: int) -> None:
    await db.execute("DELETE FROM bank_accounts WHERE id=? AND user_id=?", (account_id, user_id))
    await db.commit()


# ── CRUD долги ──
async def db_get_debts(db: aiosqlite.Connection, user_id: int) -> list:
    async with db.execute(
        "SELECT * FROM debts WHERE user_id=? ORDER BY total_amount DESC", (user_id,)
    ) as cur:
        return await cur.fetchall()


async def db_get_family_debts(db: aiosqlite.Connection, owner_id: int) -> dict:
    """Возвращает {user_id: {'name': str, 'debts': list}}"""
    result = {}
    # Сам владелец
    async with db.execute("SELECT full_name FROM users WHERE user_id=?", (owner_id,)) as cur:
        row = await cur.fetchone()
    owner_name = row["full_name"] if row else str(owner_id)
    debts = await db_get_debts(db, owner_id)
    result[owner_id] = {"name": owner_name, "debts": debts}
    # Члены семьи
    async with db.execute("SELECT member_id FROM family_members WHERE owner_id=?", (owner_id,)) as cur:
        members = await cur.fetchall()
    for m in members:
        mid = m["member_id"]
        async with db.execute("SELECT full_name FROM users WHERE user_id=?", (mid,)) as cur:
            mrow = await cur.fetchone()
        mname = mrow["full_name"] if mrow else str(mid)
        mdebts = await db_get_debts(db, mid)
        result[mid] = {"name": mname, "debts": mdebts}
    return result


async def db_upsert_debt(
    db: aiosqlite.Connection,
    user_id: int,
    account_id: Optional[int],
    bank_name: str,
    debt_type: str,
    total_amount: float,
    monthly_payment: float,
    interest_rate: float,
    next_payment_date: Optional[str],
    overdue_amount: float = 0.0,
) -> None:
    existing = None
    if account_id:
        async with db.execute(
            "SELECT id FROM debts WHERE account_id=? AND user_id=?", (account_id, user_id)
        ) as cur:
            existing = await cur.fetchone()
    if existing:
        await db.execute(
            """UPDATE debts SET bank_name=?, debt_type=?, total_amount=?, monthly_payment=?,
               interest_rate=?, next_payment_date=?, overdue_amount=?, updated_at=?
               WHERE id=?""",
            (bank_name, debt_type, total_amount, monthly_payment, interest_rate,
             next_payment_date, overdue_amount, now_iso(), existing["id"]),
        )
    else:
        await db.execute(
            """INSERT INTO debts
               (account_id, user_id, bank_name, debt_type, total_amount, monthly_payment,
                interest_rate, next_payment_date, overdue_amount, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (account_id, user_id, bank_name, debt_type, total_amount, monthly_payment,
             interest_rate, next_payment_date, overdue_amount, now_iso()),
        )
    await db.commit()


async def db_add_manual_debt(
    db: aiosqlite.Connection,
    user_id: int,
    bank_name: str,
    total_amount: float,
    monthly_payment: float,
    interest_rate: float,
    next_payment_date: str,
) -> None:
    await db.execute(
        """INSERT INTO debts
           (user_id, bank_name, debt_type, total_amount, monthly_payment,
            interest_rate, next_payment_date, updated_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (user_id, bank_name, "manual", total_amount, monthly_payment,
         interest_rate, next_payment_date, now_iso()),
    )
    await db.commit()


async def db_delete_debt(db: aiosqlite.Connection, debt_id: int, user_id: int) -> None:
    await db.execute("DELETE FROM debts WHERE id=? AND user_id=?", (debt_id, user_id))
    await db.commit()


# ── CRUD транзакции ──
async def db_add_transactions(db: aiosqlite.Connection, user_id: int, account_id: int, txs: list) -> None:
    for tx in txs:
        category = categorize_transaction(tx.get("description", ""))
        await db.execute(
            """INSERT OR IGNORE INTO transactions
               (account_id, user_id, amount, description, category, tx_date, raw_data)
               VALUES (?,?,?,?,?,?,?)""",
            (
                account_id,
                user_id,
                tx.get("amount", 0),
                tx.get("description", ""),
                category,
                tx.get("date", now_iso()),
                json.dumps(tx, ensure_ascii=False),
            ),
        )
    await db.commit()


async def db_get_transactions(db: aiosqlite.Connection, user_id: int, limit: int = 30) -> list:
    async with db.execute(
        "SELECT * FROM transactions WHERE user_id=? ORDER BY tx_date DESC LIMIT ?",
        (user_id, limit),
    ) as cur:
        return await cur.fetchall()


# ── CRUD переводы ──
async def db_create_transfer(
    db: aiosqlite.Connection,
    user_id: int,
    target_bank: str,
    target_phone: str,
    amount: float,
    code: str,
) -> None:
    expires = (now_msk() + timedelta(minutes=TRANSFER_TTL_MINUTES)).isoformat()
    await db.execute(
        """INSERT INTO pending_transfers
           (user_id, target_bank, target_phone, amount, confirm_code, created_at, expires_at)
           VALUES (?,?,?,?,?,?,?)""",
        (user_id, target_bank, target_phone, amount, code, now_iso(), expires),
    )
    await db.execute("INSERT INTO transfer_hourly (user_id, created_at) VALUES (?,?)", (user_id, now_iso()))
    await db.commit()


async def db_get_pending_transfer(db: aiosqlite.Connection, user_id: int, code: str) -> Optional[aiosqlite.Row]:
    async with db.execute(
        "SELECT * FROM pending_transfers WHERE user_id=? AND confirm_code=? AND status='pending'",
        (user_id, code),
    ) as cur:
        return await cur.fetchone()


async def db_complete_transfer(db: aiosqlite.Connection, transfer_id: int) -> None:
    await db.execute("UPDATE pending_transfers SET status='confirmed' WHERE id=?", (transfer_id,))
    await db.commit()


async def db_cancel_expired_transfers(db: aiosqlite.Connection) -> int:
    now = now_iso()
    async with db.execute(
        "UPDATE pending_transfers SET status='cancelled' WHERE status='pending' AND expires_at<?",
        (now,),
    ) as cur:
        count = cur.rowcount
    await db.commit()
    return count


async def db_count_transfers_hour(db: aiosqlite.Connection, user_id: int) -> int:
    hour_ago = (now_msk() - timedelta(hours=1)).isoformat()
    async with db.execute(
        "SELECT COUNT(*) as cnt FROM transfer_hourly WHERE user_id=? AND created_at>?",
        (user_id, hour_ago),
    ) as cur:
        row = await cur.fetchone()
    return row["cnt"] if row else 0


# ── CRUD семья ──
async def db_get_family_members(db: aiosqlite.Connection, owner_id: int) -> list:
    async with db.execute(
        """SELECT fm.member_id, u.full_name, u.username
           FROM family_members fm
           JOIN users u ON u.user_id=fm.member_id
           WHERE fm.owner_id=?""",
        (owner_id,),
    ) as cur:
        return await cur.fetchall()


async def db_add_family_member(db: aiosqlite.Connection, owner_id: int, member_id: int) -> bool:
    try:
        await db.execute(
            "INSERT INTO family_members (owner_id, member_id, added_at) VALUES (?,?,?)",
            (owner_id, member_id, now_iso()),
        )
        await db.execute("UPDATE users SET family_owner_id=? WHERE user_id=?", (owner_id, member_id))
        await db.commit()
        return True
    except Exception:
        return False


async def db_remove_family_member(db: aiosqlite.Connection, owner_id: int, member_id: int) -> None:
    await db.execute("DELETE FROM family_members WHERE owner_id=? AND member_id=?", (owner_id, member_id))
    await db.execute("UPDATE users SET family_owner_id=NULL WHERE user_id=?", (member_id,))
    await db.commit()


async def db_create_invite(db: aiosqlite.Connection, owner_id: int, member_id: int) -> None:
    await db.execute(
        "INSERT INTO family_invites (owner_id, member_id, created_at) VALUES (?,?,?)",
        (owner_id, member_id, now_iso()),
    )
    await db.commit()


async def db_get_invite(db: aiosqlite.Connection, member_id: int) -> Optional[aiosqlite.Row]:
    async with db.execute(
        "SELECT * FROM family_invites WHERE member_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (member_id,),
    ) as cur:
        return await cur.fetchone()


async def db_update_invite_status(db: aiosqlite.Connection, invite_id: int, status: str) -> None:
    await db.execute("UPDATE family_invites SET status=? WHERE id=?", (status, invite_id))
    await db.commit()


# ─── 6. БАНКОВСКИЕ ИНТЕГРАЦИИ ────────────────────────────────

async def fetch_tbank_data(token: str, account_id: int, user_id: int, db: aiosqlite.Connection) -> dict:
    """Получает данные из T-Bank Open API"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    result = {"debts": [], "transactions": [], "error": None}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            # Получаем счета
            async with session.get(
                "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetAccounts",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 401:
                    result["error"] = "Неверный токен. Проверьте API-ключ в T-Bank."
                    return result
                if resp.status != 200:
                    result["error"] = f"HTTP {resp.status} от T-Bank API"
                    return result
                data = await resp.json()

            # Получаем кредитные данные
            async with session.get(
                "https://api.tinkoff.ru/v1/accounts",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp2:
                if resp2.status == 200:
                    acc_data = await resp2.json()
                    accounts = acc_data.get("payload", {}).get("accounts", [])
                    for acc in accounts:
                        if acc.get("accountType") == "Credit":
                            debt = {
                                "bank_name": "T-Bank",
                                "debt_type": "credit_card",
                                "total_amount": abs(float(acc.get("creditLimit", 0)) - float(acc.get("balance", 0))),
                                "monthly_payment": float(acc.get("minimalPayment", 0)),
                                "interest_rate": float(acc.get("interestRate", 0)),
                                "next_payment_date": acc.get("nextPaymentDate"),
                                "overdue_amount": float(acc.get("overdueDebt", 0)),
                            }
                            result["debts"].append(debt)

            # Получаем операции
            async with session.post(
                "https://api.tinkoff.ru/v1/operations",
                json={"from": (now_msk() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z"),
                      "to": now_msk().strftime("%Y-%m-%dT23:59:59Z")},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp3:
                if resp3.status == 200:
                    ops_data = await resp3.json()
                    for op in ops_data.get("payload", {}).get("operations", [])[:50]:
                        tx = {
                            "amount": float(op.get("payment", {}).get("value", 0)) * (-1 if op.get("type") == "Debit" else 1),
                            "description": op.get("description", ""),
                            "date": op.get("date", now_iso()),
                        }
                        result["transactions"].append(tx)

    except aiohttp.ClientConnectorError:
        result["error"] = "Нет соединения с T-Bank API"
    except asyncio.TimeoutError:
        result["error"] = "Таймаут соединения с T-Bank"
    except Exception as e:
        result["error"] = str(e)
    return result


async def fetch_sber_data_by_phone(phone: str, sms_code: str, account_id: int, user_id: int, db: aiosqlite.Connection) -> dict:
    """Получает данные Сбербанка через неофициальный мобильный API"""
    result = {"debts": [], "transactions": [], "session_id": None, "error": None}
    headers = {
        "User-Agent": "okhttp/4.11.0",
        "Content-Type": "application/json",
        "X-Client-Id": "mobile_app",
        "Accept": "application/json",
    }
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            # Авторизация по SMS
            auth_payload = {
                "login": phone,
                "password": sms_code,
                "mobileSDKData": {"deviceId": "dolgomyet_bot"},
            }
            async with session.post(
                "https://online.sberbank.ru/CSAFront/api/gate/v2/mobileApp/registerApp",
                json=auth_payload,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as auth_resp:
                if auth_resp.status not in (200, 201):
                    result["error"] = f"Сбербанк: ошибка авторизации (HTTP {auth_resp.status})"
                    return result
                auth_data = await auth_resp.json()
                session_id = auth_data.get("sessionId") or auth_data.get("token")
                if not session_id:
                    result["error"] = "Сбербанк: не удалось получить сессию"
                    return result

            # Запрашиваем счета
            session_headers = {**headers, "Authorization": f"Bearer {session_id}"}
            async with session.get(
                "https://online.sberbank.ru/CSAFront/api/gate/v2/mobileApp/operations/accounts",
                headers=session_headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as acc_resp:
                if acc_resp.status == 200:
                    acc_data = await acc_resp.json()
                    for acc in acc_data.get("accounts", []):
                        if acc.get("productType") in ("CREDIT", "LOAN"):
                            debt = {
                                "bank_name": "Сбербанк",
                                "debt_type": "loan" if acc.get("productType") == "LOAN" else "credit_card",
                                "total_amount": abs(float(acc.get("debt", acc.get("balance", 0)))),
                                "monthly_payment": float(acc.get("nextPaymentAmount", 0)),
                                "interest_rate": float(acc.get("rate", 0)),
                                "next_payment_date": acc.get("nextPaymentDate"),
                                "overdue_amount": float(acc.get("overdueDebt", 0)),
                            }
                            result["debts"].append(debt)

            # История операций
            async with session.get(
                "https://online.sberbank.ru/CSAFront/api/gate/v2/mobileApp/operations/list"
                f"?from={(now_msk()-timedelta(days=30)).strftime('%Y-%m-%d')}&count=50",
                headers=session_headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as ops_resp:
                if ops_resp.status == 200:
                    ops_data = await ops_resp.json()
                    for op in ops_data.get("operations", [])[:50]:
                        tx = {
                            "amount": float(op.get("sum", {}).get("amount", 0)) * (-1 if op.get("operationType") == "DEBIT" else 1),
                            "description": op.get("description", op.get("merchant", {}).get("name", "")),
                            "date": op.get("date", now_iso()),
                        }
                        result["transactions"].append(tx)

    except aiohttp.ClientConnectorError:
        result["error"] = "Нет соединения с Сбербанк API"
    except asyncio.TimeoutError:
        result["error"] = "Таймаут соединения со Сбербанком"
    except Exception as e:
        result["error"] = str(e)
    return result


async def fetch_alfa_data(token: str, account_id: int, user_id: int, db: aiosqlite.Connection) -> dict:
    """Получает данные из Альфа-Банк API"""
    result = {"debts": [], "transactions": [], "error": None}
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(
                "https://click.alfabank.ru/openapi/api/v1/accounts",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 401:
                    result["error"] = "Неверный токен Альфа-Банк"
                    return result
                if resp.status == 200:
                    data = await resp.json()
                    for acc in data.get("accounts", []):
                        if acc.get("type") in ("CREDIT_CARD", "LOAN", "INSTALLMENT"):
                            debt_amount = abs(float(acc.get("creditDebt", acc.get("balance", 0))))
                            if debt_amount > 0:
                                debt = {
                                    "bank_name": "Альфа-Банк",
                                    "debt_type": "credit_card" if acc.get("type") == "CREDIT_CARD" else "loan",
                                    "total_amount": debt_amount,
                                    "monthly_payment": float(acc.get("minPayment", 0)),
                                    "interest_rate": float(acc.get("interestRate", 0)),
                                    "next_payment_date": acc.get("nextPaymentDate"),
                                    "overdue_amount": float(acc.get("overdueAmount", 0)),
                                }
                                result["debts"].append(debt)

            # Транзакции Альфа
            tx_from = (now_msk() - timedelta(days=30)).strftime("%Y-%m-%d")
            tx_to = now_msk().strftime("%Y-%m-%d")
            async with session.get(
                f"https://click.alfabank.ru/openapi/api/v1/transactions?from={tx_from}&to={tx_to}&limit=50",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as tx_resp:
                if tx_resp.status == 200:
                    tx_data = await tx_resp.json()
                    for op in tx_data.get("transactions", [])[:50]:
                        tx = {
                            "amount": float(op.get("amount", 0)),
                            "description": op.get("merchant", {}).get("name", op.get("description", "")),
                            "date": op.get("date", now_iso()),
                        }
                        result["transactions"].append(tx)

    except aiohttp.ClientConnectorError:
        result["error"] = "Нет соединения с Альфа-Банк API"
    except asyncio.TimeoutError:
        result["error"] = "Таймаут Альфа-Банк"
    except Exception as e:
        result["error"] = str(e)
    return result


async def fetch_bank_data(
    account: dict,
    db: aiosqlite.Connection,
) -> dict:
    """Диспетчер запросов к банкам"""
    method = account["connect_method"]
    token = decrypt_token(account["encrypted_token"]) if account["encrypted_token"] else ""
    phone = account.get("phone", "")
    acc_id = account["id"]
    uid = account["user_id"]

    if method == "tbank_token":
        return await fetch_tbank_data(token, acc_id, uid, db)
    elif method == "sber_phone":
        return {"debts": [], "transactions": [], "error": "Для Сбербанка нужна повторная авторизация по SMS"}
    elif method == "alfa_token":
        return await fetch_alfa_data(token, acc_id, uid, db)
    else:
        return {"debts": [], "transactions": [], "error": None}


async def execute_tbank_transfer(token: str, phone: str, amount: float) -> tuple[bool, str]:
    """Выполняет реальный перевод через T-Bank API"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "sourceAccountId": "auto",
        "targetPhone": phone,
        "amount": int(amount * 100),
        "currency": "RUB",
        "comment": "Перевод через ДолгоМёт",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.tinkoff.ru/v1/payment/transfer/phone",
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("resultCode") == "OK":
                    return True, ""
                error = data.get("errorMessage", data.get("message", f"HTTP {resp.status}"))
                return False, error
    except Exception as e:
        return False, str(e)


async def execute_sbp_transfer(phone: str, amount: float, bank_name: str) -> tuple[bool, str]:
    """Инициирует перевод через СБП"""
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "phone": phone,
                "amount": int(amount * 100),
                "currency": "RUB",
                "bankBik": "",
            }
            async with session.post(
                "https://api.nspk.ru/v1/payment/transfer",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    return True, ""
                return False, f"СБП: HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


# ─── 7. ГЕНЕРАЦИЯ AI-СОВЕТОВ ────────────────────────────────

def _build_advice_prompt(user: aiosqlite.Row, debts: list) -> str:
    free_money = (user["salary"] or 0) - (user["utilities"] or 0) - (user["other_expenses"] or 0)
    total_monthly = sum(d["monthly_payment"] or 0 for d in debts)
    free_after_payments = free_money - total_monthly
    debts_info = "\n".join(
        f"- {d['bank_name']}: долг {format_money(d['total_amount'])}, "
        f"платёж {format_money(d['monthly_payment'])}/мес, "
        f"ставка {d['interest_rate']}%"
        for d in debts
    )
    total_debt = sum(d["total_amount"] or 0 for d in debts)

    return (
        f"Ты опытный финансовый советник в России. Пользователь просит помощи с долгами.\n\n"
        f"Финансовое положение:\n"
        f"- Зарплата: {format_money(user['salary'])}/мес\n"
        f"- Коммуналка: {format_money(user['utilities'])}/мес\n"
        f"- Прочие расходы: {format_money(user['other_expenses'])}/мес\n"
        f"- Свободно после расходов: {format_money(free_money)}/мес\n"
        f"- Обязательные платежи: {format_money(total_monthly)}/мес\n"
        f"- Свободно после всех платежей: {format_money(free_after_payments)}/мес\n\n"
        f"Долги:\n{debts_info}\n"
        f"Итого долгов: {format_money(total_debt)}\n\n"
        f"Составь конкретный план погашения:\n"
        f"1. Какой метод подходит (лавина/снежный ком) и почему\n"
        f"2. Сколько платить по каждому кредиту сверх минимума\n"
        f"3. Через сколько месяцев выйдет из долгов\n"
        f"4. Если денег не хватает — что сократить или как увеличить доход\n\n"
        f"Ответь на русском. Максимум 350 слов. Без воды, конкретно по числам."
    )


def _template_advice(user: aiosqlite.Row, debts: list) -> str:
    if not debts:
        return "✅ Долгов не найдено. Отличная финансовая ситуация!"
    total = sum(d["total_amount"] or 0 for d in debts)
    monthly = sum(d["monthly_payment"] or 0 for d in debts)
    salary = user["salary"] or 0
    free = salary - (user["utilities"] or 0) - (user["other_expenses"] or 0) - monthly
    # Метод лавины: сортируем по убыванию ставки
    sorted_debts = sorted(debts, key=lambda d: d["interest_rate"] or 0, reverse=True)
    top = sorted_debts[0] if sorted_debts else None
    advice = f"💡 <b>Совет по погашению долгов</b>\n\n"
    advice += f"Общий долг: {format_money(total)}\n"
    advice += f"Обязательные платежи: {format_money(monthly)}/мес\n"
    advice += f"Свободно после платежей: {format_money(free)}/мес\n\n"
    if free > 0 and top:
        advice += (
            f"📌 <b>Метод лавины:</b> направьте свободные {format_money(free)} на долг "
            f"<b>{top['bank_name']}</b> (ставка {top['interest_rate']}%). "
            f"Это даст максимальную экономию на процентах.\n\n"
        )
        if top["total_amount"] and monthly > 0:
            months = int(top["total_amount"] / (top["monthly_payment"] + free)) + 1
            advice += f"⏱ Ориентировочный срок закрытия первого долга: ~{months} мес.\n"
    elif free <= 0:
        advice += (
            "⚠️ <b>Внимание:</b> Доходов не хватает на все платежи.\n"
            "Рекомендации:\n"
            "• Обратитесь в банк за реструктуризацией\n"
            "• Рассмотрите рефинансирование под меньший %\n"
            "• Найдите дополнительный источник дохода\n"
        )
    advice += "\n<i>Совет сформирован автоматически. Для AI-анализа добавьте GEMINI_API_KEY.</i>"
    return advice


async def generate_advice(user: aiosqlite.Row, debts: list) -> str:
    if not debts:
        return "✅ Долгов нет! Продолжайте в том же духе."
    prompt = _build_advice_prompt(user, debts)

    # Попытка 1: Google Gemini
    if GEMINI_API_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 500, "temperature": 0.7},
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        text = data["candidates"][0]["content"]["parts"][0]["text"]
                        return f"🤖 <b>AI-совет (Gemini):</b>\n\n{text}"
                    log.warning(f"[GEMINI] HTTP {resp.status}")
        except Exception as e:
            log.warning(f"[GEMINI] Ошибка: {e}")

    # Попытка 2: Groq
    if GROQ_API_KEY:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.1-70b-versatile",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 500,
                        "temperature": 0.7,
                    },
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        text = data["choices"][0]["message"]["content"]
                        return f"🤖 <b>AI-совет (Groq):</b>\n\n{text}"
                    log.warning(f"[GROQ] HTTP {resp.status}")
        except Exception as e:
            log.warning(f"[GROQ] Ошибка: {e}")

    # Фоллбэк: шаблонный совет
    return _template_advice(user, debts)


# ─── 8. FSM СОСТОЯНИЯ ───────────────────────────────────────
class AddBankStates(StatesGroup):
    select_bank = State()
    select_method = State()
    enter_token = State()
    enter_phone = State()
    enter_sms = State()
    enter_label = State()
    # Ручной ввод долга
    manual_bank_name = State()
    manual_amount = State()
    manual_monthly = State()
    manual_rate = State()
    manual_date = State()


class SettingsStates(StatesGroup):
    enter_salary = State()
    enter_utilities = State()
    enter_expenses = State()


class FamilyStates(StatesGroup):
    add_member = State()
    remove_member = State()


# ─── 9. КЛАВИАТУРЫ ─────────────────────────────────────────
def kb_main_menu(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 Мои долги" if lang == "ru" else "💳 My Debts", callback_data="menu:debts")
    builder.button(text="📊 Статус" if lang == "ru" else "📊 Status", callback_data="menu:status")
    builder.button(text="🏦 Счета" if lang == "ru" else "🏦 Accounts", callback_data="menu:accounts")
    builder.button(text="💸 Перевести" if lang == "ru" else "💸 Transfer", callback_data="menu:send_help")
    builder.button(text="💡 Совет AI" if lang == "ru" else "💡 AI Advice", callback_data="menu:advice")
    builder.button(text="👨‍👩‍👧 Семья" if lang == "ru" else "👨‍👩‍👧 Family", callback_data="menu:family")
    builder.button(text="⚙️ Настройки" if lang == "ru" else "⚙️ Settings", callback_data="menu:settings")
    builder.button(text="⭐ Подписка" if lang == "ru" else "⭐ Subscribe", callback_data="menu:subscribe")
    builder.button(text="❓ Помощь" if lang == "ru" else "❓ Help", callback_data="menu:help")
    builder.adjust(3, 3, 3)
    return builder.as_markup()


def kb_banks(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🟡 T-Bank / Тинькофф", callback_data="bank:tbank")
    builder.button(text="🟢 Сбербанк", callback_data="bank:sber")
    builder.button(text="🔴 Альфа-Банк", callback_data="bank:alfa")
    builder.button(text="🔵 ВТБ", callback_data="bank:vtb")
    builder.button(text="🟠 Газпромбанк", callback_data="bank:gazprom")
    builder.button(text="⚪ Райффайзен", callback_data="bank:raif")
    builder.button(text="✍️ Ручной ввод" if lang == "ru" else "✍️ Manual", callback_data="bank:manual")
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:back")
    builder.adjust(2, 2, 2, 1, 1)
    return builder.as_markup()


def kb_bank_method(bank_key: str, lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if bank_key == "tbank":
        builder.button(text="🔑 По API-токену", callback_data=f"bmethod:{bank_key}:token")
    elif bank_key == "sber":
        builder.button(text="📱 По номеру телефона", callback_data=f"bmethod:{bank_key}:phone")
    elif bank_key == "alfa":
        builder.button(text="🔑 По API-токену", callback_data=f"bmethod:{bank_key}:token")
    else:
        builder.button(text="✍️ Ручной ввод данных", callback_data=f"bmethod:{bank_key}:manual")
    builder.button(text="🔙 Назад", callback_data="cmd:add_bank")
    builder.adjust(1)
    return builder.as_markup()


def kb_settings(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 Зарплата" if lang == "ru" else "💰 Salary", callback_data="settings:salary")
    builder.button(text="🏠 Коммуналка" if lang == "ru" else "🏠 Utilities", callback_data="settings:utilities")
    builder.button(text="🛒 Расходы" if lang == "ru" else "🛒 Expenses", callback_data="settings:expenses")
    builder.button(text="🔔 Уведомления 00:00", callback_data="settings:notify:00:00")
    builder.button(text="🔔 Уведомления 16:00", callback_data="settings:notify:16:00")
    builder.button(text="🔕 Откл. уведомления" if lang == "ru" else "🔕 Disable", callback_data="settings:notify:off")
    builder.button(text="🇷🇺 Русский", callback_data="settings:lang:ru")
    builder.button(text="🇬🇧 English", callback_data="settings:lang:en")
    builder.button(text="🗑 Удалить мои данные" if lang == "ru" else "🗑 Delete my data", callback_data="settings:delete_confirm")
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:back")
    builder.adjust(3, 3, 2, 1, 1)
    return builder.as_markup()


def kb_subscribe(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"⭐ Купить Family за {FAMILY_PRICE_STARS} Stars",
        callback_data="sub:buy",
    )
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:back")
    builder.adjust(1)
    return builder.as_markup()


def kb_family(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить участника" if lang == "ru" else "➕ Add member", callback_data="family:add")
    builder.button(text="📋 Список участников" if lang == "ru" else "📋 Members list", callback_data="family:list")
    builder.button(text="❌ Удалить участника" if lang == "ru" else "❌ Remove member", callback_data="family:remove")
    builder.button(text="💰 Общий дашборд" if lang == "ru" else "💰 Family dashboard", callback_data="family:dashboard")
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:back")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def kb_confirm_delete(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить всё" if lang == "ru" else "✅ Yes, delete all", callback_data="settings:delete_yes")
    builder.button(text="❌ Отмена" if lang == "ru" else "❌ Cancel", callback_data="settings:delete_no")
    builder.adjust(2)
    return builder.as_markup()


def kb_invite_response(invite_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принять", callback_data=f"invite:accept:{invite_id}")
    builder.button(text="❌ Отказать", callback_data=f"invite:decline:{invite_id}")
    builder.adjust(2)
    return builder.as_markup()


def kb_accounts_list(accounts: list, lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for acc in accounts:
        builder.button(
            text=f"🏦 {acc['account_label']} ({acc['bank_name']})",
            callback_data=f"acc:view:{acc['id']}",
        )
    builder.button(text="➕ Добавить банк" if lang == "ru" else "➕ Add bank", callback_data="cmd:add_bank")
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:back")
    builder.adjust(1)
    return builder.as_markup()


def kb_account_actions(acc_id: int, lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Синхронизировать" if lang == "ru" else "🔄 Sync", callback_data=f"acc:sync:{acc_id}")
    builder.button(text="❌ Удалить" if lang == "ru" else "❌ Delete", callback_data=f"acc:delete:{acc_id}")
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:accounts")
    builder.adjust(2, 1)
    return builder.as_markup()


# ─── 10. ФОРМАТИРОВАНИЕ СООБЩЕНИЙ ───────────────────────────
def format_debts_message(debts: list, user: aiosqlite.Row, lang: str = "ru") -> str:
    if not debts:
        return t(lang, "no_debts")

    total_debt = sum(d["total_amount"] or 0 for d in debts)
    total_monthly = sum(d["monthly_payment"] or 0 for d in debts)
    total_overdue = sum(d["overdue_amount"] or 0 for d in debts)

    lines = ["💳 <b>Ваши долги:</b>\n"]
    for i, d in enumerate(debts):
        prefix = "└" if i == len(debts) - 1 else "├"
        overdue_str = f" ⚠️ просрочка: {format_money(d['overdue_amount'])}" if d["overdue_amount"] else ""
        next_pay = f" | след. платёж: {format_date(d['next_payment_date'])}" if d["next_payment_date"] else ""
        lines.append(
            f"{prefix} <b>{d['bank_name']}</b> ({d['debt_type']}): "
            f"<b>{format_money(d['total_amount'])}</b>"
            f"{next_pay} → {format_money(d['monthly_payment'])}/мес"
            f"{overdue_str}"
        )

    lines.append(f"\n📊 <b>Итого долгов:</b> {format_money(total_debt)}")
    lines.append(f"💰 <b>Обязательные платежи:</b> {format_money(total_monthly)}/мес")
    if total_overdue > 0:
        lines.append(f"⚠️ <b>Просрочка:</b> {format_money(total_overdue)}")

    return "\n".join(lines)


def format_status_message(user: aiosqlite.Row, debts: list, lang: str = "ru") -> str:
    salary = user["salary"] or 0
    utilities = user["utilities"] or 0
    expenses = user["other_expenses"] or 0
    monthly_payments = sum(d["monthly_payment"] or 0 for d in debts)
    free_money = salary - utilities - expenses
    balance_after = free_money - monthly_payments

    balance_icon = "✅" if balance_after >= 0 else "❌"
    total_debt = sum(d["total_amount"] or 0 for d in debts)

    lines = [
        f"📈 <b>Финансовый дашборд</b>\n",
        format_debts_message(debts, user, lang),
        "",
        "💰 <b>Ваш бюджет:</b>",
        f"Зарплата: {format_money(salary)}",
        f"Коммуналка: -{format_money(utilities)}",
        f"Прочие расходы: -{format_money(expenses)}",
        f"Обязательные платежи: -{format_money(monthly_payments)}",
        f"<b>Остаток: {format_money(balance_after)} {balance_icon}</b>",
    ]

    if balance_after < 0:
        lines.append("\n⚠️ <b>ВНИМАНИЕ: Доходов не хватает на обязательные платежи!</b>")
        lines.append("Рекомендуется обратиться в банк за реструктуризацией.")

    # Простой план погашения (метод лавины)
    if debts and salary > 0:
        sorted_by_rate = sorted(debts, key=lambda d: d["interest_rate"] or 0, reverse=True)
        lines.append("\n📉 <b>План погашения (метод лавины):</b>")
        extra = max(0, balance_after)
        for d in sorted_by_rate[:3]:
            extra_payment = extra if extra > 0 else 0
            extra = 0
            monthly = d["monthly_payment"] or 0
            total = d["total_amount"] or 0
            effective = monthly + extra_payment
            if effective > 0 and total > 0:
                months = max(1, int(total / effective))
                lines.append(
                    f"• {d['bank_name']}: {format_money(monthly)}"
                    + (f" + {format_money(extra_payment)} доп." if extra_payment > 0 else "")
                    + f" = ~{months} мес."
                )

    lines.append(f"\n🔄 Обновлено: {now_msk().strftime('%d.%m.%Y %H:%M')} МСК")
    return "\n".join(lines)


def format_daily_notification(user: aiosqlite.Row, debts: list, advice: str) -> str:
    date_str = now_msk().strftime("%d.%m.%Y")
    salary = user["salary"] or 0
    utilities = user["utilities"] or 0
    expenses = user["other_expenses"] or 0
    monthly_payments = sum(d["monthly_payment"] or 0 for d in debts)
    free_money = salary - utilities - expenses - monthly_payments
    balance_icon = "✅" if free_money >= 0 else "❌"
    total_debt = sum(d["total_amount"] or 0 for d in debts)
    total_overdue = sum(d["overdue_amount"] or 0 for d in debts)

    lines = [
        f"🔔 <b>Ежедневный отчёт | {date_str}</b>\n",
    ]

    if debts:
        lines.append("💳 <b>Долги:</b>")
        for i, d in enumerate(debts):
            prefix = "└" if i == len(debts) - 1 else "├"
            next_pay = format_date(d["next_payment_date"]) if d["next_payment_date"] else "—"
            overdue = f" ⚠️{format_money(d['overdue_amount'])}" if d["overdue_amount"] else ""
            lines.append(
                f"{prefix} {d['bank_name']}: <b>{format_money(d['total_amount'])}</b>"
                f" | {next_pay} → {format_money(d['monthly_payment'])}{overdue}"
            )
        lines.append(f"\n📊 <b>Итого:</b> {format_money(total_debt)}")
        if total_overdue > 0:
            lines.append(f"⚠️ <b>Просрочка:</b> {format_money(total_overdue)}")
    else:
        lines.append("✅ Долгов нет!")

    lines.append(f"\n💰 <b>Бюджет:</b>")
    lines.append(f"Зарплата: {format_money(salary)}")
    lines.append(f"ЖКУ+расходы: -{format_money(utilities + expenses)}")
    lines.append(f"Платежи: -{format_money(monthly_payments)}")
    lines.append(f"<b>Остаток: {format_money(free_money)} {balance_icon}</b>")

    if free_money < 0:
        lines.append("⚠️ <b>Доходов не хватает на все платежи!</b>")

    if advice:
        # Обрезаем совет для ежедневного уведомления
        short_advice = advice[:400] + "..." if len(advice) > 400 else advice
        lines.append(f"\n💡 <b>Совет:</b>\n{short_advice}")

    lines.append(f"\n🔄 <i>Последняя синхронизация: {now_msk().strftime('%d.%m %H:%M')}</i>")
    return "\n".join(lines)


def format_transactions_message(transactions: list, lang: str = "ru") -> str:
    if not transactions:
        return "📭 История операций пуста."

    lines = ["📋 <b>История операций (последние 30)</b>\n"]
    for tx in transactions[:30]:
        amount = tx["amount"] or 0
        sign = "+" if amount >= 0 else ""
        icon = "📈" if amount >= 0 else "📉"
        category = tx["category"] or "📦 Прочее"
        date = format_date(tx["tx_date"])
        desc = (tx["description"] or "")[:40]
        lines.append(
            f"{icon} {sign}{format_money(abs(amount))} | {category} | {date}\n"
            f"   <i>{desc}</i>"
        )
    return "\n".join(lines)


def format_family_dashboard(family_data: dict) -> str:
    lines = ["👨‍👩‍👧 <b>Семейный дашборд</b>\n", "─" * 25]
    total_all = 0
    for uid, data in family_data.items():
        user_total = sum(d["total_amount"] or 0 for d in data["debts"])
        total_all += user_total
        role = "👑" if uid == list(family_data.keys())[0] else "👤"
        lines.append(f"{role} <b>{data['name']}</b>: {format_money(user_total)}")
        for d in data["debts"][:3]:
            lines.append(f"   • {d['bank_name']}: {format_money(d['total_amount'])}")
    lines.append("─" * 25)
    lines.append(f"💰 <b>Общий долг семьи: {format_money(total_all)}</b>")
    return "\n".join(lines)


# ─── 11. ПЛАНИРОВЩИК ────────────────────────────────────────
_bot_instance: Optional[Bot] = None
_db_path: str = DB_PATH


async def send_daily_notifications(notify_time_filter: str) -> None:
    if _bot_instance is None:
        return
    log.info(f"[SCHEDULER] Рассылка уведомлений [{notify_time_filter}]")
    sent = 0
    failed = 0
    try:
        db = await get_db()
        try:
            async with db.execute(
                "SELECT * FROM users WHERE notify_time=? AND is_blocked=0",
                (notify_time_filter,),
            ) as cur:
                users = await cur.fetchall()

            for user in users:
                try:
                    uid = user["user_id"]
                    debts = await db_get_debts(db, uid)
                    lang = user["language"] or "ru"
                    # Генерируем быстрый шаблонный совет (не AI, чтобы не превысить лимиты)
                    advice = _template_advice(user, debts)
                    msg = format_daily_notification(user, debts, advice)
                    await _bot_instance.send_message(uid, msg, parse_mode="HTML")
                    sent += 1
                    await asyncio.sleep(0.05)  # rate limiting
                except TelegramForbiddenError:
                    await db.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (user["user_id"],))
                    await db.commit()
                    failed += 1
                except TelegramBadRequest as e:
                    log.warning(f"[SCHEDULER] Bad request для {user['user_id']}: {e}")
                    failed += 1
                except Exception as e:
                    log.error(f"[SCHEDULER] Ошибка для {user['user_id']}: {e}")
                    failed += 1
        finally:
            await db.close()
    except Exception as e:
        log.error(f"[SCHEDULER] Критическая ошибка рассылки: {e}")
    log.info(f"[SCHEDULER] Отправлено: {sent}, ошибок: {failed}")


async def send_notifications_00() -> None:
    await send_daily_notifications("00:00")


async def send_notifications_16() -> None:
    await send_daily_notifications("16:00")


async def cleanup_expired_transfers() -> None:
    try:
        db = await get_db()
        try:
            count = await db_cancel_expired_transfers(db)
            if count > 0:
                log.info(f"[SCHEDULER] Отменено просроченных переводов: {count}")
        finally:
            await db.close()
    except Exception as e:
        log.error(f"[SCHEDULER] Ошибка очистки переводов: {e}")


async def sync_all_banks_job() -> None:
    log.info("[SCHEDULER] Ночная синхронизация банков")
    synced = 0
    try:
        db = await get_db()
        try:
            async with db.execute(
                "SELECT * FROM bank_accounts WHERE is_active=1 AND connect_method NOT IN ('manual')"
            ) as cur:
                accounts = await cur.fetchall()

            for acc in accounts:
                try:
                    result = await fetch_bank_data(dict(acc), db)
                    if result["error"]:
                        continue
                    for debt in result["debts"]:
                        await db_upsert_debt(
                            db,
                            acc["user_id"],
                            acc["id"],
                            debt["bank_name"],
                            debt["debt_type"],
                            debt["total_amount"],
                            debt["monthly_payment"],
                            debt["interest_rate"],
                            debt.get("next_payment_date"),
                            debt.get("overdue_amount", 0),
                        )
                    if result["transactions"]:
                        await db_add_transactions(db, acc["user_id"], acc["id"], result["transactions"])
                    await db.execute(
                        "UPDATE bank_accounts SET last_sync=? WHERE id=?",
                        (now_iso(), acc["id"]),
                    )
                    await db.commit()
                    synced += 1
                    await asyncio.sleep(1)
                except Exception as e:
                    log.error(f"[SYNC] Ошибка счёта {acc['id']}: {e}")
        finally:
            await db.close()
    except Exception as e:
        log.error(f"[SCHEDULER] Ошибка синхронизации: {e}")
    log.info(f"[SCHEDULER] Синхронизировано счетов: {synced}")


def setup_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(send_notifications_00, "cron", hour=0, minute=0, id="notify_00")
    scheduler.add_job(send_notifications_16, "cron", hour=16, minute=0, id="notify_16")
    scheduler.add_job(cleanup_expired_transfers, "interval", minutes=5, id="cleanup_transfers")
    scheduler.add_job(sync_all_banks_job, "cron", hour=3, minute=0, id="sync_banks")
    return scheduler


# ─── 12. РОУТЕР И ХЭНДЛЕРЫ ──────────────────────────────────
router = Router()


# ── /start ──
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        uid = message.from_user.id
        username = message.from_user.username or ""
        full_name = message.from_user.full_name or ""
        await db_upsert_user(db, uid, username, full_name)
        user = await db_get_user(db, uid)
        lang = user["language"] if user else "ru"
    finally:
        await db.close()

    await message.answer(
        t(lang, "welcome", name=message.from_user.first_name),
        parse_mode="HTML",
        reply_markup=kb_main_menu(lang),
    )


# ── /menu ──
@router.message(Command("menu"))
async def cmd_menu(message: Message, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    await message.answer(t(lang, "menu_title"), parse_mode="HTML", reply_markup=kb_main_menu(lang))


# ── /help ──
@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    await message.answer(t(lang, "help_text"), parse_mode="HTML")


# ── /debts ──
@router.message(Command("debts"))
async def cmd_debts(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        user = await db_get_user(db, uid)
        debts = await db_get_debts(db, uid)
    finally:
        await db.close()
    text = format_debts_message(debts, user, lang)
    await message.answer(text, parse_mode="HTML")


# ── /transactions ──
@router.message(Command("transactions"))
async def cmd_transactions(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        txs = await db_get_transactions(db, uid, limit=30)
    finally:
        await db.close()
    text = format_transactions_message(txs, lang)
    await message.answer(text, parse_mode="HTML")


# ── /status ──
@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        user = await db_get_user(db, uid)
        debts = await db_get_debts(db, uid)
    finally:
        await db.close()
    if not user:
        await message.answer("Сначала выполните /start")
        return
    text = format_status_message(user, debts, lang)
    await message.answer(text, parse_mode="HTML")


# ── /advice ──
@router.message(Command("advice"))
async def cmd_advice(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        user = await db_get_user(db, uid)
        debts = await db_get_debts(db, uid)
    finally:
        await db.close()
    if not user:
        await message.answer("Сначала выполните /start")
        return
    wait_msg = await message.answer(t(lang, "advice_generating"), parse_mode="HTML")
    advice = await generate_advice(user, debts)
    try:
        await wait_msg.delete()
    except Exception:
        pass
    await message.answer(advice, parse_mode="HTML")


# ── /accounts ──
@router.message(Command("accounts"))
async def cmd_accounts(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        accounts = await db_get_accounts(db, uid)
    finally:
        await db.close()
    if not accounts:
        await message.answer(
            t(lang, "no_accounts"),
            parse_mode="HTML",
            reply_markup=kb_accounts_list([], lang),
        )
        return
    await message.answer(
        "🏦 <b>Ваши подключённые счета:</b>",
        parse_mode="HTML",
        reply_markup=kb_accounts_list(accounts, lang),
    )


# ── /sync ──
@router.message(Command("sync"))
async def cmd_sync(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        accounts = await db_get_accounts(db, uid)
    finally:
        await db.close()

    if not accounts:
        await message.answer(t(lang, "no_accounts"), parse_mode="HTML")
        return

    wait_msg = await message.answer(t(lang, "sync_started"), parse_mode="HTML")
    synced = 0
    db = await get_db()
    try:
        for acc in accounts:
            if acc["connect_method"] == "manual":
                continue
            result = await fetch_bank_data(dict(acc), db)
            if result["error"]:
                continue
            for debt in result["debts"]:
                await db_upsert_debt(
                    db, uid, acc["id"], debt["bank_name"], debt["debt_type"],
                    debt["total_amount"], debt["monthly_payment"], debt["interest_rate"],
                    debt.get("next_payment_date"), debt.get("overdue_amount", 0),
                )
            if result["transactions"]:
                await db_add_transactions(db, uid, acc["id"], result["transactions"])
            await db.execute("UPDATE bank_accounts SET last_sync=? WHERE id=?", (now_iso(), acc["id"]))
            await db.commit()
            synced += 1
    finally:
        await db.close()

    try:
        await wait_msg.delete()
    except Exception:
        pass
    await message.answer(t(lang, "sync_done", count=synced), parse_mode="HTML")


# ── /settings ──
@router.message(Command("settings"))
async def cmd_settings(message: Message, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    await message.answer(t(lang, "settings_title"), parse_mode="HTML", reply_markup=kb_settings(lang))


# ── /subscribe ──
@router.message(Command("subscribe"))
async def cmd_subscribe(message: Message) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        user = await db_get_user(db, uid)
        is_sub = await db_check_subscription(db, uid)
    finally:
        await db.close()

    if not user:
        await message.answer("Сначала выполните /start")
        return

    if is_sub:
        expires = user["subscription_expires"]
        status_text = t(lang, "sub_active", date=format_date(expires))
    else:
        status_text = t(lang, "sub_none")

    await message.answer(
        t(lang, "sub_info", status=status_text),
        parse_mode="HTML",
        reply_markup=kb_subscribe(lang),
    )


# ── /family ──
@router.message(Command("family"))
async def cmd_family(message: Message, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        is_sub = await db_check_subscription(db, uid)
        owner_id = await db_get_family_owner(db, uid)
    finally:
        await db.close()

    if not is_sub and owner_id is None:
        db2 = await get_db()
        try:
            owner_id2 = await db_get_family_owner(db2, uid)
        finally:
            await db2.close()
        if owner_id2 is None:
            await message.answer(t(lang, "family_no_sub"), parse_mode="HTML")
            return

    if not is_sub:
        await message.answer(t(lang, "family_not_owner"), parse_mode="HTML")
        return

    await message.answer(
        t(lang, "family_title"),
        parse_mode="HTML",
        reply_markup=kb_family(lang),
    )


# ─── /send БАНК ТЕЛЕФОН СУММА ───────────────────────────────
@router.message(Command("send"))
async def cmd_send(message: Message, command: CommandObject) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        user = await db_get_user(db, uid)
    finally:
        await db.close()

    if not user:
        await message.answer("Сначала выполните /start")
        return

    args = command.args
    if not args:
        await message.answer(t(lang, "send_help"), parse_mode="HTML")
        return

    # Парсинг: /send БАНК +7XXXXXXXXXX СУММА
    pattern = r"^(\w+)\s+(\+?7?\d{10,11}|\+?\d{10,11})\s+(\d+(?:[.,]\d+)?)(?:₽|руб\.?)?$"
    m = re.match(pattern, args.strip())
    if not m:
        await message.answer(t(lang, "send_help"), parse_mode="HTML")
        return

    bank_raw = m.group(1).upper()
    phone_raw = m.group(2)
    amount_raw = m.group(3).replace(",", ".")

    bank_name = TRANSFER_BANKS.get(bank_raw)
    if not bank_name:
        await message.answer(
            f"❌ Банк <b>{bank_raw}</b> не поддерживается.\n"
            f"Доступные: {', '.join(TRANSFER_BANKS.keys())}",
            parse_mode="HTML",
        )
        return

    try:
        amount = float(amount_raw)
    except ValueError:
        await message.answer("❌ Неверная сумма", parse_mode="HTML")
        return

    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля", parse_mode="HTML")
        return

    if amount > 600000:
        await message.answer("❌ Максимальная сумма перевода: 600 000 ₽", parse_mode="HTML")
        return

    phone = parse_phone(phone_raw)
    if not re.match(r"^\+7\d{10}$", phone):
        await message.answer("❌ Некорректный номер телефона. Формат: +79991234567", parse_mode="HTML")
        return

    # Проверка rate limit
    db2 = await get_db()
    try:
        count_hour = await db_count_transfers_hour(db2, uid)
    finally:
        await db2.close()

    if count_hour >= TRANSFER_HOURLY_LIMIT:
        await message.answer(t(lang, "rate_limit"), parse_mode="HTML")
        return

    # Создаём код подтверждения
    code = gen_confirm_code()
    db3 = await get_db()
    try:
        await db_create_transfer(db3, uid, bank_raw, phone, amount, code)
    finally:
        await db3.close()

    masked_phone = mask_phone(phone)
    await message.answer(
        t(lang, "transfer_confirm",
          bank=bank_name,
          phone=masked_phone,
          amount=format_money(amount),
          code=code),
        parse_mode="HTML",
    )


# ─── /confirm КОД ───────────────────────────────────────────
@router.message(Command("confirm"))
async def cmd_confirm(message: Message, command: CommandObject) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
    finally:
        await db.close()

    if not command.args:
        await message.answer("Использование: /confirm 123456", parse_mode="HTML")
        return

    code = command.args.strip()

    db2 = await get_db()
    try:
        transfer = await db_get_pending_transfer(db2, uid, code)
    finally:
        await db2.close()

    if not transfer:
        await message.answer(t(lang, "confirm_not_found"), parse_mode="HTML")
        return

    # Проверяем TTL
    expires = datetime.fromisoformat(transfer["expires_at"])
    if now_msk() > expires.replace(tzinfo=MSK):
        db3 = await get_db()
        try:
            await db3.execute(
                "UPDATE pending_transfers SET status='cancelled' WHERE id=?", (transfer["id"],)
            )
            await db3.commit()
        finally:
            await db3.close()
        await message.answer("⏰ Код подтверждения истёк. Создайте новый перевод.", parse_mode="HTML")
        return

    # Выполняем перевод
    bank_raw = transfer["target_bank"]
    phone = transfer["target_phone"]
    amount = transfer["amount"]

    wait_msg = await message.answer("⏳ Выполняю перевод...", parse_mode="HTML")

    success = False
    error_text = ""

    if bank_raw in ("TBANK", "TINKOFF"):
        # Ищем подключённый T-Bank аккаунт
        db4 = await get_db()
        try:
            async with db4.execute(
                "SELECT * FROM bank_accounts WHERE user_id=? AND bank_key='tbank' AND is_active=1 LIMIT 1",
                (uid,),
            ) as cur:
                acc = await cur.fetchone()
        finally:
            await db4.close()
        if acc and acc["encrypted_token"]:
            token = decrypt_token(acc["encrypted_token"])
            success, error_text = await execute_tbank_transfer(token, phone, amount)
        else:
            error_text = "T-Bank аккаунт не подключён. Добавьте через /add_bank"
    elif bank_raw == "SBP":
        success, error_text = await execute_sbp_transfer(phone, amount, bank_raw)
    else:
        # Для остальных банков — имитация (реальный API требует OAuth)
        error_text = (
            f"Прямой перевод через {TRANSFER_BANKS.get(bank_raw, bank_raw)} "
            f"требует настройки OAuth-авторизации. "
            f"Пожалуйста, выполните перевод вручную в приложении банка."
        )

    try:
        await wait_msg.delete()
    except Exception:
        pass

    if success:
        db5 = await get_db()
        try:
            await db_complete_transfer(db5, transfer["id"])
        finally:
            await db5.close()
        await message.answer(
            t(lang, "transfer_success", amount=format_money(amount), phone=mask_phone(phone)),
            parse_mode="HTML",
        )
    else:
        await message.answer(
            t(lang, "transfer_failed", error=error_text),
            parse_mode="HTML",
        )


# ─── FSM: Добавление банка ───────────────────────────────────
@router.message(Command("add_bank"))
async def cmd_add_bank(message: Message, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    await state.set_state(AddBankStates.select_bank)
    await message.answer(t(lang, "bank_select"), parse_mode="HTML", reply_markup=kb_banks(lang))


@router.callback_query(F.data.startswith("bank:"), AddBankStates.select_bank)
async def cb_bank_selected(callback: CallbackQuery, state: FSMContext) -> None:
    bank_key = callback.data.split(":")[1]
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()

    bank_name = SUPPORTED_BANKS.get(bank_key, bank_key)
    await state.update_data(bank_key=bank_key, bank_name=bank_name)

    if bank_key == "manual":
        await state.set_state(AddBankStates.manual_bank_name)
        await callback.message.edit_text(t(lang, "manual_debt_bank"), parse_mode="HTML")
        await callback.answer()
        return

    await state.set_state(AddBankStates.select_method)
    await callback.message.edit_text(
        t(lang, "bank_method", bank=bank_name),
        parse_mode="HTML",
        reply_markup=kb_bank_method(bank_key, lang),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bmethod:"), AddBankStates.select_method)
async def cb_bank_method(callback: CallbackQuery, state: FSMContext) -> None:
    parts = callback.data.split(":")
    bank_key = parts[1]
    method = parts[2]
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()

    data = await state.get_data()
    bank_name = data.get("bank_name", bank_key)

    if method == "token":
        await state.update_data(connect_method=f"{bank_key}_token")
        await state.set_state(AddBankStates.enter_token)
        await callback.message.edit_text(
            t(lang, "enter_token", bank=bank_name), parse_mode="HTML"
        )
    elif method == "phone":
        await state.update_data(connect_method=f"{bank_key}_phone")
        await state.set_state(AddBankStates.enter_phone)
        await callback.message.edit_text(
            t(lang, "enter_phone", bank=bank_name), parse_mode="HTML"
        )
    elif method == "manual":
        await state.set_state(AddBankStates.manual_bank_name)
        await callback.message.edit_text(t(lang, "manual_debt_bank"), parse_mode="HTML")
    await callback.answer()


@router.message(AddBankStates.enter_token)
async def fsm_enter_token(message: Message, state: FSMContext) -> None:
    token = message.text.strip()
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()

    # Удаляем сообщение с токеном через 30 сек (безопасность)
    async def _delete_later():
        await asyncio.sleep(30)
        try:
            await message.delete()
        except Exception:
            pass
    asyncio.create_task(_delete_later())

    encrypted = encrypt_token(token)
    await state.update_data(encrypted_token=encrypted)
    await state.set_state(AddBankStates.enter_label)
    await message.answer(t(lang, "enter_account_label"), parse_mode="HTML")


@router.message(AddBankStates.enter_phone)
async def fsm_enter_phone(message: Message, state: FSMContext) -> None:
    phone = parse_phone(message.text.strip())
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()

    if not re.match(r"^\+7\d{10}$", phone):
        await message.answer(t(lang, "enter_phone", bank=""), parse_mode="HTML")
        return

    await state.update_data(phone=phone)
    # Для Сбера нужен SMS
    data = await state.get_data()
    if data.get("bank_key") == "sber":
        await state.set_state(AddBankStates.enter_sms)
        # Здесь можно было бы инициировать отправку SMS через Сбер API
        await message.answer(
            f"📨 На номер <code>{mask_phone(phone)}</code> отправлен SMS-код.\n"
            f"Введите код из SMS:",
            parse_mode="HTML",
        )
    else:
        await state.set_state(AddBankStates.enter_label)
        await message.answer(t(lang, "enter_account_label"), parse_mode="HTML")


@router.message(AddBankStates.enter_sms)
async def fsm_enter_sms(message: Message, state: FSMContext) -> None:
    sms_code = message.text.strip()
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
    finally:
        await db.close()

    data = await state.get_data()
    phone = data.get("phone", "")
    bank_key = data.get("bank_key", "")
    bank_name = data.get("bank_name", "")

    wait_msg = await message.answer(t(lang, "bank_connecting", bank=bank_name), parse_mode="HTML")

    # Попытка авторизации через Сбер
    temp_account_id = 0
    result = await fetch_sber_data_by_phone(phone, sms_code, temp_account_id, uid, None)

    try:
        await wait_msg.delete()
    except Exception:
        pass

    if result["error"]:
        await message.answer(t(lang, "bank_error", bank=bank_name, error=result["error"]), parse_mode="HTML")
        await state.clear()
        return

    # Сохраняем зашифрованный "токен" (в данном случае SMS-сессию)
    encrypted = encrypt_token(sms_code)
    await state.update_data(encrypted_token=encrypted, sms_result=result)
    await state.set_state(AddBankStates.enter_label)
    await message.answer(t(lang, "enter_account_label"), parse_mode="HTML")


@router.message(AddBankStates.enter_label)
async def fsm_enter_label(message: Message, state: FSMContext) -> None:
    label = message.text.strip()[:50]
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        data = await state.get_data()

        bank_key = data.get("bank_key", "")
        bank_name = data.get("bank_name", "")
        connect_method = data.get("connect_method", f"{bank_key}_token")
        encrypted_token = data.get("encrypted_token")
        phone = data.get("phone")

        wait_msg = await message.answer(t(lang, "bank_connecting", bank=bank_name), parse_mode="HTML")

        account_id = await db_add_account(
            db, uid, bank_name, bank_key, connect_method, encrypted_token, phone, label
        )

        # Синхронизируем данные
        acc_dict = {
            "id": account_id, "user_id": uid,
            "connect_method": connect_method,
            "encrypted_token": encrypted_token,
            "phone": phone, "bank_key": bank_key,
        }
        result = await fetch_bank_data(acc_dict, db)
        synced_debts = 0
        if not result["error"]:
            for debt in result["debts"]:
                await db_upsert_debt(
                    db, uid, account_id, debt["bank_name"], debt["debt_type"],
                    debt["total_amount"], debt["monthly_payment"], debt["interest_rate"],
                    debt.get("next_payment_date"), debt.get("overdue_amount", 0),
                )
                synced_debts += 1
            if result["transactions"]:
                await db_add_transactions(db, uid, account_id, result["transactions"])

        try:
            await wait_msg.delete()
        except Exception:
            pass

        extra = f"\nЗагружено долгов: {synced_debts}" if synced_debts else ""
        await message.answer(
            t(lang, "bank_connected", bank=bank_name) + extra,
            parse_mode="HTML",
            reply_markup=kb_main_menu(lang),
        )
    except Exception as e:
        log.error(f"[ADD_BANK] Ошибка сохранения: {e}")
        await message.answer(f"❌ Ошибка: {e}", parse_mode="HTML")
    finally:
        await db.close()
        await state.clear()


# ── FSM Ручной ввод долга ──
@router.message(AddBankStates.manual_bank_name)
async def fsm_manual_bank_name(message: Message, state: FSMContext) -> None:
    await state.update_data(manual_bank=message.text.strip())
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    await state.set_state(AddBankStates.manual_amount)
    await message.answer(t(lang, "manual_debt_amount"), parse_mode="HTML")


@router.message(AddBankStates.manual_amount)
async def fsm_manual_amount(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    try:
        amount = float(message.text.strip().replace(",", ".").replace(" ", ""))
        await state.update_data(manual_amount=amount)
        await state.set_state(AddBankStates.manual_monthly)
        await message.answer(t(lang, "manual_debt_monthly"), parse_mode="HTML")
    except ValueError:
        await message.answer(t(lang, "invalid_number"), parse_mode="HTML")


@router.message(AddBankStates.manual_monthly)
async def fsm_manual_monthly(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    try:
        monthly = float(message.text.strip().replace(",", ".").replace(" ", ""))
        await state.update_data(manual_monthly=monthly)
        await state.set_state(AddBankStates.manual_rate)
        await message.answer(t(lang, "manual_debt_rate"), parse_mode="HTML")
    except ValueError:
        await message.answer(t(lang, "invalid_number"), parse_mode="HTML")


@router.message(AddBankStates.manual_rate)
async def fsm_manual_rate(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
    finally:
        await db.close()
    try:
        rate = float(message.text.strip().replace(",", "."))
        await state.update_data(manual_rate=rate)
        await state.set_state(AddBankStates.manual_date)
        await message.answer(t(lang, "manual_debt_date"), parse_mode="HTML")
    except ValueError:
        await message.answer(t(lang, "invalid_number"), parse_mode="HTML")


@router.message(AddBankStates.manual_date)
async def fsm_manual_date(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        date_str = message.text.strip()
        # Парсим дату ДД.ММ.ГГГГ
        try:
            dt = datetime.strptime(date_str, "%d.%m.%Y")
            date_iso = dt.isoformat()
        except ValueError:
            await message.answer("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ (например 25.04.2026)", parse_mode="HTML")
            return

        data = await state.get_data()
        await db_add_manual_debt(
            db, uid,
            data["manual_bank"],
            data["manual_amount"],
            data["manual_monthly"],
            data["manual_rate"],
            date_iso,
        )
        await message.answer(
            t(lang, "manual_debt_saved"),
            parse_mode="HTML",
            reply_markup=kb_main_menu(lang),
        )
    except Exception as e:
        log.error(f"[MANUAL_DEBT] Ошибка: {e}")
        await message.answer(f"❌ Ошибка: {e}")
    finally:
        await db.close()
        await state.clear()


# ── FSM Настройки ──
@router.callback_query(F.data == "settings:salary")
async def cb_settings_salary(callback: CallbackQuery, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()
    await state.set_state(SettingsStates.enter_salary)
    await callback.message.edit_text(t(lang, "enter_salary"), parse_mode="HTML")
    await callback.answer()


@router.message(SettingsStates.enter_salary)
async def fsm_salary(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
        try:
            salary = float(message.text.strip().replace(",", ".").replace(" ", ""))
            await db_set_field(db, message.from_user.id, "salary", salary)
            await state.clear()
            await message.answer(t(lang, "saved"), parse_mode="HTML", reply_markup=kb_settings(lang))
        except ValueError:
            await message.answer(t(lang, "invalid_number"), parse_mode="HTML")
    finally:
        await db.close()


@router.callback_query(F.data == "settings:utilities")
async def cb_settings_utilities(callback: CallbackQuery, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()
    await state.set_state(SettingsStates.enter_utilities)
    await callback.message.edit_text(t(lang, "enter_utilities"), parse_mode="HTML")
    await callback.answer()


@router.message(SettingsStates.enter_utilities)
async def fsm_utilities(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
        try:
            val = float(message.text.strip().replace(",", ".").replace(" ", ""))
            await db_set_field(db, message.from_user.id, "utilities", val)
            await state.clear()
            await message.answer(t(lang, "saved"), parse_mode="HTML", reply_markup=kb_settings(lang))
        except ValueError:
            await message.answer(t(lang, "invalid_number"), parse_mode="HTML")
    finally:
        await db.close()


@router.callback_query(F.data == "settings:expenses")
async def cb_settings_expenses(callback: CallbackQuery, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()
    await state.set_state(SettingsStates.enter_expenses)
    await callback.message.edit_text(t(lang, "enter_expenses"), parse_mode="HTML")
    await callback.answer()


@router.message(SettingsStates.enter_expenses)
async def fsm_expenses(message: Message, state: FSMContext) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, message.from_user.id)
        try:
            val = float(message.text.strip().replace(",", ".").replace(" ", ""))
            await db_set_field(db, message.from_user.id, "other_expenses", val)
            await state.clear()
            await message.answer(t(lang, "saved"), parse_mode="HTML", reply_markup=kb_settings(lang))
        except ValueError:
            await message.answer(t(lang, "invalid_number"), parse_mode="HTML")
    finally:
        await db.close()


@router.callback_query(F.data.startswith("settings:notify:"))
async def cb_notify_time(callback: CallbackQuery) -> None:
    value = callback.data.replace("settings:notify:", "")
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        if value == "off":
            await db_set_field(db, uid, "notify_time", None)
            await callback.message.edit_text(t(lang, "notify_off"), parse_mode="HTML")
        else:
            await db_set_field(db, uid, "notify_time", value)
            await callback.message.edit_text(t(lang, "notify_changed", time=value), parse_mode="HTML")
    finally:
        await db.close()
    await callback.answer()


@router.callback_query(F.data.startswith("settings:lang:"))
async def cb_lang(callback: CallbackQuery) -> None:
    lang_new = callback.data.split(":")[2]
    db = await get_db()
    try:
        uid = callback.from_user.id
        await db_set_field(db, uid, "language", lang_new)
        lang = lang_new
    finally:
        await db.close()
    await callback.message.edit_text(
        t(lang, "lang_changed"),
        parse_mode="HTML",
        reply_markup=kb_settings(lang),
    )
    await callback.answer()


@router.callback_query(F.data == "settings:delete_confirm")
async def cb_delete_confirm(callback: CallbackQuery) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()
    await callback.message.edit_text(
        "⚠️ <b>Вы уверены?</b>\nВсе ваши данные, счета и долги будут удалены безвозвратно.",
        parse_mode="HTML",
        reply_markup=kb_confirm_delete(lang),
    )
    await callback.answer()


@router.callback_query(F.data == "settings:delete_yes")
async def cb_delete_yes(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        await db.execute("DELETE FROM users WHERE user_id=?", (uid,))
        await db.commit()
    finally:
        await db.close()
    await callback.message.edit_text(t(lang, "data_deleted"), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "settings:delete_no")
async def cb_delete_no(callback: CallbackQuery) -> None:
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()
    await callback.message.edit_text(t(lang, "settings_title"), parse_mode="HTML", reply_markup=kb_settings(lang))
    await callback.answer()


# ── Кабинет счёта ──
@router.callback_query(F.data.startswith("acc:view:"))
async def cb_acc_view(callback: CallbackQuery) -> None:
    acc_id = int(callback.data.split(":")[2])
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        async with db.execute("SELECT * FROM bank_accounts WHERE id=? AND user_id=?", (acc_id, uid)) as cur:
            acc = await cur.fetchone()
        if not acc:
            await callback.answer("Счёт не найден", show_alert=True)
            return
        last_sync = format_date_full(acc["last_sync"])
        text = (
            f"🏦 <b>{acc['account_label']}</b>\n"
            f"Банк: {acc['bank_name']}\n"
            f"Метод: {acc['connect_method']}\n"
            f"Последняя синхронизация: {last_sync}\n"
            f"Статус: {'✅ активен' if acc['is_active'] else '❌ отключён'}"
        )
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb_account_actions(acc_id, lang))
    finally:
        await db.close()
    await callback.answer()


@router.callback_query(F.data.startswith("acc:sync:"))
async def cb_acc_sync(callback: CallbackQuery) -> None:
    acc_id = int(callback.data.split(":")[2])
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        async with db.execute("SELECT * FROM bank_accounts WHERE id=? AND user_id=?", (acc_id, uid)) as cur:
            acc = await cur.fetchone()
        if not acc:
            await callback.answer("Счёт не найден", show_alert=True)
            return
        await callback.answer("⏳ Синхронизирую...")
        result = await fetch_bank_data(dict(acc), db)
        if result["error"]:
            await callback.message.answer(f"❌ {result['error']}", parse_mode="HTML")
            return
        synced = 0
        for debt in result["debts"]:
            await db_upsert_debt(
                db, uid, acc_id, debt["bank_name"], debt["debt_type"],
                debt["total_amount"], debt["monthly_payment"], debt["interest_rate"],
                debt.get("next_payment_date"), debt.get("overdue_amount", 0),
            )
            synced += 1
        if result["transactions"]:
            await db_add_transactions(db, uid, acc_id, result["transactions"])
        await db.execute("UPDATE bank_accounts SET last_sync=? WHERE id=?", (now_iso(), acc_id))
        await db.commit()
        await callback.message.answer(f"✅ Синхронизировано! Долгов обновлено: {synced}", parse_mode="HTML")
    finally:
        await db.close()


@router.callback_query(F.data.startswith("acc:delete:"))
async def cb_acc_delete(callback: CallbackQuery) -> None:
    acc_id = int(callback.data.split(":")[2])
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        await db_delete_account(db, acc_id, uid)
    finally:
        await db.close()
    await callback.message.edit_text("✅ Счёт удалён.", parse_mode="HTML")
    await callback.answer()


# ── Семья ──
@router.callback_query(F.data == "family:add")
async def cb_family_add(callback: CallbackQuery, state: FSMContext) -> None:
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        is_sub = await db_check_subscription(db, uid)
        members = await db_get_family_members(db, uid)
    finally:
        await db.close()

    if not is_sub:
        await callback.answer(t(lang, "family_no_sub"), show_alert=True)
        return

    if len(members) >= FAMILY_MAX_MEMBERS - 1:
        await callback.answer(f"❌ Максимум {FAMILY_MAX_MEMBERS} участников в семье", show_alert=True)
        return

    await state.set_state(FamilyStates.add_member)
    await callback.message.edit_text(
        "👤 Введите @username или числовой ID пользователя которого хотите добавить в семью:",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(FamilyStates.add_member)
async def fsm_family_add_member(message: Message, state: FSMContext) -> None:
    raw = message.text.strip().lstrip("@")
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)

        # Ищем пользователя по username или user_id
        if raw.isdigit():
            async with db.execute("SELECT * FROM users WHERE user_id=?", (int(raw),)) as cur:
                target = await cur.fetchone()
        else:
            async with db.execute("SELECT * FROM users WHERE username=?", (raw,)) as cur:
                target = await cur.fetchone()

        if not target:
            await message.answer(
                "❌ Пользователь не найден. Убедитесь, что он зарегистрирован в боте.",
                parse_mode="HTML",
            )
            await state.clear()
            return

        target_id = target["user_id"]
        if target_id == uid:
            await message.answer("❌ Нельзя добавить себя.", parse_mode="HTML")
            await state.clear()
            return

        # Создаём приглашение
        await db_create_invite(db, uid, target_id)
    finally:
        await db.close()
        await state.clear()

    # Отправляем уведомление целевому пользователю
    db2 = await get_db()
    try:
        async with db2.execute(
            "SELECT id FROM family_invites WHERE owner_id=? AND member_id=? ORDER BY id DESC LIMIT 1",
            (uid, target_id),
        ) as cur:
            inv = await cur.fetchone()
        invite_id = inv["id"] if inv else 0
        owner_name = message.from_user.full_name
        target_lang = await db_get_user_lang(db2, target_id)
    finally:
        await db2.close()

    try:
        await message.bot.send_message(
            target_id,
            f"👨‍👩‍👧 <b>Приглашение в семью</b>\n\n"
            f"Пользователь <b>{owner_name}</b> приглашает вас в семейную группу.\n"
            f"Вы сможете вместе отслеживать долги.",
            parse_mode="HTML",
            reply_markup=kb_invite_response(invite_id),
        )
        await message.answer(
            f"✅ Приглашение отправлено пользователю <b>{target['full_name']}</b>.",
            parse_mode="HTML",
        )
    except TelegramForbiddenError:
        await message.answer("❌ Пользователь заблокировал бота.", parse_mode="HTML")


@router.callback_query(F.data.startswith("invite:accept:"))
async def cb_invite_accept(callback: CallbackQuery) -> None:
    invite_id = int(callback.data.split(":")[2])
    db = await get_db()
    try:
        uid = callback.from_user.id
        async with db.execute("SELECT * FROM family_invites WHERE id=? AND member_id=?", (invite_id, uid)) as cur:
            invite = await cur.fetchone()
        if not invite:
            await callback.answer("❌ Приглашение не найдено", show_alert=True)
            return
        owner_id = invite["owner_id"]
        await db_add_family_member(db, owner_id, uid)
        await db_update_invite_status(db, invite_id, "accepted")
    finally:
        await db.close()

    await callback.message.edit_text(
        "✅ <b>Вы приняли приглашение!</b>\nТеперь вы состоите в семейной группе.",
        parse_mode="HTML",
    )
    # Уведомляем владельца
    try:
        await callback.bot.send_message(
            owner_id,
            f"✅ <b>{callback.from_user.full_name}</b> принял(а) приглашение в семью!",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("invite:decline:"))
async def cb_invite_decline(callback: CallbackQuery) -> None:
    invite_id = int(callback.data.split(":")[2])
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM family_invites WHERE id=?", (invite_id,)) as cur:
            invite = await cur.fetchone()
        await db_update_invite_status(db, invite_id, "declined")
        owner_id = invite["owner_id"] if invite else None
    finally:
        await db.close()

    await callback.message.edit_text("❌ Вы отклонили приглашение.", parse_mode="HTML")
    if owner_id:
        try:
            await callback.bot.send_message(
                owner_id,
                f"ℹ️ <b>{callback.from_user.full_name}</b> отклонил(а) приглашение.",
                parse_mode="HTML",
            )
        except Exception:
            pass
    await callback.answer()


@router.callback_query(F.data == "family:list")
async def cb_family_list(callback: CallbackQuery) -> None:
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        members = await db_get_family_members(db, uid)
    finally:
        await db.close()

    if not members:
        await callback.message.edit_text(
            "👨‍👩‍👧 <b>Семья</b>\n\nПока нет участников. Добавьте через кнопку ниже.",
            parse_mode="HTML",
            reply_markup=kb_family(lang),
        )
        await callback.answer()
        return

    lines = ["👨‍👩‍👧 <b>Участники семьи:</b>\n"]
    for m in members:
        uname = f"@{m['username']}" if m["username"] else ""
        lines.append(f"• <b>{m['full_name']}</b> {uname}")
    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=kb_family(lang),
    )
    await callback.answer()


@router.callback_query(F.data == "family:dashboard")
async def cb_family_dashboard(callback: CallbackQuery) -> None:
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        family_data = await db_get_family_debts(db, uid)
    finally:
        await db.close()

    text = format_family_dashboard(family_data)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb_family(lang))
    await callback.answer()


@router.callback_query(F.data == "family:remove")
async def cb_family_remove(callback: CallbackQuery, state: FSMContext) -> None:
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        members = await db_get_family_members(db, uid)
    finally:
        await db.close()

    if not members:
        await callback.answer("Нет участников для удаления", show_alert=True)
        return

    await state.set_state(FamilyStates.remove_member)
    lines = ["Введите @username или ID участника для удаления:\n"]
    for m in members:
        lines.append(f"• {m['full_name']} (@{m['username'] or m['member_id']})")
    await callback.message.edit_text("\n".join(lines), parse_mode="HTML")
    await callback.answer()


@router.message(FamilyStates.remove_member)
async def fsm_family_remove(message: Message, state: FSMContext) -> None:
    raw = message.text.strip().lstrip("@")
    db = await get_db()
    try:
        uid = message.from_user.id
        lang = await db_get_user_lang(db, uid)
        if raw.isdigit():
            async with db.execute("SELECT * FROM users WHERE user_id=?", (int(raw),)) as cur:
                target = await cur.fetchone()
        else:
            async with db.execute("SELECT * FROM users WHERE username=?", (raw,)) as cur:
                target = await cur.fetchone()
        if not target:
            await message.answer("❌ Пользователь не найден", parse_mode="HTML")
            await state.clear()
            return
        await db_remove_family_member(db, uid, target["user_id"])
    finally:
        await db.close()
        await state.clear()
    await message.answer(
        f"✅ <b>{target['full_name']}</b> удалён из семьи.",
        parse_mode="HTML",
        reply_markup=kb_family(),
    )


# ── Подписка Stars ──
@router.callback_query(F.data == "sub:buy")
async def cb_sub_buy(callback: CallbackQuery) -> None:
    uid = callback.from_user.id
    await callback.answer()
    await callback.message.bot.send_invoice(
        chat_id=uid,
        title="ДолгоМёт Family — 30 дней",
        description=(
            "Семейная подписка: до 5 участников, общий дашборд долгов, "
            "совместные уведомления, AI-советы для всей семьи."
        ),
        payload=f"family_sub_{uid}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label="Family подписка", amount=FAMILY_PRICE_STARS)],
    )


@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout: PreCheckoutQuery) -> None:
    await pre_checkout.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message) -> None:
    payment: SuccessfulPayment = message.successful_payment
    uid = message.from_user.id
    expires = (now_msk() + timedelta(days=FAMILY_DURATION_DAYS)).isoformat()
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, uid)
        await db.execute(
            "UPDATE users SET subscription='family', subscription_expires=? WHERE user_id=?",
            (expires, uid),
        )
        await db.commit()
    finally:
        await db.close()
    await message.answer(
        f"🎉 <b>Подписка Family активирована!</b>\n\n"
        f"⭐ Оплачено: {payment.total_amount} Stars\n"
        f"📅 Активна до: {format_date(expires)}\n\n"
        f"Теперь вы можете добавлять членов семьи через /family",
        parse_mode="HTML",
        reply_markup=kb_main_menu(lang),
    )


# ── Главное меню — callback-диспетчер ──
@router.callback_query(F.data.startswith("menu:"))
async def cb_menu(callback: CallbackQuery, state: FSMContext) -> None:
    action = callback.data.split(":")[1]
    uid = callback.from_user.id
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, uid)
        user = await db_get_user(db, uid)
        debts = await db_get_debts(db, uid) if action in ("debts", "status", "advice") else []
    finally:
        await db.close()

    await callback.answer()

    if action == "back":
        await state.clear()
        await callback.message.edit_text(
            t(lang, "menu_title"), parse_mode="HTML", reply_markup=kb_main_menu(lang)
        )

    elif action == "debts":
        text = format_debts_message(debts, user, lang)
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_kb_back(lang))

    elif action == "status":
        text = format_status_message(user, debts, lang)
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_kb_back(lang))

    elif action == "accounts":
        db2 = await get_db()
        try:
            accounts = await db_get_accounts(db2, uid)
        finally:
            await db2.close()
        if accounts:
            await callback.message.edit_text(
                "🏦 <b>Ваши счета:</b>",
                parse_mode="HTML",
                reply_markup=kb_accounts_list(accounts, lang),
            )
        else:
            await callback.message.edit_text(
                t(lang, "no_accounts"),
                parse_mode="HTML",
                reply_markup=kb_accounts_list([], lang),
            )

    elif action == "send_help":
        await callback.message.edit_text(t(lang, "send_help"), parse_mode="HTML", reply_markup=_kb_back(lang))

    elif action == "advice":
        await callback.message.edit_text(t(lang, "advice_generating"), parse_mode="HTML")
        advice = await generate_advice(user, debts)
        await callback.message.edit_text(advice, parse_mode="HTML", reply_markup=_kb_back(lang))

    elif action == "family":
        db3 = await get_db()
        try:
            is_sub = await db_check_subscription(db3, uid)
            owner_id = await db_get_family_owner(db3, uid)
        finally:
            await db3.close()
        if not is_sub and owner_id is None:
            await callback.message.edit_text(
                t(lang, "family_no_sub"), parse_mode="HTML", reply_markup=_kb_back(lang)
            )
            return
        if not is_sub:
            await callback.message.edit_text(
                t(lang, "family_not_owner"), parse_mode="HTML", reply_markup=_kb_back(lang)
            )
            return
        await callback.message.edit_text(
            t(lang, "family_title"), parse_mode="HTML", reply_markup=kb_family(lang)
        )

    elif action == "settings":
        await callback.message.edit_text(
            t(lang, "settings_title"), parse_mode="HTML", reply_markup=kb_settings(lang)
        )

    elif action == "subscribe":
        db4 = await get_db()
        try:
            is_sub = await db_check_subscription(db4, uid)
            user4 = await db_get_user(db4, uid)
        finally:
            await db4.close()
        if is_sub:
            expires = user4["subscription_expires"] if user4 else None
            status_text = t(lang, "sub_active", date=format_date(expires))
        else:
            status_text = t(lang, "sub_none")
        await callback.message.edit_text(
            t(lang, "sub_info", status=status_text),
            parse_mode="HTML",
            reply_markup=kb_subscribe(lang),
        )

    elif action == "help":
        await callback.message.edit_text(t(lang, "help_text"), parse_mode="HTML", reply_markup=_kb_back(lang))


def _kb_back(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Назад" if lang == "ru" else "🔙 Back", callback_data="menu:back")
    return builder.as_markup()


# ── cmd:add_bank callback (из меню счетов) ──
@router.callback_query(F.data == "cmd:add_bank")
async def cb_cmd_add_bank(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    db = await get_db()
    try:
        lang = await db_get_user_lang(db, callback.from_user.id)
    finally:
        await db.close()
    await state.set_state(AddBankStates.select_bank)
    await callback.message.edit_text(t(lang, "bank_select"), parse_mode="HTML", reply_markup=kb_banks(lang))
    await callback.answer()


@router.callback_query(F.data == "menu:accounts")
async def cb_menu_accounts(callback: CallbackQuery) -> None:
    db = await get_db()
    try:
        uid = callback.from_user.id
        lang = await db_get_user_lang(db, uid)
        accounts = await db_get_accounts(db, uid)
    finally:
        await db.close()
    if accounts:
        await callback.message.edit_text(
            "🏦 <b>Ваши счета:</b>", parse_mode="HTML", reply_markup=kb_accounts_list(accounts, lang)
        )
    else:
        await callback.message.edit_text(
            t(lang, "no_accounts"), parse_mode="HTML", reply_markup=kb_accounts_list([], lang)
        )
    await callback.answer()


# ─── 13. АДМИНИСТРАТИВНЫЕ КОМАНДЫ ───────────────────────────
def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_ID:
            db = await get_db()
            try:
                lang = await db_get_user_lang(db, message.from_user.id)
            finally:
                await db.close()
            await message.answer(t(lang, "admin_only"), parse_mode="HTML")
            return
        return await func(message, *args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


@router.message(Command("stats"))
@admin_only
async def cmd_stats(message: Message) -> None:
    db = await get_db()
    try:
        async with db.execute("SELECT COUNT(*) as cnt FROM users") as cur:
            total_users = (await cur.fetchone())["cnt"]
        async with db.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE subscription='family' AND subscription_expires>?",
            (now_iso(),),
        ) as cur:
            family_subs = (await cur.fetchone())["cnt"]
        async with db.execute("SELECT COUNT(*) as cnt FROM bank_accounts WHERE is_active=1") as cur:
            active_accounts = (await cur.fetchone())["cnt"]
        async with db.execute("SELECT COUNT(*) as cnt FROM debts") as cur:
            total_debts = (await cur.fetchone())["cnt"]
        async with db.execute("SELECT SUM(total_amount) as s FROM debts") as cur:
            row = await cur.fetchone()
            total_amount = row["s"] or 0
        async with db.execute("SELECT COUNT(*) as cnt FROM users WHERE is_blocked=1") as cur:
            blocked = (await cur.fetchone())["cnt"]
    finally:
        await db.close()

    text = (
        f"📊 <b>Статистика ДолгоМёт</b>\n\n"
        f"👤 Пользователей: {total_users}\n"
        f"⭐ Family-подписок: {family_subs}\n"
        f"🏦 Активных счетов: {active_accounts}\n"
        f"💳 Долгов в базе: {total_debts}\n"
        f"💰 Общая сумма долгов: {format_money(total_amount)}\n"
        f"🚫 Заблокировали бота: {blocked}\n"
        f"\n🕐 {now_msk().strftime('%d.%m.%Y %H:%M')} МСК"
    )
    await message.answer(text, parse_mode="HTML")


@router.message(Command("broadcast"))
@admin_only
async def cmd_broadcast(message: Message, command: CommandObject) -> None:
    if not command.args:
        await message.answer("Использование: /broadcast ТЕКСТ СООБЩЕНИЯ")
        return

    text = command.args
    db = await get_db()
    try:
        async with db.execute("SELECT user_id FROM users WHERE is_blocked=0") as cur:
            users = await cur.fetchall()
    finally:
        await db.close()

    sent = 0
    failed = 0
    status_msg = await message.answer(f"⏳ Рассылка {len(users)} пользователям...")

    for user in users:
        try:
            await message.bot.send_message(user["user_id"], text, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            failed += 1
            db2 = await get_db()
            try:
                await db2.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (user["user_id"],))
                await db2.commit()
            finally:
                await db2.close()
        except Exception:
            failed += 1

    try:
        await status_msg.edit_text(
            f"✅ Рассылка завершена!\nОтправлено: {sent}\nОшибок: {failed}"
        )
    except Exception:
        pass


@router.message(Command("ban"))
@admin_only
async def cmd_ban(message: Message, command: CommandObject) -> None:
    if not command.args or not command.args.strip().isdigit():
        await message.answer("Использование: /ban USER_ID")
        return
    ban_uid = int(command.args.strip())
    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (ban_uid,))
        await db.commit()
    finally:
        await db.close()
    await message.answer(f"✅ Пользователь {ban_uid} заблокирован.")


@router.message(Command("unban"))
@admin_only
async def cmd_unban(message: Message, command: CommandObject) -> None:
    if not command.args or not command.args.strip().isdigit():
        await message.answer("Использование: /unban USER_ID")
        return
    unban_uid = int(command.args.strip())
    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_blocked=0 WHERE user_id=?", (unban_uid,))
        await db.commit()
    finally:
        await db.close()
    await message.answer(f"✅ Пользователь {unban_uid} разблокирован.")


@router.message(Command("dbbackup"))
@admin_only
async def cmd_dbbackup(message: Message) -> None:
    import os as _os
    if not _os.path.exists(DB_PATH):
        await message.answer("❌ База данных не найдена")
        return
    try:
        from aiogram.types import FSInputFile
        db_file = FSInputFile(DB_PATH, filename="dolgomyet_backup.db")
        await message.answer_document(db_file, caption=f"💾 Бэкап БД | {now_msk().strftime('%d.%m.%Y %H:%M')}")
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки бэкапа: {e}")


@router.message(Command("adddebt"))
@admin_only
async def cmd_admin_adddebt(message: Message, command: CommandObject) -> None:
    """Добавить долг пользователю (для тестирования): /adddebt USER_ID БАНК СУММА"""
    if not command.args:
        await message.answer("Использование: /adddebt USER_ID БАНК СУММА")
        return
    parts = command.args.split()
    if len(parts) < 3:
        await message.answer("Нужно: USER_ID БАНК СУММА")
        return
    try:
        target_uid = int(parts[0])
        bank = parts[1]
        amount = float(parts[2])
        db = await get_db()
        try:
            await db_add_manual_debt(db, target_uid, bank, amount, amount * 0.05, 18.0, now_iso())
        finally:
            await db.close()
        await message.answer(f"✅ Долг {format_money(amount)} в банке {bank} добавлен пользователю {target_uid}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# ─── 14. ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК ──────────────────────
from aiogram.types import ErrorEvent


@router.errors()
async def error_handler(event: ErrorEvent) -> None:
    log.error(f"[ERROR] Необработанное исключение: {event.exception}", exc_info=True)
    if event.update.message:
        try:
            db = await get_db()
            try:
                lang = await db_get_user_lang(db, event.update.message.from_user.id)
            finally:
                await db.close()
            await event.update.message.answer(
                "❌ Произошла внутренняя ошибка. Попробуйте ещё раз или напишите /start",
                parse_mode="HTML",
            )
        except Exception:
            pass
    elif event.update.callback_query:
        try:
            await event.update.callback_query.answer("❌ Ошибка. Попробуйте ещё раз.", show_alert=True)
        except Exception:
            pass


# ─── 15. STARTUP / SHUTDOWN ─────────────────────────────────
async def on_startup(bot: Bot, scheduler: AsyncIOScheduler) -> None:
    global _bot_instance
    _bot_instance = bot

    # Создаём базу
    db = await get_db()
    try:
        await create_tables(db)
    finally:
        await db.close()

    # Запускаем планировщик
    scheduler.start()
    log.info("[SCHEDULER] APScheduler запущен")

    # Регистрируем команды в Telegram
    from aiogram.types import BotCommand
    commands = [
        BotCommand(command="start", description="Запуск / регистрация"),
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="debts", description="Мои долги"),
        BotCommand(command="transactions", description="История операций"),
        BotCommand(command="advice", description="AI-совет по погашению"),
        BotCommand(command="status", description="Полный дашборд"),
        BotCommand(command="accounts", description="Управление счетами"),
        BotCommand(command="add_bank", description="Добавить банк"),
        BotCommand(command="settings", description="Настройки"),
        BotCommand(command="subscribe", description="Купить Family подписку"),
        BotCommand(command="family", description="Семейная группа"),
        BotCommand(command="send", description="Перевод денег"),
        BotCommand(command="confirm", description="Подтвердить перевод"),
        BotCommand(command="sync", description="Синхронизация данных"),
        BotCommand(command="help", description="Справка"),
    ]
    await bot.set_my_commands(commands)
    log.info("[BOT] Команды зарегистрированы")

    bot_info = await bot.get_me()
    log.info(f"[BOT] Запущен: @{bot_info.username} (ID: {bot_info.id})")


async def on_shutdown(scheduler: AsyncIOScheduler) -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        log.info("[SCHEDULER] APScheduler остановлен")
    log.info("[BOT] Бот остановлен")


# ─── 16. MAIN ───────────────────────────────────────────────
async def main() -> None:
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    scheduler = setup_scheduler()

    dp.include_router(router)

    dp.startup.register(lambda: on_startup(bot, scheduler))
    dp.shutdown.register(lambda: on_shutdown(scheduler))

    log.info("[BOT] Запуск polling...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
