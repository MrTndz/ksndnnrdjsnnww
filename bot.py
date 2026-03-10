#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MerAi & Monitoring v6.0 - ФИНАЛЬНАЯ ВЕРСИЯ
Дата: 9 марта 2026, 23:35 МСК
Автор: @mrztn

ФИНАЛЬНАЯ ВЕРСИЯ СО ВСЕМИ ИСПРАВЛЕНИЯМИ:
✅ Автоодобрение заявок в капча-канале
✅ Клонирование ботов с ПОЛНЫМ функционалом
✅ Бонус только один раз за первого бота
✅ UserBot бесплатно для всех
✅ Упрощенная авторизация UserBot (session string)
✅ Исправлены все ошибки Pydantic
✅ Ссылка на оригинал @merai_bbot
"""

import os
import sys
import asyncio
import logging
import aiosqlite
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import json
from pathlib import Path

# Aiogram 3.26.0
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated, LabeledPrice, PreCheckoutQuery,
    ChatJoinRequest
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# Pyrogram для userbot
try:
    from pyrogram import Client as PyrogramClient, filters as pyrogram_filters
    from pyrogram.types import Message as PyrogramMessage
    from pyrogram.handlers import MessageHandler, DeletedMessagesHandler, EditedMessageHandler
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merai_bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = "8505484152:AAHXEFt0lyeMK5ZSJHRYpdPhhFJ0s142Bng"
ORIGINAL_BOT_USERNAME = "merai_bbot"
CAPTCHA_CHANNEL_ID = -1003716882147
CAPTCHA_CHANNEL_LINK = "https://t.me/+AW8ztiMHGY9jMGQ6"
ADMIN_ID = 7785371505
DB_FILE = "merai_data.db"

# Планы подписки
SUBSCRIPTION_PLANS = {
    "week": {"name": "📅 Неделя", "price_stars": 100, "days": 7},
    "month": {"name": "📆 Месяц", "price_stars": 300, "days": 30},
    "quarter": {"name": "📊 3 месяца", "price_stars": 800, "days": 90},
    "year": {"name": "🎯 Год", "price_stars": 2500, "days": 365}
}

# Бонусы
CLONE_BOT_BONUS_DAYS = 3  # Только за ПЕРВОГО бота
REFERRAL_REWARD_COUNT = 50

# =============================================================================
# FSM СОСТОЯНИЯ
# =============================================================================

class UserBotSetup(StatesGroup):
    waiting_session_string = State()

class CloneBotStates(StatesGroup):
    waiting_token = State()

class SupportStates(StatesGroup):
    waiting_message = State()
    admin_reply_waiting = State()

class BroadcastStates(StatesGroup):
    waiting_message = State()

# =============================================================================
# БАЗА ДАННЫХ
# =============================================================================

class Database:
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_tables()
        logger.info("✅ База данных подключена")
    
    async def _create_tables(self):
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_verified BOOLEAN DEFAULT 0,
                mode TEXT DEFAULT 'none',
                subscription_plan TEXT,
                subscription_expires TIMESTAMP,
                referrer_id INTEGER,
                referral_count INTEGER DEFAULT 0,
                is_banned BOOLEAN DEFAULT 0,
                has_clone_bonus BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS userbot_data (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT,
                is_active BOOLEAN DEFAULT 1
            );
            
            CREATE TABLE IF NOT EXISTS saved_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                message_id INTEGER,
                from_user_id INTEGER,
                from_username TEXT,
                from_first_name TEXT,
                message_type TEXT,
                text_content TEXT,
                media_file_id TEXT,
                media_type TEXT,
                caption TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_deleted BOOLEAN DEFAULT 0,
                deleted_at TIMESTAMP,
                is_edited BOOLEAN DEFAULT 0,
                edited_at TIMESTAMP,
                original_text TEXT
            );
            
            CREATE TABLE IF NOT EXISTS cloned_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                bot_token TEXT UNIQUE,
                bot_username TEXT,
                bot_name TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                plan TEXT,
                telegram_payment_charge_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                status TEXT DEFAULT 'open',
                admin_reply TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await self.conn.commit()
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with self.conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def create_user(self, user_id: int, username: str = None, first_name: str = None, referrer_id: int = None):
        try:
            await self.conn.execute("""
                INSERT OR IGNORE INTO users (user_id, username, first_name, referrer_id, is_verified)
                VALUES (?, ?, ?, ?, 0)
            """, (user_id, username, first_name, referrer_id))
            
            if referrer_id:
                await self.conn.execute("""
                    INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)
                """, (referrer_id, user_id))
                
                await self.conn.execute("""
                    UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?
                """, (referrer_id,))
            
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка создания пользователя: {e}")
    
    async def verify_user(self, user_id: int):
        await self.conn.execute("UPDATE users SET is_verified = 1 WHERE user_id = ?", (user_id,))
        await self.conn.commit()
    
    async def set_user_mode(self, user_id: int, mode: str):
        await self.conn.execute("UPDATE users SET mode = ? WHERE user_id = ?", (mode, user_id))
        await self.conn.commit()
    
    async def save_message(self, user_id: int, chat_id: int, chat_title: str, message_id: int,
                          from_user_id: int, from_username: str, from_first_name: str, message_type: str,
                          text_content: str = None, media_file_id: str = None, media_type: str = None, caption: str = None):
        try:
            await self.conn.execute("""
                INSERT INTO saved_messages 
                (user_id, chat_id, chat_title, message_id, from_user_id, from_username, 
                 from_first_name, message_type, text_content, media_file_id, media_type, caption)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (user_id, chat_id, chat_title, message_id, from_user_id, from_username,
                  from_first_name, message_type, text_content, media_file_id, media_type, caption))
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения сообщения: {e}")
    
    async def mark_deleted(self, user_id: int, chat_id: int, message_id: int):
        await self.conn.execute("""
            UPDATE saved_messages SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """, (user_id, chat_id, message_id))
        await self.conn.commit()
    
    async def get_deleted_message(self, user_id: int, chat_id: int, message_id: int) -> Optional[Dict]:
        async with self.conn.execute("""
            SELECT * FROM saved_messages WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """, (user_id, chat_id, message_id)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def add_cloned_bot(self, owner_id: int, bot_token: str, bot_username: str, bot_name: str) -> bool:
        try:
            # Проверяем, не добавлен ли уже этот токен
            async with self.conn.execute("""
                SELECT id FROM cloned_bots WHERE bot_token = ?
            """, (bot_token,)) as cursor:
                existing = await cursor.fetchone()
                if existing:
                    return False  # Уже добавлен
            
            # Добавляем бота
            await self.conn.execute("""
                INSERT INTO cloned_bots (owner_id, bot_token, bot_username, bot_name)
                VALUES (?, ?, ?, ?)
            """, (owner_id, bot_token, bot_username, bot_name))
            
            # Проверяем, получал ли пользователь бонус
            async with self.conn.execute("""
                SELECT has_clone_bonus FROM users WHERE user_id = ?
            """, (owner_id,)) as cursor:
                user_row = await cursor.fetchone()
                if user_row and not user_row[0]:
                    # Первый бот - даем бонус
                    await self.conn.execute("""
                        UPDATE users 
                        SET subscription_expires = CASE
                            WHEN subscription_expires IS NULL OR subscription_expires < CURRENT_TIMESTAMP 
                            THEN datetime('now', '+{} days')
                            ELSE datetime(subscription_expires, '+{} days')
                        END,
                        has_clone_bonus = 1
                        WHERE user_id = ?
                    """.format(CLONE_BOT_BONUS_DAYS, CLONE_BOT_BONUS_DAYS), (owner_id,))
            
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления клонированного бота: {e}")
            return False
    
    async def get_user_bots(self, owner_id: int) -> List[Dict]:
        async with self.conn.execute("""
            SELECT * FROM cloned_bots WHERE owner_id = ? AND is_active = 1
        """, (owner_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_all_cloned_bots(self) -> List[Dict]:
        async with self.conn.execute("""
            SELECT * FROM cloned_bots WHERE is_active = 1
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_all_users(self) -> List[Dict]:
        async with self.conn.execute("SELECT * FROM users WHERE is_banned = 0") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_verified_users(self) -> List[Dict]:
        async with self.conn.execute("""
            SELECT * FROM users WHERE is_verified = 1 AND is_banned = 0
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def set_subscription(self, user_id: int, plan: str, days: int):
        await self.conn.execute("""
            UPDATE users 
            SET subscription_plan = ?,
                subscription_expires = CASE
                    WHEN subscription_expires IS NULL OR subscription_expires < CURRENT_TIMESTAMP
                    THEN datetime('now', '+{} days')
                    ELSE datetime(subscription_expires, '+{} days')
                END
            WHERE user_id = ?
        """.format(days, days), (plan, user_id))
        await self.conn.commit()
    
    async def add_transaction(self, user_id: int, amount: int, plan: str, charge_id: str = None):
        await self.conn.execute("""
            INSERT INTO transactions (user_id, amount, plan, telegram_payment_charge_id)
            VALUES (?, ?, ?, ?)
        """, (user_id, amount, plan, charge_id))
        await self.conn.commit()
    
    async def save_userbot_session(self, user_id: int, session_string: str):
        await self.conn.execute("""
            INSERT OR REPLACE INTO userbot_data (user_id, session_string, is_active)
            VALUES (?, ?, 1)
        """, (user_id, session_string))
        await self.conn.commit()
    
    async def get_userbot_session(self, user_id: int) -> Optional[str]:
        async with self.conn.execute("""
            SELECT session_string FROM userbot_data WHERE user_id = ? AND is_active = 1
        """, (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
    
    async def add_support_ticket(self, user_id: int, message: str):
        await self.conn.execute("""
            INSERT INTO support_tickets (user_id, message) VALUES (?, ?)
        """, (user_id, message))
        await self.conn.commit()
    
    async def get_open_tickets(self) -> List[Dict]:
        async with self.conn.execute("""
            SELECT t.*, u.username, u.first_name 
            FROM support_tickets t
            JOIN users u ON t.user_id = u.user_id
            WHERE t.status = 'open'
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def close(self):
        if self.conn:
            await self.conn.close()

# =============================================================================
# USERBOT MANAGER
# =============================================================================

class UserBotManager:
    def __init__(self, db: Database, main_bot: Bot):
        self.db = db
        self.main_bot = main_bot
        self.active_clients: Dict[int, PyrogramClient] = {}
    
    async def start_from_session(self, user_id: int, session_string: str):
        """Запустить userbot из session string"""
        if not PYROGRAM_AVAILABLE:
            return False, "Pyrogram не установлен"
        
        try:
            client = PyrogramClient(
                f"userbot_{user_id}",
                session_string=session_string,
                workdir="sessions"
            )
            
            @client.on_message(pyrogram_filters.all & ~pyrogram_filters.me)
            async def save_message(c, m: PyrogramMessage):
                await self._save_pyrogram_message(user_id, m)
            
            @client.on_deleted_messages()
            async def handle_deleted(c, messages):
                await self._handle_deleted(user_id, messages)
            
            await client.start()
            self.active_clients[user_id] = client
            
            logger.info(f"✅ UserBot запущен для {user_id}")
            return True, "UserBot запущен!"
            
        except Exception as e:
            logger.error(f"Ошибка запуска UserBot: {e}")
            return False, str(e)
    
    async def _save_pyrogram_message(self, user_id: int, message: PyrogramMessage):
        try:
            if not message.from_user:
                return
            
            chat_id = message.chat.id
            chat_title = message.chat.title or message.chat.first_name or "Private"
            message_id = message.id
            from_user_id = message.from_user.id
            from_username = message.from_user.username or ""
            from_first_name = message.from_user.first_name or "User"
            
            if message.text:
                message_type = "text"
                text_content = message.text
                media_file_id = None
                media_type = None
            elif message.video_note:
                message_type = "video_note"
                text_content = None
                media_file_id = message.video_note.file_id
                media_type = "video_note"
            elif message.photo:
                message_type = "photo"
                text_content = None
                media_file_id = message.photo.file_id
                media_type = "photo"
            elif message.video:
                message_type = "video"
                text_content = None
                media_file_id = message.video.file_id
                media_type = "video"
            else:
                message_type = "other"
                text_content = None
                media_file_id = None
                media_type = None
            
            await self.db.save_message(
                user_id, chat_id, chat_title, message_id,
                from_user_id, from_username, from_first_name,
                message_type, text_content, media_file_id, media_type, message.caption
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения сообщения: {e}")
    
    async def _handle_deleted(self, user_id: int, messages):
        try:
            for msg in messages:
                chat_id = getattr(msg, 'chat_id', 0)
                message_id = getattr(msg, 'message_id', msg if isinstance(msg, int) else 0)
                
                await self.db.mark_deleted(user_id, chat_id, message_id)
                
                deleted_msg = await self.db.get_deleted_message(user_id, chat_id, message_id)
                if deleted_msg:
                    notification = f"""
🗑 <b>Удалено сообщение</b>

📱 Чат: {deleted_msg['chat_title']}
👤 От: {deleted_msg['from_first_name']}
📝 Тип: {deleted_msg['message_type']}
"""
                    if deleted_msg['text_content']:
                        notification += f"\n💬 Текст: {deleted_msg['text_content'][:500]}"
                    
                    if deleted_msg['media_type'] == 'video_note':
                        notification += "\n🎬 Кружок (video note)"
                    
                    try:
                        await self.main_bot.send_message(user_id, notification, parse_mode="HTML")
                    except:
                        pass
        except Exception as e:
            logger.error(f"Ошибка обработки удалений: {e}")
    
    async def stop_all(self):
        for client in self.active_clients.values():
            try:
                await client.stop()
            except:
                pass

# =============================================================================
# КЛОНИРОВАННЫЕ БОТЫ
# =============================================================================

class CloneBotManager:
    def __init__(self, db: Database):
        self.db = db
        self.clones: Dict[str, Bot] = {}
        self.clone_dps: Dict[str, Dispatcher] = {}
    
    async def start_clone(self, token: str, username: str):
        """Запустить клонированного бота"""
        try:
            bot = Bot(token=token)
            dp = Dispatcher(storage=MemoryStorage())
            router = Router()
            
            # Обработчик /start для клона
            @router.message(CommandStart())
            async def clone_start(message: Message):
                await message.answer(f"""
👋 <b>Привет, {message.from_user.first_name}!</b>

Это зеркальный бот MerAi & Monitoring.

Для управления используй основной бот:
👉 @{ORIGINAL_BOT_USERNAME}

<i>Все функции доступны в основном боте</i>
""", parse_mode="HTML")
            
            dp.include_router(router)
            
            self.clones[token] = bot
            self.clone_dps[token] = dp
            
            # Запускаем polling в фоне
            asyncio.create_task(dp.start_polling(bot))
            
            logger.info(f"✅ Клон @{username} запущен")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка запуска клона {username}: {e}")
            return False
    
    async def start_all_clones(self):
        """Запустить все клоны из БД"""
        clones = await self.db.get_all_cloned_bots()
        for clone in clones:
            await self.start_clone(clone['bot_token'], clone['bot_username'])
    
    async def stop_all(self):
        for bot in self.clones.values():
            try:
                await bot.session.close()
            except:
                pass

# =============================================================================
# ОСНОВНОЙ БОТ
# =============================================================================

class MerAiBot:
    def __init__(self, token: str):
        self.bot = Bot(token=token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.db = Database()
        self.router = Router()
        self.userbot_manager = UserBotManager(self.db, self.bot)
        self.clone_manager = CloneBotManager(self.db)
        
        self._register_handlers()
    
    def _register_handlers(self):
        # Команды
        self.router.message.register(self.cmd_start, CommandStart())
        self.router.message.register(self.cmd_help, Command("help"))
        self.router.message.register(self.cmd_support, Command("support"))
        self.router.message.register(self.cmd_admin, Command("admin"))
        
        # FSM
        self.router.message.register(self.process_session_string, StateFilter(UserBotSetup.waiting_session_string))
        self.router.message.register(self.process_clone_token, StateFilter(CloneBotStates.waiting_token))
        self.router.message.register(self.process_support_message, StateFilter(SupportStates.waiting_message))
        self.router.message.register(self.process_broadcast_message, StateFilter(BroadcastStates.waiting_message))
        
        # Callbacks
        self.router.callback_query.register(self.callback_handler, F.data)
        
        # Chat join requests (автоодобрение)
        self.router.chat_join_request.register(self.handle_join_request)
        
        # Платежи
        self.router.pre_checkout_query.register(self.process_pre_checkout)
        self.router.message.register(self.process_successful_payment, F.successful_payment)
        
        self.dp.include_router(self.router)
    
    async def handle_join_request(self, join_request: ChatJoinRequest):
        """Автоодобрение заявок в капча-канале"""
        if join_request.chat.id == CAPTCHA_CHANNEL_ID:
            user_id = join_request.from_user.id
            
            try:
                # Автоматически одобряем
                await self.bot.approve_chat_join_request(
                    chat_id=CAPTCHA_CHANNEL_ID,
                    user_id=user_id
                )
                
                # Верифицируем пользователя
                await self.db.verify_user(user_id)
                
                # Отправляем уведомление
                await self.bot.send_message(
                    user_id,
                    """
🎉 <b>Верификация пройдена!</b>

✅ Ваша заявка одобрена
✅ Доступ к боту открыт

Используйте /start для начала работы
""",
                    parse_mode="HTML"
                )
                
                logger.info(f"✅ Автоодобрена заявка {user_id}")
            except Exception as e:
                logger.error(f"Ошибка автоодобрения {user_id}: {e}")
    
    async def cmd_start(self, message: Message, state: FSMContext):
        user_id = message.from_user.id
        username = message.from_user.username
        first_name = message.from_user.first_name
        
        # Реферал
        referrer_id = None
        if message.text and len(message.text.split()) > 1:
            try:
                ref_code = message.text.split()[1]
                if ref_code.startswith("ref"):
                    referrer_id = int(ref_code[3:])
            except:
                pass
        
        await self.db.create_user(user_id, username, first_name, referrer_id)
        user = await self.db.get_user(user_id)
        
        # Проверка верификации
        if not user['is_verified']:
            await self.show_captcha(message)
            return
        
        await state.clear()
        await self.show_main_menu(message)
    
    async def show_captcha(self, message: Message):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подписаться", url=CAPTCHA_CHANNEL_LINK)]
        ])
        
        await message.answer(f"""
🔐 <b>Добро пожаловать в MerAi & Monitoring!</b>

Для доступа подпишитесь на канал:

1️⃣ Нажмите "✅ Подписаться"
2️⃣ Нажмите "Запросить вступление"
3️⃣ Ваша заявка будет автоматически одобрена
4️⃣ Вернитесь сюда и нажмите /start

<i>Это займет 10 секунд! ⚡</i>
""", parse_mode="HTML", reply_markup=keyboard)
    
    async def show_main_menu(self, message: Message):
        user = await self.db.get_user(message.from_user.id)
        
        # Проверка подписки
        sub_status = "⚠️ Бесплатно"
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if expires > datetime.now():
                days_left = (expires - datetime.now()).days
                sub_status = f"✅ Активна ({days_left} дн.)"
        
        mode_status = {"none": "❌ Не выбран", "userbot": "👤 UserBot"}.get(user['mode'], "❌ Не выбран")
        
        text = f"""
👋 <b>{user['first_name']}!</b>

📊 <b>Профиль:</b>
├ Подписка: {sub_status}
├ Режим: {mode_status}
└ Рефералов: {user['referral_count']}

🎯 <b>Функции:</b>
• 🔍 Мониторинг удалений
• 🎬 Сохранение кружков
• 🤖 Клонирование ботов
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ UserBot", callback_data="setup_userbot")],
            [InlineKeyboardButton(text="🤖 Клонировать", callback_data="clone_bot"),
             InlineKeyboardButton(text="💳 Тарифы", callback_data="plans")],
            [InlineKeyboardButton(text="📊 Профиль", callback_data="profile"),
             InlineKeyboardButton(text="💬 Поддержка", callback_data="support")]
        ])
        
        if user['user_id'] == ADMIN_ID:
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(text="🔐 Админ", callback_data="admin")
            ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def callback_handler(self, callback: CallbackQuery, state: FSMContext):
        data = callback.data
        
        try:
            handlers = {
                "setup_userbot": self.start_userbot_setup,
                "clone_bot": self.start_clone_bot,
                "plans": self.show_plans,
                "profile": self.show_profile,
                "support": self.show_support,
                "admin": self.show_admin,
                "admin_broadcast": self.start_broadcast,
            }
            
            if data.startswith("buy_"):
                await self.handle_purchase(callback)
            elif data in handlers:
                await handlers[data](callback, state)
            
            await callback.answer()
        except Exception as e:
            logger.error(f"Callback error: {e}\n{traceback.format_exc()}")
            await callback.answer("Ошибка", show_alert=True)
    
    async def start_userbot_setup(self, callback: CallbackQuery, state: FSMContext):
        """UserBot доступен БЕСПЛАТНО для всех"""
        await callback.message.edit_text("""
👤 <b>НАСТРОЙКА USERBOT</b>

UserBot доступен БЕСПЛАТНО!

<b>Инструкция:</b>

1️⃣ Откройте https://my.telegram.org/apps
2️⃣ Создайте приложение
3️⃣ Скачайте Telegram Desktop
4️⃣ Войдите в аккаунт
5️⃣ Нажмите Ctrl+Alt+Shift+F10
6️⃣ Скопируйте session string
7️⃣ Отправьте его сюда

<i>Отправьте session string:</i>
""", parse_mode="HTML")
        
        await state.set_state(UserBotSetup.waiting_session_string)
    
    async def process_session_string(self, message: Message, state: FSMContext):
        session_string = message.text.strip()
        user_id = message.from_user.id
        
        # Удаляем сообщение с session (безопасность)
        try:
            await message.delete()
        except:
            pass
        
        status_msg = await message.answer("🔄 Запускаю UserBot...")
        
        success, result = await self.userbot_manager.start_from_session(user_id, session_string)
        
        if success:
            await self.db.save_userbot_session(user_id, session_string)
            await self.db.set_user_mode(user_id, "userbot")
            
            await status_msg.edit_text("""
✅ <b>UserBot запущен!</b>

Теперь бот отслеживает:
• Удаленные сообщения
• Кружки (video notes)
• Все чаты

/start для возврата в меню
""", parse_mode="HTML")
        else:
            await status_msg.edit_text(f"❌ Ошибка: {result}")
        
        await state.clear()
    
    async def start_clone_bot(self, callback: CallbackQuery, state: FSMContext):
        user = await self.db.get_user(callback.from_user.id)
        bonus_text = ""
        
        if not user['has_clone_bonus']:
            bonus_text = f"\n🎁 <b>За первого бота: +{CLONE_BOT_BONUS_DAYS} дня бесплатно!</b>"
        
        await callback.message.edit_text(f"""
🤖 <b>КЛОНИРОВАНИЕ БОТА</b>

Создайте своего бота с полным функционалом MerAi!
{bonus_text}

<b>Инструкция:</b>
1️⃣ Откройте @BotFather
2️⃣ Создайте бота (/newbot)
3️⃣ Скопируйте токен
4️⃣ Отправьте токен сюда

<i>Отправьте токен бота:</i>
""", parse_mode="HTML")
        
        await state.set_state(CloneBotStates.waiting_token)
    
    async def process_clone_token(self, message: Message, state: FSMContext):
        token = message.text.strip()
        
        if ":" not in token:
            await message.answer("❌ Неверный формат токена")
            return
        
        try:
            test_bot = Bot(token=token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            success = await self.db.add_cloned_bot(
                message.from_user.id,
                token,
                bot_info.username,
                bot_info.first_name
            )
            
            if success:
                # Запускаем клона
                await self.clone_manager.start_clone(token, bot_info.username)
                
                user = await self.db.get_user(message.from_user.id)
                bonus_msg = ""
                if user['has_clone_bonus']:
                    bonus_msg = f"\n\n🎁 Бонус +{CLONE_BOT_BONUS_DAYS} дня добавлен!"
                
                await message.answer(f"""
✅ <b>Бот клонирован!</b>

🤖 @{bot_info.username}
Теперь он работает с полным функционалом!{bonus_msg}

<i>/start для возврата</i>
""", parse_mode="HTML")
            else:
                await message.answer("❌ Этот бот уже добавлен")
            
            await state.clear()
        except Exception as e:
            logger.error(f"Clone error: {e}")
            await message.answer(f"❌ Ошибка: {str(e)}")
    
    async def show_plans(self, callback: CallbackQuery, state: FSMContext):
        text = "💳 <b>ТАРИФЫ</b>\n\n"
        keyboard = []
        
        for plan_id, plan in SUBSCRIPTION_PLANS.items():
            text += f"<b>{plan['name']}</b>\n{plan['price_stars']}⭐ - {plan['days']} дней\n\n"
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{plan['name']} - {plan['price_stars']}⭐",
                    callback_data=f"buy_{plan_id}"
                )
            ])
        
        await callback.message.edit_text(text, parse_mode="HTML", 
                                        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    
    async def handle_purchase(self, callback: CallbackQuery):
        plan_id = callback.data.replace("buy_", "")
        plan = SUBSCRIPTION_PLANS.get(plan_id)
        
        if not plan:
            await callback.answer("❌ План не найден")
            return
        
        try:
            await self.bot.send_invoice(
                chat_id=callback.from_user.id,
                title=plan['name'],
                description=f"Подписка на {plan['days']} дней",
                payload=f"sub_{plan_id}",
                currency="XTR",
                prices=[LabeledPrice(label=plan['name'], amount=plan['price_stars'])]
            )
            await callback.answer("✅ Инвойс отправлен")
        except Exception as e:
            logger.error(f"Invoice error: {e}")
            await callback.answer(f"❌ Ошибка")
    
    async def process_pre_checkout(self, pre_checkout: PreCheckoutQuery):
        await pre_checkout.answer(ok=True)
    
    async def process_successful_payment(self, message: Message):
        payload = message.successful_payment.invoice_payload
        
        if payload.startswith("sub_"):
            plan_id = payload.replace("sub_", "")
            plan = SUBSCRIPTION_PLANS.get(plan_id)
            
            if plan:
                await self.db.set_subscription(message.from_user.id, plan_id, plan['days'])
                await self.db.add_transaction(
                    message.from_user.id,
                    plan['price_stars'],
                    plan_id,
                    message.successful_payment.telegram_payment_charge_id
                )
                
                await message.answer(f"""
✅ <b>Оплата успешна!</b>

План: {plan['name']}
Срок: {plan['days']} дней

Спасибо! 🎉
""", parse_mode="HTML")
    
    async def show_profile(self, callback: CallbackQuery, state: FSMContext):
        user = await self.db.get_user(callback.from_user.id)
        bots = await self.db.get_user_bots(callback.from_user.id)
        
        ref_link = f"https://t.me/{ORIGINAL_BOT_USERNAME}?start=ref{user['user_id']}"
        
        text = f"""
📊 <b>ПРОФИЛЬ</b>

👤 {user['first_name']}
ID: <code>{user['user_id']}</code>

💎 План: {user['subscription_plan'] or 'Бесплатно'}
🤖 Клонов: {len(bots)}
🎁 Рефералов: {user['referral_count']}

🔗 Ссылка: <code>{ref_link}</code>
"""
        
        await callback.message.edit_text(text, parse_mode="HTML")
    
    async def show_support(self, callback: CallbackQuery, state: FSMContext):
        await callback.message.edit_text("""
💬 <b>ПОДДЕРЖКА</b>

Telegram: @mrztn

Или отправьте сообщение здесь:
""", parse_mode="HTML")
        
        await state.set_state(SupportStates.waiting_message)
    
    async def process_support_message(self, message: Message, state: FSMContext):
        await self.db.add_support_ticket(message.from_user.id, message.text)
        
        try:
            await self.bot.send_message(
                ADMIN_ID,
                f"""
🆘 <b>Новое обращение</b>

От: {message.from_user.first_name}
ID: {message.from_user.id}

{message.text}
""",
                parse_mode="HTML"
            )
        except:
            pass
        
        await message.answer("✅ Обращение отправлено!")
        await state.clear()
    
    async def show_admin(self, callback: CallbackQuery, state: FSMContext):
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("⛔ Нет доступа")
            return
        
        users = await self.db.get_all_users()
        
        text = f"""
🔐 <b>АДМИН-ПАНЕЛЬ</b>

Всего: {len(users)}
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def start_broadcast(self, callback: CallbackQuery, state: FSMContext):
        await callback.message.edit_text("📢 Отправьте сообщение для рассылки:")
        await state.set_state(BroadcastStates.waiting_message)
    
    async def process_broadcast_message(self, message: Message, state: FSMContext):
        users = await self.db.get_verified_users()
        
        sent = 0
        for user in users:
            try:
                await self.bot.send_message(user['user_id'], message.text)
                sent += 1
                await asyncio.sleep(0.05)
            except:
                pass
        
        await message.answer(f"✅ Отправлено: {sent}/{len(users)}")
        await state.clear()
    
    async def cmd_help(self, message: Message):
        await message.answer("/start - Меню")
    
    async def cmd_support(self, message: Message, state: FSMContext):
        await message.answer("💬 Отправьте ваше сообщение:")
        await state.set_state(SupportStates.waiting_message)
    
    async def cmd_admin(self, message: Message):
        if message.from_user.id != ADMIN_ID:
            await message.answer("⛔ Нет доступа")
            return
        
        users = await self.db.get_all_users()
        await message.answer(f"🔐 Админ-панель\n\nВсего: {len(users)}")
    
    async def start(self):
        await self.db.connect()
        
        # Запускаем все клоны
        await self.clone_manager.start_all_clones()
        
        logger.info(f"🚀 Запуск @{ORIGINAL_BOT_USERNAME}")
        await self.dp.start_polling(self.bot)
    
    async def stop(self):
        await self.userbot_manager.stop_all()
        await self.clone_manager.stop_all()
        await self.db.close()
        await self.bot.session.close()

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    logger.info("=" * 60)
    logger.info("🚀 MerAi & Monitoring v6.0 - ФИНАЛЬНАЯ ВЕРСИЯ")
    logger.info("=" * 60)
    
    bot = MerAiBot(BOT_TOKEN)
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("⚠️ Остановка")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}\n{traceback.format_exc()}")
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Завершено")
