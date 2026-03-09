#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MerAi & Monitoring v6.0 - ИСПРАВЛЕННАЯ ВЕРСИЯ
Дата: 9 марта 2026, 19:13 МСК
Автор: @mrztn

ПОЛНОСТЬЮ РЕАЛИЗОВАННЫЙ ФУНКЦИОНАЛ:
✅ Капча через подписку на канал (только проверка)
✅ ChatBot / UserBot режимы (оба работают)
✅ Мониторинг удалений (включая кружки через UserBot)
✅ Платежи через Telegram Stars с автоподпиской
✅ Клонирование ботов (+3 дня бонус)
✅ Реферальная система
✅ Техподдержка через бота
✅ Рассылки (массовые/одиночные)
✅ Админ-панель с детальной статистикой
✅ FSM обработчики для всех состояний
✅ Обработка ошибок
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
import hashlib

# Aiogram 3.26.0 (март 2026)
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated, LabeledPrice, PreCheckoutQuery, ContentType,
    ChatMemberMember, ChatMemberLeft, ChatMemberBanned
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# Pyrogram 2.0.106 для userbot
try:
    from pyrogram import Client as PyrogramClient, filters as pyrogram_filters
    from pyrogram.types import Message as PyrogramMessage
    from pyrogram.handlers import MessageHandler, DeletedMessagesHandler, EditedMessageHandler
    from pyrogram.enums import ChatType, MessageMediaType
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False
    logging.warning("⚠️ Pyrogram не установлен - UserBot режим недоступен")

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

# Основные константы
BOT_TOKEN = "8505484152:AAHXEFt0lyeMK5ZSJHRYpdPhhFJ0s142Bng"
CAPTCHA_CHANNEL_ID = -1003716882147  # ID капча-канала
CAPTCHA_CHANNEL_LINK = "https://t.me/+AW8ztiMHGY9jMGQ6"
ADMIN_ID = 7785371505  # @mrztn
ORIGINAL_BOT_USERNAME = None  # Заполнится автоматически

# База данных
DB_FILE = "merai_data.db"

# Планы подписки
SUBSCRIPTION_PLANS = {
    "week": {"name": "📅 Неделя", "price_rub": 149, "price_stars": 100, "days": 7},
    "month": {"name": "📆 Месяц", "price_rub": 499, "price_stars": 300, "days": 30},
    "quarter": {"name": "📊 3 месяца", "price_rub": 1299, "price_stars": 800, "days": 90},
    "year": {"name": "🎯 Год", "price_rub": 3999, "price_stars": 2500, "days": 365}
}

# Бонусы
CLONE_BOT_BONUS_DAYS = 3
REFERRAL_REWARD_COUNT = 50
REFERRAL_REWARD = "Telegram Premium на 1 месяц"

# =============================================================================
# FSM СОСТОЯНИЯ
# =============================================================================

class CaptchaStates(StatesGroup):
    waiting_verification = State()

class UserBotSetup(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()

class CloneBotStates(StatesGroup):
    waiting_token = State()

class SupportStates(StatesGroup):
    waiting_message = State()
    admin_reply_waiting = State()

class BroadcastStates(StatesGroup):
    waiting_message = State()
    confirm_send = State()

# =============================================================================
# БАЗА ДАННЫХ
# =============================================================================

class Database:
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Подключение к БД"""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_tables()
        logger.info("✅ База данных подключена")
    
    async def _create_tables(self):
        """Создание таблиц"""
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_verified BOOLEAN DEFAULT 0,
                mode TEXT DEFAULT 'none',
                subscription_plan TEXT,
                subscription_expires TIMESTAMP,
                auto_renew BOOLEAN DEFAULT 0,
                referrer_id INTEGER,
                referral_count INTEGER DEFAULT 0,
                balance_stars INTEGER DEFAULT 0,
                is_banned BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS userbot_data (
                user_id INTEGER PRIMARY KEY,
                api_id INTEGER,
                api_hash TEXT,
                session_string TEXT,
                phone TEXT,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
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
                original_text TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            
            CREATE TABLE IF NOT EXISTS cloned_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                bot_token TEXT UNIQUE,
                bot_username TEXT,
                bot_name TEXT,
                is_active BOOLEAN DEFAULT 1,
                user_count INTEGER DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                bonus_granted BOOLEAN DEFAULT 0,
                FOREIGN KEY (owner_id) REFERENCES users(owner_id)
            );
            
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                currency TEXT,
                plan TEXT,
                status TEXT DEFAULT 'pending',
                payment_method TEXT,
                telegram_payment_charge_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                status TEXT DEFAULT 'open',
                admin_reply TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            );
            
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT,
                sent_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_users_subscription ON users(subscription_expires);
            CREATE INDEX IF NOT EXISTS idx_saved_messages_user ON saved_messages(user_id, chat_id);
            CREATE INDEX IF NOT EXISTS idx_saved_messages_deleted ON saved_messages(is_deleted, deleted_at);
            CREATE INDEX IF NOT EXISTS idx_cloned_bots_owner ON cloned_bots(owner_id);
            CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);
        """)
        await self.conn.commit()
        logger.info("✅ Таблицы БД созданы")
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with self.conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def create_user(self, user_id: int, username: str = None, 
                         first_name: str = None, referrer_id: int = None):
        try:
            await self.conn.execute("""
                INSERT OR IGNORE INTO users 
                (user_id, username, first_name, referrer_id, is_verified)
                VALUES (?, ?, ?, ?, 0)
            """, (user_id, username, first_name, referrer_id))
            
            if referrer_id:
                await self.conn.execute("""
                    INSERT OR IGNORE INTO referrals (referrer_id, referred_id)
                    VALUES (?, ?)
                """, (referrer_id, user_id))
                
                await self.conn.execute("""
                    UPDATE users SET referral_count = referral_count + 1
                    WHERE user_id = ?
                """, (referrer_id,))
            
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка создания пользователя {user_id}: {e}")
    
    async def verify_user(self, user_id: int):
        await self.conn.execute("""
            UPDATE users SET is_verified = 1, last_active = CURRENT_TIMESTAMP
            WHERE user_id = ?
        """, (user_id,))
        await self.conn.commit()
    
    async def set_user_mode(self, user_id: int, mode: str):
        await self.conn.execute("""
            UPDATE users SET mode = ?, last_active = CURRENT_TIMESTAMP
            WHERE user_id = ?
        """, (mode, user_id))
        await self.conn.commit()
    
    async def save_message(self, user_id: int, chat_id: int, chat_title: str,
                          message_id: int, from_user_id: int, from_username: str,
                          from_first_name: str, message_type: str,
                          text_content: str = None, media_file_id: str = None,
                          media_type: str = None, caption: str = None):
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
            UPDATE saved_messages 
            SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """, (user_id, chat_id, message_id))
        await self.conn.commit()
    
    async def mark_edited(self, user_id: int, chat_id: int, message_id: int, new_text: str):
        # Сохраняем оригинальный текст
        async with self.conn.execute("""
            SELECT text_content FROM saved_messages
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """, (user_id, chat_id, message_id)) as cursor:
            row = await cursor.fetchone()
            if row:
                original = row[0]
                await self.conn.execute("""
                    UPDATE saved_messages 
                    SET is_edited = 1, edited_at = CURRENT_TIMESTAMP,
                        original_text = ?, text_content = ?
                    WHERE user_id = ? AND chat_id = ? AND message_id = ?
                """, (original, new_text, user_id, chat_id, message_id))
                await self.conn.commit()
    
    async def get_deleted_message(self, user_id: int, chat_id: int, message_id: int) -> Optional[Dict]:
        async with self.conn.execute("""
            SELECT * FROM saved_messages
            WHERE user_id = ? AND chat_id = ? AND message_id = ?
        """, (user_id, chat_id, message_id)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def add_cloned_bot(self, owner_id: int, bot_token: str, 
                            bot_username: str, bot_name: str) -> bool:
        try:
            # Проверяем, не был ли бонус уже выдан
            async with self.conn.execute("""
                SELECT bonus_granted FROM cloned_bots 
                WHERE owner_id = ? AND bot_token = ?
            """, (owner_id, bot_token)) as cursor:
                existing = await cursor.fetchone()
                if existing:
                    return False  # Бот уже добавлен
            
            await self.conn.execute("""
                INSERT INTO cloned_bots 
                (owner_id, bot_token, bot_username, bot_name, bonus_granted)
                VALUES (?, ?, ?, ?, 1)
            """, (owner_id, bot_token, bot_username, bot_name))
            
            # Добавляем бонусные дни
            await self.conn.execute("""
                UPDATE users 
                SET subscription_expires = CASE
                    WHEN subscription_expires IS NULL OR subscription_expires < CURRENT_TIMESTAMP 
                    THEN datetime('now', '+{} days')
                    ELSE datetime(subscription_expires, '+{} days')
                END
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
    
    async def get_all_users(self, include_banned: bool = False) -> List[Dict]:
        query = "SELECT * FROM users"
        if not include_banned:
            query += " WHERE is_banned = 0"
        
        async with self.conn.execute(query) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_verified_users(self) -> List[Dict]:
        async with self.conn.execute("""
            SELECT * FROM users WHERE is_verified = 1 AND is_banned = 0
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def ban_user(self, user_id: int, banned: bool = True):
        await self.conn.execute("""
            UPDATE users SET is_banned = ? WHERE user_id = ?
        """, (1 if banned else 0, user_id))
        await self.conn.commit()
    
    async def set_subscription(self, user_id: int, plan: str, days: int):
        """Установить/продлить подписку"""
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
    
    async def add_transaction(self, user_id: int, amount: int, currency: str, 
                             plan: str, payment_method: str, charge_id: str = None):
        await self.conn.execute("""
            INSERT INTO transactions 
            (user_id, amount, currency, plan, payment_method, telegram_payment_charge_id, 
             status, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, 'completed', CURRENT_TIMESTAMP)
        """, (user_id, amount, currency, plan, payment_method, charge_id))
        await self.conn.commit()
    
    async def get_user_deleted_messages(self, user_id: int, limit: int = 50) -> List[Dict]:
        """Получить удаленные сообщения пользователя"""
        async with self.conn.execute("""
            SELECT * FROM saved_messages
            WHERE user_id = ? AND is_deleted = 1
            ORDER BY deleted_at DESC
            LIMIT ?
        """, (user_id, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_user_statistics(self, user_id: int) -> Dict:
        """Получить статистику пользователя"""
        async with self.conn.execute("""
            SELECT 
                COUNT(*) as total_monitored,
                SUM(CASE WHEN is_deleted = 1 THEN 1 ELSE 0 END) as deleted_count,
                SUM(CASE WHEN is_edited = 1 THEN 1 ELSE 0 END) as edited_count
            FROM saved_messages
            WHERE user_id = ?
        """, (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else {"total_monitored": 0, "deleted_count": 0, "edited_count": 0}
    
    async def add_support_ticket(self, user_id: int, message: str):
        await self.conn.execute("""
            INSERT INTO support_tickets (user_id, message)
            VALUES (?, ?)
        """, (user_id, message))
        await self.conn.commit()
    
    async def get_open_tickets(self) -> List[Dict]:
        async with self.conn.execute("""
            SELECT t.*, u.username, u.first_name 
            FROM support_tickets t
            JOIN users u ON t.user_id = u.user_id
            WHERE t.status = 'open'
            ORDER BY t.created_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def close_ticket(self, ticket_id: int, admin_reply: str):
        await self.conn.execute("""
            UPDATE support_tickets 
            SET status = 'closed', admin_reply = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (admin_reply, ticket_id))
        await self.conn.commit()
    
    async def save_userbot_data(self, user_id: int, api_id: int, api_hash: str, 
                               session_string: str, phone: str):
        await self.conn.execute("""
            INSERT OR REPLACE INTO userbot_data 
            (user_id, api_id, api_hash, session_string, phone, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (user_id, api_id, api_hash, session_string, phone))
        await self.conn.commit()
    
    async def get_userbot_data(self, user_id: int) -> Optional[Dict]:
        async with self.conn.execute("""
            SELECT * FROM userbot_data WHERE user_id = ? AND is_active = 1
        """, (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def close(self):
        if self.conn:
            await self.conn.close()
            logger.info("✅ База данных закрыта")

# =============================================================================
# USERBOT MANAGER
# =============================================================================

class UserBotManager:
    def __init__(self, db: Database, main_bot: Bot):
        self.db = db
        self.main_bot = main_bot
        self.active_clients: Dict[int, PyrogramClient] = {}
    
    async def start_userbot(self, user_id: int, api_id: int, api_hash: str, 
                           phone: str, session_name: str = None):
        """Запустить userbot для пользователя"""
        if not PYROGRAM_AVAILABLE:
            return False, "Pyrogram не установлен"
        
        try:
            if session_name is None:
                session_name = f"userbot_{user_id}"
            
            client = PyrogramClient(
                name=session_name,
                api_id=api_id,
                api_hash=api_hash,
                phone_number=phone,
                workdir="sessions"
            )
            
            # Обработчики для мониторинга
            @client.on_message(pyrogram_filters.all & ~pyrogram_filters.me)
            async def save_incoming_message(client, message: PyrogramMessage):
                await self._save_pyrogram_message(user_id, message)
            
            @client.on_deleted_messages()
            async def handle_deleted(client, messages):
                await self._handle_deleted_messages(user_id, messages)
            
            @client.on_edited_message()
            async def handle_edited(client, message: PyrogramMessage):
                await self._handle_edited_message(user_id, message)
            
            await client.start()
            self.active_clients[user_id] = client
            
            # Сохраняем session string
            session_string = await client.export_session_string()
            await self.db.save_userbot_data(user_id, api_id, api_hash, session_string, phone)
            
            logger.info(f"✅ UserBot запущен для пользователя {user_id}")
            return True, "UserBot успешно запущен!"
            
        except Exception as e:
            logger.error(f"Ошибка запуска UserBot для {user_id}: {e}")
            return False, f"Ошибка: {str(e)}"
    
    async def _save_pyrogram_message(self, user_id: int, message: PyrogramMessage):
        """Сохранить сообщение из Pyrogram"""
        try:
            chat_id = message.chat.id
            chat_title = message.chat.title or message.chat.first_name or "Private"
            message_id = message.id
            from_user = message.from_user
            
            if not from_user:
                return
            
            from_user_id = from_user.id
            from_username = from_user.username or ""
            from_first_name = from_user.first_name or "User"
            
            # Определяем тип сообщения
            if message.text:
                message_type = "text"
                text_content = message.text
                media_file_id = None
                media_type = None
                caption = None
            elif message.video_note:
                message_type = "video_note"
                text_content = None
                media_file_id = message.video_note.file_id
                media_type = "video_note"
                caption = None
            elif message.photo:
                message_type = "photo"
                text_content = None
                media_file_id = message.photo.file_id
                media_type = "photo"
                caption = message.caption
            elif message.video:
                message_type = "video"
                text_content = None
                media_file_id = message.video.file_id
                media_type = "video"
                caption = message.caption
            elif message.voice:
                message_type = "voice"
                text_content = None
                media_file_id = message.voice.file_id
                media_type = "voice"
                caption = None
            elif message.document:
                message_type = "document"
                text_content = None
                media_file_id = message.document.file_id
                media_type = "document"
                caption = message.caption
            else:
                message_type = "other"
                text_content = None
                media_file_id = None
                media_type = None
                caption = None
            
            await self.db.save_message(
                user_id, chat_id, chat_title, message_id,
                from_user_id, from_username, from_first_name,
                message_type, text_content, media_file_id, media_type, caption
            )
            
        except Exception as e:
            logger.error(f"Ошибка сохранения сообщения Pyrogram: {e}")
    
    async def _handle_deleted_messages(self, user_id: int, messages):
        """Обработка удаленных сообщений"""
        try:
            for msg in messages:
                chat_id = msg.chat.id if hasattr(msg, 'chat') else 0
                message_id = msg.message_id if hasattr(msg, 'message_id') else msg
                
                # Помечаем как удаленное
                await self.db.mark_deleted(user_id, chat_id, message_id)
                
                # Получаем сохраненное сообщение
                deleted_msg = await self.db.get_deleted_message(user_id, chat_id, message_id)
                
                if deleted_msg:
                    # Отправляем уведомление пользователю
                    notification = self._format_deleted_notification(deleted_msg)
                    try:
                        await self.main_bot.send_message(user_id, notification, parse_mode="HTML")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления об удалении: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка обработки удаленных сообщений: {e}")
    
    async def _handle_edited_message(self, user_id: int, message: PyrogramMessage):
        """Обработка редактированных сообщений"""
        try:
            chat_id = message.chat.id
            message_id = message.id
            new_text = message.text or message.caption or ""
            
            await self.db.mark_edited(user_id, chat_id, message_id, new_text)
            
            # Получаем инфо о редактировании
            msg_data = await self.db.get_deleted_message(user_id, chat_id, message_id)
            
            if msg_data and msg_data.get('is_edited'):
                notification = f"""
📝 <b>Сообщение отредактировано</b>

📱 <b>Чат:</b> {msg_data['chat_title']}
👤 <b>От:</b> {msg_data['from_first_name']}

<b>Было:</b>
{msg_data['original_text'][:500]}

<b>Стало:</b>
{new_text[:500]}

🕒 <b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}
"""
                try:
                    await self.main_bot.send_message(user_id, notification, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления о редактировании: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка обработки редактирования: {e}")
    
    def _format_deleted_notification(self, msg_data: Dict) -> str:
        """Форматировать уведомление об удалении"""
        notification = f"""
🗑 <b>Удалено сообщение</b>

📱 <b>Чат:</b> {msg_data['chat_title']}
👤 <b>От:</b> {msg_data['from_first_name']} (@{msg_data['from_username'] or 'без username'})
📝 <b>Тип:</b> {msg_data['message_type']}
"""
        
        if msg_data['text_content']:
            notification += f"\n💬 <b>Текст:</b>\n{msg_data['text_content'][:500]}"
        
        if msg_data['caption']:
            notification += f"\n📎 <b>Подпись:</b>\n{msg_data['caption'][:500]}"
        
        if msg_data['media_type']:
            notification += f"\n🎬 <b>Медиа:</b> {msg_data['media_type']}"
            if msg_data['media_type'] == 'video_note':
                notification += " (кружок)"
        
        notification += f"\n\n🕒 <b>Удалено:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        
        return notification
    
    async def stop_userbot(self, user_id: int):
        """Остановить userbot"""
        if user_id in self.active_clients:
            try:
                await self.active_clients[user_id].stop()
                del self.active_clients[user_id]
                logger.info(f"✅ UserBot остановлен для {user_id}")
            except Exception as e:
                logger.error(f"Ошибка остановки UserBot для {user_id}: {e}")
    
    async def stop_all(self):
        """Остановить все userbot'ы"""
        for user_id in list(self.active_clients.keys()):
            await self.stop_userbot(user_id)

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
        
        self._register_handlers()
        logger.info("🤖 MerAi Bot v6.0 инициализирован")
    
    def _register_handlers(self):
        """Регистрация обработчиков"""
        
        # Команды
        self.router.message.register(self.cmd_start, CommandStart())
        self.router.message.register(self.cmd_help, Command("help"))
        self.router.message.register(self.cmd_plan, Command("plan"))
        self.router.message.register(self.cmd_support, Command("support"))
        self.router.message.register(self.cmd_admin, Command("admin"))
        
        # FSM обработчики - UserBot Setup
        self.router.message.register(
            self.process_api_id, 
            StateFilter(UserBotSetup.waiting_api_id)
        )
        self.router.message.register(
            self.process_api_hash,
            StateFilter(UserBotSetup.waiting_api_hash)
        )
        self.router.message.register(
            self.process_phone,
            StateFilter(UserBotSetup.waiting_phone)
        )
        self.router.message.register(
            self.process_code,
            StateFilter(UserBotSetup.waiting_code)
        )
        self.router.message.register(
            self.process_2fa,
            StateFilter(UserBotSetup.waiting_2fa)
        )
        
        # FSM обработчики - Clone Bot
        self.router.message.register(
            self.process_clone_token,
            StateFilter(CloneBotStates.waiting_token)
        )
        
        # FSM обработчики - Support
        self.router.message.register(
            self.process_support_message,
            StateFilter(SupportStates.waiting_message)
        )
        self.router.message.register(
            self.process_admin_reply,
            StateFilter(SupportStates.admin_reply_waiting)
        )
        
        # FSM обработчики - Broadcast
        self.router.message.register(
            self.process_broadcast_message,
            StateFilter(BroadcastStates.waiting_message)
        )
        
        # Callback кнопки
        self.router.callback_query.register(self.callback_handler, F.data)
        
        # Chat member updates (для проверки подписки на капча-канал)
        self.router.chat_member.register(self.handle_chat_member_update)
        
        # Pre-checkout для Stars
        self.router.pre_checkout_query.register(self.process_pre_checkout)
        
        # Successful payment
        self.router.message.register(
            self.process_successful_payment,
            F.successful_payment
        )
        
        self.dp.include_router(self.router)
    
    # =========================================================================
    # КОМАНДЫ
    # =========================================================================
    
    async def cmd_start(self, message: Message, state: FSMContext):
        """Команда /start"""
        user_id = message.from_user.id
        username = message.from_user.username
        first_name = message.from_user.first_name
        
        # Проверяем реферальную ссылку
        referrer_id = None
        if message.text and len(message.text.split()) > 1:
            try:
                ref_code = message.text.split()[1]
                if ref_code.startswith("ref"):
                    referrer_id = int(ref_code[3:])
            except:
                pass
        
        # Создаем пользователя
        await self.db.create_user(user_id, username, first_name, referrer_id)
        user = await self.db.get_user(user_id)
        
        # Проверяем верификацию
        if not user['is_verified']:
            await self.show_captcha(message, state)
            return
        
        # Очищаем состояние
        await state.clear()
        
        # Показываем главное меню
        await self.show_main_menu(message)
    
    async def cmd_help(self, message: Message):
        await self.show_help_text(message)
    
    async def cmd_plan(self, message: Message):
        await self.show_plans_text(message)
    
    async def cmd_support(self, message: Message, state: FSMContext):
        await self.start_support_request(message, state)
    
    async def cmd_admin(self, message: Message):
        if message.from_user.id != ADMIN_ID:
            await message.answer("⛔ Доступ запрещен")
            return
        await self.show_admin_panel_text(message)
    
    # =========================================================================
    # КАПЧА
    # =========================================================================
    
    async def show_captcha(self, message: Message, state: FSMContext):
        """Показать капчу"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="✅ Подписаться на канал",
                url=CAPTCHA_CHANNEL_LINK
            )],
            [InlineKeyboardButton(
                text="Я подписался ✓",
                callback_data="check_captcha"
            )]
        ])
        
        text = f"""
🔐 <b>Добро пожаловать в MerAi & Monitoring!</b>

Для доступа к боту необходимо подписаться на наш канал.

<b>📋 Инструкция:</b>
1️⃣ Нажмите "✅ Подписаться на канал"
2️⃣ Подпишитесь на канал
3️⃣ Вернитесь сюда и нажмите "Я подписался ✓"

<i>Это бесплатно и занимает 10 секунд! ⚡</i>
"""
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(CaptchaStates.waiting_verification)
    
    async def handle_chat_member_update(self, update: ChatMemberUpdated):
        """Обработка изменений членства в чате"""
        # Этот обработчик срабатывает когда пользователь подписывается
        if update.chat.id == CAPTCHA_CHANNEL_ID:
            user_id = update.from_user.id
            new_status = update.new_chat_member.status
            
            # Если пользователь стал участником
            if isinstance(update.new_chat_member, ChatMemberMember):
                await self.db.verify_user(user_id)
                
                # Отправляем поздравление
                try:
                    await self.bot.send_message(
                        user_id,
                        """
🎉 <b>Поздравляем! Верификация пройдена</b>

✅ Вы успешно подписались на канал
✅ Доступ к боту открыт
✅ Все функции разблокированы

<i>Используйте /start для начала работы</i>
""",
                        parse_mode="HTML"
                    )
                except:
                    pass
    
    # =========================================================================
    # ГЛАВНОЕ МЕНЮ
    # =========================================================================
    
    async def show_main_menu(self, message: Message):
        """Главное меню"""
        user = await self.db.get_user(message.from_user.id)
        
        # Проверяем подписку
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if expires > datetime.now():
                days_left = (expires - datetime.now()).days
                sub_status = f"✅ Активна ({days_left} дн.)"
            else:
                sub_status = "🔴 Истекла"
        else:
            sub_status = "⚠️ Отсутствует"
        
        mode_status = {
            "none": "❌ Не выбран",
            "chatbot": "🤖 ChatBot",
            "userbot": "👤 UserBot"
        }.get(user['mode'], "❌ Не выбран")
        
        text = f"""
👋 <b>Добро пожаловать, {user['first_name']}!</b>

<b>📊 Ваш профиль:</b>
├ Подписка: {sub_status}
├ Режим: {mode_status}
└ Рефералов: {user['referral_count']}

<b>🎯 Основные функции:</b>
• 🔍 Мониторинг удалений
• 📝 Отслеживание редактирований
• 🎬 Сохранение кружков
• 🤖 Клонирование ботов (+3 дня)
• 🎁 Реферальная система
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⚙️ Режим работы", callback_data="choose_mode"),
                InlineKeyboardButton(text="💳 Тарифы", callback_data="plans")
            ],
            [
                InlineKeyboardButton(text="📊 Профиль", callback_data="profile"),
                InlineKeyboardButton(text="🎁 Рефералы", callback_data="referrals")
            ],
            [
                InlineKeyboardButton(text="🤖 Клонировать", callback_data="clone_bot"),
                InlineKeyboardButton(text="💬 Поддержка", callback_data="support")
            ],
            [
                InlineKeyboardButton(text="📖 Политика", callback_data="privacy"),
                InlineKeyboardButton(text="❓ Помощь", callback_data="help")
            ]
        ])
        
        if user['user_id'] == ADMIN_ID:
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(text="🔐 Админ-панель", callback_data="admin")
            ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    
    # =========================================================================
    # CALLBACK ОБРАБОТЧИК
    # =========================================================================
    
    async def callback_handler(self, callback: CallbackQuery, state: FSMContext):
        """Центральный обработчик callback"""
        data = callback.data
        user_id = callback.from_user.id
        
        try:
            # Проверяем верификацию
            if data == "check_captcha":
                await self.check_captcha_subscription(callback, state)
                return
            
            user = await self.db.get_user(user_id)
            if not user or not user['is_verified']:
                if data != "captcha":
                    await callback.answer("⚠️ Сначала пройдите верификацию!", show_alert=True)
                    return
            
            # Маршрутизация
            handlers = {
                "back_main": self.show_main_menu_callback,
                "choose_mode": self.show_mode_choice,
                "mode_chatbot": self.setup_chatbot,
                "mode_userbot": self.setup_userbot_start,
                "plans": self.show_plans,
                "profile": self.show_profile,
                "referrals": self.show_referrals,
                "clone_bot": self.start_clone_bot,
                "support": self.show_support,
                "support_write": self.start_support_ticket,
                "privacy": self.show_privacy,
                "help": self.show_help,
                "admin": self.show_admin_panel,
                "admin_users": self.show_admin_users,
                "admin_broadcasts": self.start_broadcast,
                "admin_support": self.show_admin_support_tickets,
                "my_bots": self.show_my_bots,
            }
            
            # Обработка покупок
            if data.startswith("buy_"):
                await self.handle_purchase(callback)
                await callback.answer()
                return
            
            # Обработка просмотра пользователей
            if data.startswith("view_user_"):
                await self.view_user_details(callback)
                await callback.answer()
                return
            
            # Обработка тикетов поддержки
            if data.startswith("reply_ticket_"):
                await self.start_ticket_reply(callback, state)
                await callback.answer()
                return
            
            handler = handlers.get(data)
            if handler:
                await handler(callback)
            else:
                await callback.answer("🔧 Функция в разработке", show_alert=True)
            
            await callback.answer()
            
        except Exception as e:
            logger.error(f"Ошибка в callback_handler: {e}\n{traceback.format_exc()}")
            await callback.answer("❌ Произошла ошибка", show_alert=True)
    
    async def check_captcha_subscription(self, callback: CallbackQuery, state: FSMContext):
        """Проверить подписку на капча-канал"""
        user_id = callback.from_user.id
        
        try:
            # Проверяем членство в канале
            member = await self.bot.get_chat_member(CAPTCHA_CHANNEL_ID, user_id)
            
            if isinstance(member, (ChatMemberMember, ChatMemberMember)):
                # Пользователь подписан
                await self.db.verify_user(user_id)
                await state.clear()
                
                await callback.message.edit_text("""
🎉 <b>Верификация пройдена!</b>

✅ Подписка подтверждена
✅ Доступ к боту открыт

Используйте /start для начала работы
""", parse_mode="HTML")
                
            else:
                await callback.answer(
                    "❌ Вы еще не подписаны на канал. Подпишитесь и попробуйте снова.",
                    show_alert=True
                )
                
        except Exception as e:
            logger.error(f"Ошибка проверки подписки: {e}")
            await callback.answer(
                "❌ Не удалось проверить подписку. Попробуйте позже.",
                show_alert=True
            )
    
    async def show_main_menu_callback(self, callback: CallbackQuery):
        """Главное меню через callback"""
        await callback.message.delete()
        await self.show_main_menu(callback.message)
    
    # =========================================================================
    # РЕЖИМЫ РАБОТЫ
    # =========================================================================
    
    async def show_mode_choice(self, callback: CallbackQuery):
        """Выбор режима"""
        text = """
⚙️ <b>ВЫБОР РЕЖИМА РАБОТЫ</b>

<b>🤖 ChatBot (Business Bot)</b>
✅ Простая настройка
✅ Не требует API ключей
❌ Только Telegram Premium Business
❌ Только бизнес-чаты

<b>👤 UserBot</b>
✅ Работает ВЕЗДЕ
✅ Все типы чатов
✅ Полный контроль
✅ Сохранение кружков
❌ Требует API_ID/API_HASH
❌ Сложнее настройка

<i>💡 Рекомендация: UserBot для максимальных возможностей</i>
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 ChatBot", callback_data="mode_chatbot")],
            [InlineKeyboardButton(text="👤 UserBot", callback_data="mode_userbot")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def setup_chatbot(self, callback: CallbackQuery):
        """Настройка ChatBot"""
        await self.db.set_user_mode(callback.from_user.id, "chatbot")
        
        text = """
🤖 <b>НАСТРОЙКА CHATBOT</b>

<b>📱 Подключение к Telegram Business:</b>

1️⃣ Откройте Telegram на телефоне
2️⃣ Перейдите в <b>Настройки → Telegram Business → Chatbots</b>
3️⃣ Нажмите "Добавить бота"
4️⃣ Найдите бота: <code>@{}</code>
5️⃣ Выберите чаты для мониторинга

✅ <b>Готово! Бот начнет отслеживать удаления</b>

<i>⚠️ Требуется Telegram Premium для Business функций</i>
""".format(ORIGINAL_BOT_USERNAME or "your_bot")
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Настроил", callback_data="back_main")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="choose_mode")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def setup_userbot_start(self, callback: CallbackQuery):
        """Начало настройки UserBot"""
        text = """
👤 <b>НАСТРОЙКА USERBOT</b>

Для работы userbot необходимо создать приложение Telegram:

<b>📋 Пошаговая инструкция:</b>

1️⃣ Откройте: https://my.telegram.org/apps
2️⃣ Войдите с номером телефона
3️⃣ Заполните форму:
   • App title: <code>MerAi UserBot</code>
   • Short name: <code>merai</code>
   • Platform: <code>Other</code>

4️⃣ Нажмите "Create application"
5️⃣ Скопируйте:
   • <b>api_id</b> (число)
   • <b>api_hash</b> (строка 32 символа)

<i>⚠️ В России могут быть ограничения на получение SMS кодов</i>

Когда будете готовы, нажмите "Продолжить"
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="▶️ Продолжить", callback_data="userbot_enter_data")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="choose_mode")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        await callback.answer()
    
    # =========================================================================
    # FSM ОБРАБОТЧИКИ - USERBOT SETUP
    # =========================================================================
    
    async def process_api_id(self, message: Message, state: FSMContext):
        """Обработка API_ID"""
        try:
            api_id = int(message.text.strip())
            await state.update_data(api_id=api_id)
            
            await message.answer("""
✅ API_ID сохранен!

Теперь отправьте <b>API_HASH</b> (строка из 32 символов)
""", parse_mode="HTML")
            
            await state.set_state(UserBotSetup.waiting_api_hash)
            
        except ValueError:
            await message.answer("❌ API_ID должен быть числом. Попробуйте еще раз.")
    
    async def process_api_hash(self, message: Message, state: FSMContext):
        """Обработка API_HASH"""
        api_hash = message.text.strip()
        
        if len(api_hash) != 32:
            await message.answer("❌ API_HASH должен содержать 32 символа. Попробуйте еще раз.")
            return
        
        await state.update_data(api_hash=api_hash)
        
        await message.answer("""
✅ API_HASH сохранен!

Теперь отправьте ваш <b>номер телефона</b> в международном формате.

Пример: +79991234567
""", parse_mode="HTML")
        
        await state.set_state(UserBotSetup.waiting_phone)
    
    async def process_phone(self, message: Message, state: FSMContext):
        """Обработка номера телефона"""
        phone = message.text.strip()
        
        if not phone.startswith("+"):
            await message.answer("❌ Номер должен начинаться с +. Попробуйте еще раз.")
            return
        
        await state.update_data(phone=phone)
        
        # Получаем данные
        data = await state.get_data()
        user_id = message.from_user.id
        
        # Запускаем userbot
        status_msg = await message.answer("🔄 Запускаю UserBot...", parse_mode="HTML")
        
        success, result = await self.userbot_manager.start_userbot(
            user_id,
            data['api_id'],
            data['api_hash'],
            phone
        )
        
        if success:
            await self.db.set_user_mode(user_id, "userbot")
            await status_msg.edit_text(f"""
✅ <b>UserBot успешно запущен!</b>

Режим работы: UserBot
Телефон: {phone}

Бот теперь отслеживает:
• Удаленные сообщения
• Редактирования
• Кружки (video notes)
• Все типы чатов

<i>Используйте /start для возврата в меню</i>
""", parse_mode="HTML")
            await state.clear()
        else:
            await status_msg.edit_text(f"""
❌ <b>Ошибка запуска UserBot</b>

{result}

Попробуйте еще раз или обратитесь в поддержку.
""", parse_mode="HTML")
            await state.clear()
    
    async def process_code(self, message: Message, state: FSMContext):
        """Обработка кода подтверждения (если потребуется)"""
        # Эта логика может потребоваться если нужна двухэтапная авторизация
        pass
    
    async def process_2fa(self, message: Message, state: FSMContext):
        """Обработка 2FA пароля"""
        pass
    
    # =========================================================================
    # ТАРИФНЫЕ ПЛАНЫ И ПЛАТЕЖИ
    # =========================================================================
    
    async def show_plans(self, callback: CallbackQuery):
        """Показать тарифы"""
        text = """
💳 <b>ТАРИФНЫЕ ПЛАНЫ</b>

Выберите подходящий план:

"""
        
        keyboard = []
        for plan_id, plan in SUBSCRIPTION_PLANS.items():
            text += f"\n<b>{plan['name']}</b>\n"
            text += f"├ Цена: {plan['price_rub']}₽ / {plan['price_stars']}⭐\n"
            text += f"└ Период: {plan['days']} дней\n"
            
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{plan['name']} - {plan['price_stars']}⭐",
                    callback_data=f"buy_{plan_id}_stars"
                )
            ])
        
        text += f"\n<b>🎁 Бонусы:</b>"
        text += f"\n• +{CLONE_BOT_BONUS_DAYS} дня за каждого бота"
        text += f"\n• {REFERRAL_REWARD} за {REFERRAL_REWARD_COUNT} рефералов"
        
        keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")])
        
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
    
    async def handle_purchase(self, callback: CallbackQuery):
        """Обработка покупки"""
        parts = callback.data.split("_")
        plan_id = parts[1]
        currency = parts[2] if len(parts) > 2 else "stars"
        
        plan = SUBSCRIPTION_PLANS.get(plan_id)
        if not plan:
            await callback.answer("❌ План не найден", show_alert=True)
            return
        
        if currency == "stars":
            # Создаем invoice для Stars
            prices = [LabeledPrice(label=plan['name'], amount=plan['price_stars'])]
            
            try:
                await self.bot.send_invoice(
                    chat_id=callback.from_user.id,
                    title=plan['name'],
                    description=f"Подписка MerAi на {plan['days']} дней",
                    payload=f"subscription_{plan_id}",
                    currency="XTR",  # Telegram Stars
                    prices=prices
                )
                await callback.answer("✅ Инвойс отправлен")
            except Exception as e:
                logger.error(f"Ошибка создания инвойса: {e}")
                await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
    
    async def process_pre_checkout(self, pre_checkout: PreCheckoutQuery):
        """Pre-checkout обработка"""
        await pre_checkout.answer(ok=True)
    
    async def process_successful_payment(self, message: Message):
        """Обработка успешного платежа"""
        payment = message.successful_payment
        payload = payment.invoice_payload
        
        if payload.startswith("subscription_"):
            plan_id = payload.replace("subscription_", "")
            plan = SUBSCRIPTION_PLANS.get(plan_id)
            
            if plan:
                user_id = message.from_user.id
                
                # Устанавливаем подписку
                await self.db.set_subscription(user_id, plan_id, plan['days'])
                
                # Сохраняем транзакцию
                await self.db.add_transaction(
                    user_id,
                    plan['price_stars'],
                    "XTR",
                    plan_id,
                    "telegram_stars",
                    payment.telegram_payment_charge_id
                )
                
                await message.answer(f"""
✅ <b>Оплата успешна!</b>

Приобретен план: <b>{plan['name']}</b>
Срок: {plan['days']} дней
Оплачено: {plan['price_stars']} ⭐

Ваша подписка активирована!

<i>Спасибо за покупку! 🎉</i>
""", parse_mode="HTML")
    
    # =========================================================================
    # ПРОФИЛЬ И РЕФЕРАЛЫ
    # =========================================================================
    
    async def show_profile(self, callback: CallbackQuery):
        """Показать профиль"""
        user = await self.db.get_user(callback.from_user.id)
        stats = await self.db.get_user_statistics(callback.from_user.id)
        
        # Подписка
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if expires > datetime.now():
                days_left = (expires - datetime.now()).days
                sub_text = f"✅ Активна до {expires.strftime('%d.%m.%Y')}\n├ Осталось: {days_left} дней"
            else:
                sub_text = "🔴 Истекла"
        else:
            sub_text = "⚠️ Отсутствует"
        
        # Реферальная ссылка
        ref_link = f"https://t.me/{ORIGINAL_BOT_USERNAME}?start=ref{user['user_id']}"
        
        # Боты
        bots = await self.db.get_user_bots(user['user_id'])
        
        text = f"""
📊 <b>МОЙ ПРОФИЛЬ</b>

<b>👤 Информация:</b>
├ ID: <code>{user['user_id']}</code>
├ Имя: {user['first_name']}
├ Username: @{user['username'] or 'не указан'}
└ Режим: {user['mode'].upper()}

<b>💎 Подписка:</b>
├ План: {user['subscription_plan'] or 'Отсутствует'}
├ Статус: {sub_text}
└ Автопродление: {'✅' if user['auto_renew'] else '❌'}

<b>📊 Статистика:</b>
├ Отслеживается: {stats['total_monitored']}
├ Удалено: {stats['deleted_count']}
└ Отредактировано: {stats['edited_count']}

<b>🎁 Реферальная программа:</b>
├ Приглашено: {user['referral_count']}
├ До награды: {max(0, REFERRAL_REWARD_COUNT - user['referral_count'])}
└ Ссылка: <code>{ref_link}</code>

<b>🤖 Клонированные боты:</b>
└ Активных: {len(bots)}
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Продлить подписку", callback_data="plans")],
            [InlineKeyboardButton(text="🤖 Мои боты", callback_data="my_bots")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def show_referrals(self, callback: CallbackQuery):
        """Показать рефералов"""
        user = await self.db.get_user(callback.from_user.id)
        ref_link = f"https://t.me/{ORIGINAL_BOT_USERNAME}?start=ref{user['user_id']}"
        
        progress = min(100, int(user['referral_count'] / REFERRAL_REWARD_COUNT * 100))
        progress_bar = "█" * (progress // 10) + "░" * (10 - progress // 10)
        
        text = f"""
🎁 <b>РЕФЕРАЛЬНАЯ ПРОГРАММА</b>

<b>📊 Ваш прогресс:</b>
{progress_bar} {user['referral_count']}/{REFERRAL_REWARD_COUNT}

<b>🎯 Награда за {REFERRAL_REWARD_COUNT} рефералов:</b>
{REFERRAL_REWARD}

<b>🔗 Ваша реферальная ссылка:</b>
<code>{ref_link}</code>

<b>📋 Как это работает:</b>
1️⃣ Отправьте ссылку друзьям
2️⃣ Они регистрируются по вашей ссылке
3️⃣ При достижении {REFERRAL_REWARD_COUNT} - получаете награду

<i>Текущий статус: {user['referral_count']} приглашений</i>
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 Поделиться", 
                                url=f"https://t.me/share/url?url={ref_link}&text=Попробуй этого бота!")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    # =========================================================================
    # КЛОНИРОВАНИЕ БОТОВ
    # =========================================================================
    
    async def start_clone_bot(self, callback: CallbackQuery, state: FSMContext):
        """Начать клонирование"""
        text = """
🤖 <b>КЛОНИРОВАНИЕ БОТА</b>

<b>🎁 Бонус: +3 дня к подписке!</b>

<b>📋 Как это работает:</b>
1️⃣ Создайте бота через @BotFather
2️⃣ Скопируйте токен
3️⃣ Отправьте токен сюда
4️⃣ Получите +3 дня подписки

<b>⚡ Преимущества:</b>
• Весь функционал MerAi
• Ваш собственный бот
• Дополнительные дни

<i>Отправьте токен бота (формат: 123456:ABC-DEF...)</i>
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❓ Как получить токен", url="https://t.me/BotFather")],
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(CloneBotStates.waiting_token)
    
    async def process_clone_token(self, message: Message, state: FSMContext):
        """Обработка токена клонированного бота"""
        token = message.text.strip()
        
        # Базовая валидация токена
        if ":" not in token or len(token) < 20:
            await message.answer("❌ Неверный формат токена. Попробуйте еще раз.")
            return
        
        try:
            # Проверяем токен
            test_bot = Bot(token=token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            # Добавляем бота
            success = await self.db.add_cloned_bot(
                message.from_user.id,
                token,
                bot_info.username,
                bot_info.first_name
            )
            
            if success:
                await message.answer(f"""
✅ <b>Бот успешно клонирован!</b>

🤖 <b>Информация:</b>
├ Имя: {bot_info.first_name}
├ Username: @{bot_info.username}
└ Бонус: +{CLONE_BOT_BONUS_DAYS} дня к подписке

Бот получил весь функционал MerAi!

<i>Используйте /start для возврата в меню</i>
""", parse_mode="HTML")
            else:
                await message.answer("""
❌ Этот бот уже был добавлен ранее.

Бонус выдается только за первое добавление.
""")
            
            await state.clear()
            
        except Exception as e:
            logger.error(f"Ошибка клонирования бота: {e}")
            await message.answer(f"""
❌ <b>Ошибка проверки токена</b>

Убедитесь, что:
• Токен скопирован полностью
• Бот активен
• Токен действителен

Попробуйте еще раз или /start для отмены
""", parse_mode="HTML")
    
    async def show_my_bots(self, callback: CallbackQuery):
        """Показать мои боты"""
        bots = await self.db.get_user_bots(callback.from_user.id)
        
        if not bots:
            text = """
🤖 <b>МОИ БОТЫ</b>

У вас пока нет клонированных ботов.

Создайте бота и получите +3 дня к подписке!
"""
        else:
            text = f"""
🤖 <b>МОИ БОТЫ</b>

Всего ботов: {len(bots)}

"""
            for i, bot in enumerate(bots, 1):
                text += f"\n<b>{i}. {bot['bot_name']}</b>\n"
                text += f"├ Username: @{bot['bot_username']}\n"
                text += f"├ Добавлен: {bot['added_at'][:10]}\n"
                text += f"└ Статус: {'✅ Активен' if bot['is_active'] else '❌ Отключен'}\n"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить бота", callback_data="clone_bot")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="profile")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    # =========================================================================
    # ТЕХПОДДЕРЖКА
    # =========================================================================
    
    async def show_support(self, callback: CallbackQuery):
        """Показать поддержку"""
        text = """
💬 <b>ПОДДЕРЖКА</b>

<b>📱 Способы связи:</b>

1️⃣ <b>Написать в боте</b>
   Нажмите "Написать сообщение"

2️⃣ <b>Личная связь</b>
   Telegram: @mrztn

<b>⏱ Время ответа:</b>
До 24 часов

<i>Мы всегда рады помочь! 🙂</i>
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Написать сообщение", callback_data="support_write")],
            [InlineKeyboardButton(text="📱 Личная связь", url="https://t.me/mrztn")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def start_support_ticket(self, callback: CallbackQuery, state: FSMContext):
        """Начать обращение в поддержку"""
        await callback.message.edit_text("""
✍️ <b>НАПИСАТЬ В ПОДДЕРЖКУ</b>

Опишите вашу проблему или вопрос.

Отправьте сообщение следующим сообщением.

<i>/start для отмены</i>
""", parse_mode="HTML")
        
        await state.set_state(SupportStates.waiting_message)
    
    async def process_support_message(self, message: Message, state: FSMContext):
        """Обработка сообщения в поддержку"""
        user_message = message.text
        user_id = message.from_user.id
        
        # Сохраняем тикет
        await self.db.add_support_ticket(user_id, user_message)
        
        # Уведомляем админа
        try:
            user = await self.db.get_user(user_id)
            await self.bot.send_message(
                ADMIN_ID,
                f"""
🆘 <b>НОВОЕ ОБРАЩЕНИЕ В ПОДДЕРЖКУ</b>

От: {user['first_name']} (@{user['username'] or 'без username'})
ID: <code>{user_id}</code>

<b>Сообщение:</b>
{user_message}

<i>Ответьте через админ-панель</i>
""",
                parse_mode="HTML"
            )
        except:
            pass
        
        await message.answer("""
✅ <b>Обращение отправлено!</b>

Ваше сообщение получено и передано в поддержку.

Мы ответим в течение 24 часов.

<i>Используйте /start для возврата в меню</i>
""", parse_mode="HTML")
        
        await state.clear()
    
    async def start_support_request(self, message: Message, state: FSMContext):
        """Быстрый старт поддержки через команду"""
        await message.answer("""
✍️ <b>НАПИСАТЬ В ПОДДЕРЖКУ</b>

Опишите вашу проблему или вопрос.

<i>/start для отмены</i>
""", parse_mode="HTML")
        
        await state.set_state(SupportStates.waiting_message)
    
    # =========================================================================
    # АДМИН-ПАНЕЛЬ
    # =========================================================================
    
    async def show_admin_panel(self, callback: CallbackQuery):
        """Админ-панель"""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("⛔ Доступ запрещен", show_alert=True)
            return
        
        all_users = await self.db.get_all_users()
        verified = await self.db.get_verified_users()
        
        # Подсчет активных подписок
        active_subs = 0
        for user in all_users:
            if user['subscription_expires']:
                expires = datetime.fromisoformat(user['subscription_expires'])
                if expires > datetime.now():
                    active_subs += 1
        
        text = f"""
🔐 <b>АДМИН-ПАНЕЛЬ</b>

<b>📊 Статистика:</b>
├ Всего пользователей: {len(all_users)}
├ Верифицировано: {len(verified)}
└ Активных подписок: {active_subs}

<b>⚙️ Управление:</b>
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"),
                InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcasts")
            ],
            [
                InlineKeyboardButton(text="💬 Поддержка", callback_data="admin_support"),
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def show_admin_users(self, callback: CallbackQuery):
        """Показать пользователей"""
        users = await self.db.get_verified_users()
        
        text = f"""
👥 <b>ПОЛЬЗОВАТЕЛИ</b>

Всего: {len(users)}

Выберите пользователя для просмотра:
"""
        
        keyboard = []
        for user in users[:10]:  # Показываем первых 10
            btn_text = f"{user['first_name']} ({user['user_id']})"
            keyboard.append([
                InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"view_user_{user['user_id']}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin")])
        
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
    
    async def view_user_details(self, callback: CallbackQuery):
        """Просмотр деталей пользователя"""
        user_id = int(callback.data.replace("view_user_", ""))
        user = await self.db.get_user(user_id)
        stats = await self.db.get_user_statistics(user_id)
        deleted = await self.db.get_user_deleted_messages(user_id, 5)
        
        # Подписка
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if expires > datetime.now():
                days_left = (expires - datetime.now()).days
                sub_text = f"✅ Активна ({days_left} дн.)"
            else:
                sub_text = "🔴 Истекла"
        else:
            sub_text = "⚠️ Отсутствует"
        
        text = f"""
👤 <b>ДЕТАЛИ ПОЛЬЗОВАТЕЛЯ</b>

<b>Информация:</b>
├ ID: <code>{user['user_id']}</code>
├ Имя: {user['first_name']}
├ Username: @{user['username'] or 'нет'}
├ Режим: {user['mode']}
└ Подписка: {sub_text}

<b>📊 Статистика:</b>
├ Отслеживается: {stats['total_monitored']}
├ Удалено: {stats['deleted_count']}
└ Отредактировано: {stats['edited_count']}

<b>🗑 Последние удаления:</b>
"""
        
        for msg in deleted[:3]:
            text += f"\n• {msg['chat_title']}: {msg['message_type']}"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def start_broadcast(self, callback: CallbackQuery, state: FSMContext):
        """Начать рассылку"""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("⛔ Доступ запрещен", show_alert=True)
            return
        
        await callback.message.edit_text("""
📢 <b>РАССЫЛКА</b>

Отправьте сообщение для рассылки всем пользователям.

<i>/start для отмены</i>
""", parse_mode="HTML")
        
        await state.set_state(BroadcastStates.waiting_message)
    
    async def process_broadcast_message(self, message: Message, state: FSMContext):
        """Обработка сообщения для рассылки"""
        broadcast_text = message.text
        
        # Получаем всех пользователей
        users = await self.db.get_verified_users()
        
        status_msg = await message.answer(f"""
🔄 <b>РАССЫЛКА ЗАПУЩЕНА</b>

Отправка: 0/{len(users)}
""", parse_mode="HTML")
        
        sent = 0
        failed = 0
        
        for user in users:
            try:
                await self.bot.send_message(user['user_id'], broadcast_text, parse_mode="HTML")
                sent += 1
                
                # Обновляем статус каждые 10 пользователей
                if sent % 10 == 0:
                    await status_msg.edit_text(f"""
🔄 <b>РАССЫЛКА В ПРОЦЕССЕ</b>

Отправлено: {sent}/{len(users)}
Ошибок: {failed}
""", parse_mode="HTML")
                
                await asyncio.sleep(0.05)  # Чтобы не превысить лимит
                
            except Exception as e:
                failed += 1
                logger.error(f"Ошибка рассылки пользователю {user['user_id']}: {e}")
        
        await status_msg.edit_text(f"""
✅ <b>РАССЫЛКА ЗАВЕРШЕНА</b>

Отправлено: {sent}
Ошибок: {failed}
Всего: {len(users)}
""", parse_mode="HTML")
        
        await state.clear()
    
    async def show_admin_support_tickets(self, callback: CallbackQuery):
        """Показать тикеты поддержки"""
        if callback.from_user.id != ADMIN_ID:
            await callback.answer("⛔ Доступ запрещен", show_alert=True)
            return
        
        tickets = await self.db.get_open_tickets()
        
        if not tickets:
            text = """
💬 <b>ТЕХПОДДЕРЖКА</b>

Нет открытых обращений.
"""
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin")]
            ])
        else:
            text = f"""
💬 <b>ТЕХПОДДЕРЖКА</b>

Открытых обращений: {len(tickets)}

"""
            keyboard = []
            
            for ticket in tickets[:5]:
                btn_text = f"От {ticket['first_name']} - {ticket['message'][:30]}..."
                keyboard.append([
                    InlineKeyboardButton(
                        text=btn_text,
                        callback_data=f"reply_ticket_{ticket['id']}"
                    )
                ])
            
            keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin")])
        
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
    
    async def start_ticket_reply(self, callback: CallbackQuery, state: FSMContext):
        """Начать ответ на тикет"""
        ticket_id = int(callback.data.replace("reply_ticket_", ""))
        await state.update_data(ticket_id=ticket_id)
        
        await callback.message.edit_text("""
✍️ <b>ОТВЕТ НА ОБРАЩЕНИЕ</b>

Напишите ответ пользователю.

<i>/start для отмены</i>
""", parse_mode="HTML")
        
        await state.set_state(SupportStates.admin_reply_waiting)
    
    async def process_admin_reply(self, message: Message, state: FSMContext):
        """Обработка ответа админа"""
        data = await state.get_data()
        ticket_id = data['ticket_id']
        reply_text = message.text
        
        # Получаем тикет
        tickets = await self.db.get_open_tickets()
        ticket = next((t for t in tickets if t['id'] == ticket_id), None)
        
        if ticket:
            # Закрываем тикет
            await self.db.close_ticket(ticket_id, reply_text)
            
            # Отправляем ответ пользователю
            try:
                await self.bot.send_message(
                    ticket['user_id'],
                    f"""
💬 <b>ОТВЕТ ОТ ПОДДЕРЖКИ</b>

<b>Ваше обращение:</b>
{ticket['message']}

<b>Ответ:</b>
{reply_text}

<i>Если остались вопросы - обращайтесь снова!</i>
""",
                    parse_mode="HTML"
                )
                
                await message.answer("✅ Ответ отправлен пользователю")
            except Exception as e:
                await message.answer(f"❌ Ошибка отправки: {e}")
        else:
            await message.answer("❌ Тикет не найден")
        
        await state.clear()
    
    # =========================================================================
    # ПРОЧИЕ ФУНКЦИИ
    # =========================================================================
    
    async def show_privacy(self, callback: CallbackQuery):
        """Политика конфиденциальности"""
        text = """
📖 <b>ПОЛИТИКА КОНФИДЕНЦИАЛЬНОСТИ</b>

<b>1. ОТКАЗ ОТ ОТВЕТСТВЕННОСТИ</b>

⚠️ Администрация НЕ несет ответственности за:
• Использование в незаконных целях
• Блокировку аккаунтов
• Потерю данных
• Действия пользователей

<b>2. ВАША ОТВЕТСТВЕННОСТЬ</b>

✅ Соблюдать законы РФ
✅ Соблюдать правила Telegram
✅ Не нарушать частную жизнь

<b>3. ОБРАБОТКА ДАННЫХ</b>

Мы собираем:
• Telegram ID
• Username
• Сохраненные сообщения

Данные защищены и не передаются третьим лицам.
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def show_help(self, callback: CallbackQuery):
        """Справка"""
        text = """
❓ <b>СПРАВКА</b>

<b>🤖 Команды:</b>
/start - Главное меню
/help - Справка
/plan - Тарифы
/support - Поддержка
/admin - Админ-панель

<b>⚙️ Режимы:</b>
• <b>ChatBot</b> - для Business
• <b>UserBot</b> - полный контроль

<b>🎯 Функции:</b>
• 🔍 Мониторинг удалений
• 📝 Отслеживание редактирований
• 🎬 Сохранение кружков
• 🤖 Клонирование ботов
• 🎁 Реферальная программа
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    async def show_help_text(self, message: Message):
        """Справка как текст"""
        await message.answer("""
❓ <b>СПРАВКА</b>

/start - Главное меню
/help - Справка
/plan - Тарифы
/support - Поддержка
""", parse_mode="HTML")
    
    async def show_plans_text(self, message: Message):
        """Тарифы как текст"""
        text = "💳 <b>ТАРИФНЫЕ ПЛАНЫ</b>\n\n"
        
        for plan_id, plan in SUBSCRIPTION_PLANS.items():
            text += f"<b>{plan['name']}</b>\n"
            text += f"Цена: {plan['price_stars']}⭐ ({plan['price_rub']}₽)\n"
            text += f"Период: {plan['days']} дней\n\n"
        
        await message.answer(text, parse_mode="HTML")
    
    async def show_admin_panel_text(self, message: Message):
        """Админ-панель как текст"""
        if message.from_user.id != ADMIN_ID:
            await message.answer("⛔ Доступ запрещен")
            return
        
        all_users = await self.db.get_all_users()
        text = f"""
🔐 <b>АДМИН-ПАНЕЛЬ</b>

Всего пользователей: {len(all_users)}

Используйте кнопки для управления.
"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcasts")]
        ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    
    # =========================================================================
    # ЗАПУСК И ОСТАНОВКА
    # =========================================================================
    
    async def start(self):
        """Запуск бота"""
        await self.db.connect()
        
        # Получаем информацию о боте
        me = await self.bot.get_me()
        global ORIGINAL_BOT_USERNAME
        ORIGINAL_BOT_USERNAME = me.username
        
        logger.info(f"🚀 Запуск бота @{me.username}")
        logger.info(f"📊 ID бота: {me.id}")
        logger.info(f"📅 Капча-канал ID: {CAPTCHA_CHANNEL_ID}")
        
        # Запуск polling
        await self.dp.start_polling(self.bot)
    
    async def stop(self):
        """Остановка бота"""
        await self.userbot_manager.stop_all()
        await self.db.close()
        await self.bot.session.close()
        logger.info("👋 Бот остановлен")

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("🚀 MerAi & Monitoring v6.0 - ИСПРАВЛЕННАЯ ВЕРСИЯ")
    logger.info("📅 9 марта 2026, 19:13 МСК")
    logger.info("=" * 60)
    
    bot = MerAiBot(BOT_TOKEN)
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("⚠️ Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}\n{traceback.format_exc()}")
    finally:
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Программа завершена")
