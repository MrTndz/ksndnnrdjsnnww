#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MerAi & Monitoring - Telegram Bot/Userbot для мониторинга удаленных сообщений
Версия: 5.2.0
Дата: Март 2026
Автор: @mrztn

ВАЖНО: Этот бот использует актуальные API Telegram 2026 года
"""

import os
import sys
import asyncio
import logging
import json
import aiosqlite
import zipfile
import io
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from pathlib import Path

# Telegram библиотеки (март 2026)
import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot import types, asyncio_filters
from telebot.asyncio_handler_backends import State, StatesGroup

# Userbot поддержка
try:
    from pyrogram import Client, filters
    from pyrogram.types import Message
    from pyrogram.handlers import MessageHandler, DeletedMessagesHandler
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False
    logging.warning("Pyrogram не установлен - userbot режим недоступен")

# AI провайдеры
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# =============================================================================
# КОНФИГУРАЦИЯ И КОНСТАНТЫ
# =============================================================================

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merai_bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ID администратора
ADMIN_ID = 7785371505

# База данных
DB_FILE = "merai_data.db"

# Планы подписки (в Telegram Stars)
SUBSCRIPTION_PLANS = {
    "trial": {"name": "🎁 Пробный", "price": 100, "days": 7, "description": "Пробный период на 7 дней"},
    "basic": {"name": "⚡ Базовый", "price": 300, "days": 30, "description": "Базовый функционал на месяц"},
    "premium": {"name": "👑 Премиум", "price": 800, "days": 90, "description": "Все функции на 3 месяца"},
    "lifetime": {"name": "💎 Навсегда", "price": 2000, "days": 36500, "description": "Пожизненный доступ"}
}

# =============================================================================
# КЛАССЫ БАЗЫ ДАННЫХ
# =============================================================================

class DatabaseManager:
    """Менеджер базы данных SQLite"""
    
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Подключение к БД и создание таблиц"""
        self.conn = await aiosqlite.connect(self.db_path)
        await self._create_tables()
        logger.info("✅ База данных подключена")
    
    async def _create_tables(self):
        """Создание необходимых таблиц"""
        await self.conn.executescript("""
            -- Пользователи
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                subscription_plan TEXT DEFAULT 'trial',
                subscription_expires TIMESTAMP,
                stars_balance INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                mode TEXT DEFAULT 'bot',
                api_id TEXT,
                api_hash TEXT,
                session_string TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Настройки бота (админ-панель)
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- AI ключи
            CREATE TABLE IF NOT EXISTS ai_keys (
                provider TEXT PRIMARY KEY,
                api_key TEXT,
                model TEXT,
                enabled BOOLEAN DEFAULT 1,
                system_prompt TEXT
            );
            
            -- Сохраненные сообщения (для перехвата удалений)
            CREATE TABLE IF NOT EXISTS saved_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                message_id INTEGER,
                message_type TEXT,
                content TEXT,
                media_file_id TEXT,
                caption TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_deleted BOOLEAN DEFAULT 0
            );
            
            -- Подключенные боты пользователей
            CREATE TABLE IF NOT EXISTS user_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                bot_token TEXT,
                bot_username TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- История транзакций
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                currency TEXT,
                plan TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Индексы для производительности
            CREATE INDEX IF NOT EXISTS idx_saved_messages_user ON saved_messages(user_id, chat_id);
            CREATE INDEX IF NOT EXISTS idx_saved_messages_deleted ON saved_messages(is_deleted);
            CREATE INDEX IF NOT EXISTS idx_users_subscription ON users(subscription_expires);
        """)
        await self.conn.commit()
        logger.info("✅ Таблицы БД созданы/проверены")
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Получить пользователя по ID"""
        async with self.conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(zip([col[0] for col in cursor.description], row))
        return None
    
    async def create_or_update_user(self, user_id: int, **kwargs):
        """Создать или обновить пользователя"""
        user = await self.get_user(user_id)
        
        if not user:
            # Создаем нового пользователя с пробным периодом
            kwargs.setdefault('subscription_plan', 'trial')
            kwargs.setdefault('subscription_expires', datetime.now() + timedelta(days=7))
            kwargs.setdefault('mode', 'bot')
            
            columns = ', '.join(['user_id'] + list(kwargs.keys()))
            placeholders = ', '.join(['?'] * (len(kwargs) + 1))
            values = [user_id] + list(kwargs.values())
            
            await self.conn.execute(
                f"INSERT INTO users ({columns}) VALUES ({placeholders})",
                values
            )
        else:
            # Обновляем существующего
            if kwargs:
                set_clause = ', '.join([f"{k} = ?" for k in kwargs.keys()])
                values = list(kwargs.values()) + [user_id]
                await self.conn.execute(
                    f"UPDATE users SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                    values
                )
        
        await self.conn.commit()
    
    async def save_message(self, user_id: int, chat_id: int, message_id: int, 
                          message_type: str, content: str = None, 
                          media_file_id: str = None, caption: str = None):
        """Сохранить сообщение для мониторинга"""
        await self.conn.execute("""
            INSERT INTO saved_messages 
            (user_id, chat_id, message_id, message_type, content, media_file_id, caption)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, chat_id, message_id, message_type, content, media_file_id, caption))
        await self.conn.commit()
    
    async def get_setting(self, key: str, default: str = None) -> Optional[str]:
        """Получить настройку из админ-панели"""
        async with self.conn.execute(
            "SELECT value FROM bot_settings WHERE key = ?", (key,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else default
    
    async def set_setting(self, key: str, value: str):
        """Установить настройку в админ-панели"""
        await self.conn.execute("""
            INSERT OR REPLACE INTO bot_settings (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (key, value))
        await self.conn.commit()
    
    async def get_ai_key(self, provider: str) -> Optional[Dict]:
        """Получить AI ключ"""
        async with self.conn.execute(
            "SELECT * FROM ai_keys WHERE provider = ? AND enabled = 1", (provider,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(zip([col[0] for col in cursor.description], row))
        return None
    
    async def set_ai_key(self, provider: str, api_key: str, model: str = None, 
                        system_prompt: str = None):
        """Установить AI ключ"""
        await self.conn.execute("""
            INSERT OR REPLACE INTO ai_keys (provider, api_key, model, system_prompt, enabled)
            VALUES (?, ?, ?, ?, 1)
        """, (provider, api_key, model, system_prompt))
        await self.conn.commit()
    
    async def close(self):
        """Закрыть соединение с БД"""
        if self.conn:
            await self.conn.close()
            logger.info("✅ База данных закрыта")

# =============================================================================
# TELEGRAM BOT (режим бота)
# =============================================================================

class MerAiBot:
    """Основной класс Telegram бота"""
    
    def __init__(self, token: str):
        self.bot = AsyncTeleBot(token)
        self.db = DatabaseManager()
        self.userbot_client = None
        
        logger.info("🤖 MerAi Bot инициализирован")
    
    async def start(self):
        """Запуск бота"""
        await self.db.connect()
        await self._init_default_settings()
        await self._register_handlers()
        
        logger.info("🚀 Запуск MerAi Bot...")
        
        # Запускаем бота
        try:
            await self.bot.infinity_polling()
        except Exception as e:
            logger.error(f"❌ Ошибка при работе бота: {e}")
            raise
    
    async def _init_default_settings(self):
        """Инициализация настроек по умолчанию"""
        defaults = {
            "payment_stars_enabled": "true",
            "payment_crypto_enabled": "false",
            "payment_card_enabled": "false",
            "feature_deletion_monitor": "true",
            "feature_ai_chat": "true",
            "feature_user_bots": "true"
        }
        
        for key, value in defaults.items():
            existing = await self.db.get_setting(key)
            if existing is None:
                await self.db.set_setting(key, value)
    
    async def _register_handlers(self):
        """Регистрация обработчиков команд и сообщений"""
        
        # Команда /start
        @self.bot.message_handler(commands=['start'])
        async def start_command(message: types.Message):
            user_id = message.from_user.id
            
            # Создаем/обновляем пользователя в БД
            await self.db.create_or_update_user(
                user_id,
                username=message.from_user.username,
                first_name=message.from_user.first_name
            )
            
            user = await self.db.get_user(user_id)
            
            # Проверяем подписку
            if user['subscription_expires']:
                expires = datetime.fromisoformat(user['subscription_expires'])
                if expires < datetime.now():
                    plan_name = "🔴 Подписка истекла"
                else:
                    days_left = (expires - datetime.now()).days
                    plan_name = f"✅ {SUBSCRIPTION_PLANS.get(user['subscription_plan'], {}).get('name', 'Неизвестный')} ({days_left} дн.)"
            else:
                plan_name = "⚠️ Нет подписки"
            
            welcome_text = f"""
<b>🎯 Добро пожаловать в MerAi & Monitoring!</b>

👤 <b>Ваш профиль:</b>
├ ID: <code>{user_id}</code>
├ План: {plan_name}
└ Режим: {user['mode'].upper()}

<b>📋 Основные возможности:</b>
├ 🔍 Мониторинг удаленных сообщений
├ 📝 Перехват редактирований
├ 📦 Автоархивация при массовом удалении
├ 🤖 AI-ассистент (Gemini, Grok, GLM)
└ 🔗 Поддержка пользовательских ботов

<i>💡 Используйте /help для полного списка команд</i>
"""
            
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("📱 Мой профиль", callback_data="profile"),
                types.InlineKeyboardButton("💳 Тарифы", callback_data="plans"),
                types.InlineKeyboardButton("❓ Помощь", callback_data="help"),
                types.InlineKeyboardButton("⚙️ Настройки", callback_data="settings")
            )
            
            if user_id == ADMIN_ID:
                markup.add(types.InlineKeyboardButton("🔐 Админ-панель", callback_data="admin"))
            
            await self.bot.send_message(
                message.chat.id,
                welcome_text,
                parse_mode='HTML',
                reply_markup=markup
            )
        
        # Команда /admin (только для администратора)
        @self.bot.message_handler(commands=['admin'])
        async def admin_command(message: types.Message):
            if message.from_user.id != ADMIN_ID:
                await self.bot.reply_to(message, "❌ У вас нет доступа к админ-панели")
                return
            
            await self._show_admin_panel(message.chat.id)
        
        # Команда /help
        @self.bot.message_handler(commands=['help'])
        async def help_command(message: types.Message):
            help_text = """
<b>📖 Инструкция по использованию MerAi & Monitoring</b>

<b>🤖 РЕЖИМ БОТА (Telegram Premium Business):</b>
1. Добавьте бота в настройках Telegram Business
2. Бот будет автоматически перехватывать удаления в бизнес-чатах
3. Вы получите уведомления о всех удалениях

<b>👤 РЕЖИМ USERBOT (Полный контроль):</b>
1. Создайте приложение на my.telegram.org/apps
2. Получите API_ID и API_HASH
3. Добавьте их в настройках (/settings)
4. Бот будет работать от вашего имени

<b>⚙️ РАЗЛИЧИЯ:</b>
<b>Бот:</b> ✅ Не требует API, ❌ Работает только в Business чатах
<b>Userbot:</b> ✅ Работает везде, ❌ Требует API_ID/API_HASH

<b>🔧 КОМАНДЫ:</b>
/start - Главное меню
/profile - Мой профиль
/plans - Тарифные планы
/ai - AI-ассистент
/help - Эта справка
/admin - Админ-панель (только для @mrztn)

<b>⚠️ ВАЖНО:</b>
• Самоуничтожающиеся сообщения нельзя перехватить
• Кружки (voice circles) не сохраняются в Telegram
• Для userbot режима каждый пользователь должен создать свое приложение
"""
            
            await self.bot.send_message(
                message.chat.id,
                help_text,
                parse_mode='HTML'
            )
        
        # Обработка callback запросов
        @self.bot.callback_query_handler(func=lambda call: True)
        async def callback_handler(call: types.CallbackQuery):
            await self._handle_callback(call)
        
        logger.info("✅ Обработчики команд зарегистрированы")
    
    async def _show_admin_panel(self, chat_id: int):
        """Отображение админ-панели"""
        admin_text = """
<b>🔐 АДМИН-ПАНЕЛЬ</b>

<b>Управление системой MerAi & Monitoring</b>

Выберите раздел для управления:
"""
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
            types.InlineKeyboardButton("🔑 AI Ключи", callback_data="admin_ai"),
            types.InlineKeyboardButton("💳 Платежи", callback_data="admin_payments"),
            types.InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings"),
            types.InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
            types.InlineKeyboardButton("🔙 Назад", callback_data="start")
        )
        
        await self.bot.send_message(chat_id, admin_text, parse_mode='HTML', reply_markup=markup)
    
    async def _handle_callback(self, call: types.CallbackQuery):
        """Обработка callback кнопок"""
        data = call.data
        user_id = call.from_user.id
        
        try:
            if data == "start":
                # Возврат в главное меню
                await self.bot.delete_message(call.message.chat.id, call.message.id)
                await self.start_command(call.message)
            
            elif data == "profile":
                await self._show_profile(call.message.chat.id, user_id)
            
            elif data == "plans":
                await self._show_plans(call.message.chat.id, user_id)
            
            elif data == "help":
                await self.help_command(call.message)
            
            elif data == "admin" and user_id == ADMIN_ID:
                await self._show_admin_panel(call.message.chat.id)
            
            elif data.startswith("admin_") and user_id == ADMIN_ID:
                await self._handle_admin_callback(call)
            
            # Подтверждаем callback
            await self.bot.answer_callback_query(call.id)
        
        except Exception as e:
            logger.error(f"Ошибка при обработке callback {data}: {e}")
            await self.bot.answer_callback_query(call.id, "❌ Произошла ошибка")
    
    async def _show_profile(self, chat_id: int, user_id: int):
        """Показать профиль пользователя"""
        user = await self.db.get_user(user_id)
        
        if not user:
            await self.bot.send_message(chat_id, "❌ Пользователь не найден")
            return
        
        # Форматируем дату окончания подписки
        if user['subscription_expires']:
            expires = datetime.fromisoformat(user['subscription_expires'])
            if expires < datetime.now():
                status = "🔴 Истекла"
                expires_str = expires.strftime("%d.%m.%Y %H:%M")
            else:
                days_left = (expires - datetime.now()).days
                status = f"✅ Активна ({days_left} дн.)"
                expires_str = expires.strftime("%d.%m.%Y %H:%M")
        else:
            status = "⚠️ Отсутствует"
            expires_str = "—"
        
        plan_info = SUBSCRIPTION_PLANS.get(user['subscription_plan'], {})
        
        profile_text = f"""
<b>👤 МОЙ ПРОФИЛЬ</b>

<b>📊 Информация:</b>
├ ID: <code>{user_id}</code>
├ Имя: {user['first_name']}
├ Username: @{user['username'] or 'не указан'}
└ Зарегистрирован: {user['created_at'][:10]}

<b>💎 Подписка:</b>
├ План: {plan_info.get('name', 'Неизвестный')}
├ Статус: {status}
├ Истекает: {expires_str}
└ Stars баланс: {user['stars_balance']} ⭐

<b>⚙️ Режим работы:</b>
└ {user['mode'].upper()} {'✅' if user['is_active'] else '❌'}

<b>🤖 Подключенные боты:</b>
└ Загрузка...
"""
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("💳 Продлить подписку", callback_data="plans"),
            types.InlineKeyboardButton("⚙️ Настройки режима", callback_data="settings_mode"),
            types.InlineKeyboardButton("🔙 Назад", callback_data="start")
        )
        
        await self.bot.send_message(chat_id, profile_text, parse_mode='HTML', reply_markup=markup)
    
    async def _show_plans(self, chat_id: int, user_id: int):
        """Показать тарифные планы"""
        plans_text = """
<b>💳 ТАРИФНЫЕ ПЛАНЫ</b>

Выберите подходящий план подписки:
"""
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        
        for plan_id, plan in SUBSCRIPTION_PLANS.items():
            button_text = f"{plan['name']} - {plan['price']} ⭐ ({plan['days']} дн.)"
            markup.add(types.InlineKeyboardButton(button_text, callback_data=f"buy_{plan_id}"))
        
        markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="profile"))
        
        await self.bot.send_message(chat_id, plans_text, parse_mode='HTML', reply_markup=markup)
    
    async def _handle_admin_callback(self, call: types.CallbackQuery):
        """Обработка админ-панели"""
        data = call.data
        
        if data == "admin_ai":
            await self._admin_show_ai_keys(call.message.chat.id)
        
        elif data == "admin_settings":
            await self._admin_show_settings(call.message.chat.id)
        
        # Добавьте другие разделы админ-панели здесь
    
    async def _admin_show_ai_keys(self, chat_id: int):
        """Админ: показать AI ключи"""
        text = """
<b>🔑 УПРАВЛЕНИЕ AI КЛЮЧАМИ</b>

Текущие ключи:
"""
        
        # Получаем ключи из БД
        async with self.db.conn.execute("SELECT * FROM ai_keys") as cursor:
            keys = await cursor.fetchall()
            
            if keys:
                for key in keys:
                    provider, api_key, model, enabled = key[0], key[1][:20] + "...", key[2], key[3]
                    status = "✅" if enabled else "❌"
                    text += f"\n{status} <b>{provider.upper()}</b>: <code>{api_key}</code> ({model or 'default'})"
            else:
                text += "\n<i>Ключи не добавлены</i>"
        
        text += "\n\n<b>Команды для добавления ключей:</b>"
        text += "\n/setkey gemini API_KEY [model]"
        text += "\n/setkey grok API_KEY [model]"
        text += "\n/setkey glm API_KEY [model]"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin"))
        
        await self.bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
    
    async def _admin_show_settings(self, chat_id: int):
        """Админ: показать настройки"""
        # Получаем настройки
        stars_enabled = await self.db.get_setting("payment_stars_enabled", "true")
        crypto_enabled = await self.db.get_setting("payment_crypto_enabled", "false")
        card_enabled = await self.db.get_setting("payment_card_enabled", "false")
        
        text = f"""
<b>⚙️ НАСТРОЙКИ СИСТЕМЫ</b>

<b>💳 Платежные методы:</b>
├ Telegram Stars: {'✅' if stars_enabled == 'true' else '❌'}
├ Криптовалюта: {'✅' if crypto_enabled == 'true' else '❌'}
└ Банковские карты: {'✅' if card_enabled == 'true' else '❌'}

<b>🔧 Функции:</b>
├ Мониторинг удалений: ✅
├ AI-ассистент: ✅
└ Пользовательские боты: ✅
"""
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(
                f"Stars: {'✅' if stars_enabled == 'true' else '❌'}",
                callback_data="toggle_stars"
            ),
            types.InlineKeyboardButton(
                f"Crypto: {'✅' if crypto_enabled == 'true' else '❌'}",
                callback_data="toggle_crypto"
            ),
            types.InlineKeyboardButton("🔙 Назад", callback_data="admin")
        )
        
        await self.bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)

# =============================================================================
# USERBOT РЕЖИМ (Pyrogram)
# =============================================================================

class MerAiUserbot:
    """Userbot для перехвата удаленных сообщений"""
    
    def __init__(self, api_id: int, api_hash: str, session_name: str = "merai_session"):
        if not PYROGRAM_AVAILABLE:
            raise RuntimeError("Pyrogram не установлен! Установите: pip install pyrogram tgcrypto")
        
        self.app = Client(
            session_name,
            api_id=api_id,
            api_hash=api_hash,
            workdir="/home/claude"
        )
        
        self.db = DatabaseManager()
        self.register_handlers()
        
        logger.info("👤 MerAi Userbot инициализирован")
    
    def register_handlers(self):
        """Регистрация обработчиков userbot"""
        
        # Обработчик новых сообщений (для сохранения)
        @self.app.on_message(filters.all & ~filters.me)
        async def save_message_handler(client: Client, message: Message):
            try:
                # Сохраняем сообщение для возможности восстановления
                await self.save_message_to_db(message)
            except Exception as e:
                logger.error(f"Ошибка сохранения сообщения: {e}")
        
        # Обработчик удаленных сообщений
        @self.app.on_deleted_messages()
        async def deleted_messages_handler(client: Client, messages: List[Message]):
            try:
                for msg in messages:
                    await self.handle_deleted_message(msg)
            except Exception as e:
                logger.error(f"Ошибка обработки удаленных сообщений: {e}")
        
        logger.info("✅ Обработчики userbot зарегистрированы")
    
    async def save_message_to_db(self, message: Message):
        """Сохранить сообщение в БД"""
        user_id = message.from_user.id if message.from_user else 0
        chat_id = message.chat.id
        message_id = message.id
        
        # Определяем тип сообщения
        if message.text:
            msg_type = "text"
            content = message.text
            file_id = None
        elif message.photo:
            msg_type = "photo"
            content = message.caption or ""
            file_id = message.photo.file_id
        elif message.video:
            msg_type = "video"
            content = message.caption or ""
            file_id = message.video.file_id
        elif message.document:
            msg_type = "document"
            content = message.caption or ""
            file_id = message.document.file_id
        elif message.voice:
            msg_type = "voice"
            content = ""
            file_id = message.voice.file_id
        else:
            msg_type = "other"
            content = ""
            file_id = None
        
        await self.db.save_message(
            user_id, chat_id, message_id, msg_type, content, file_id, message.caption
        )
    
    async def handle_deleted_message(self, message: Message):
        """Обработать удаленное сообщение"""
        # Получаем сохраненное сообщение из БД
        async with self.db.conn.execute("""
            SELECT * FROM saved_messages 
            WHERE chat_id = ? AND message_id = ? AND is_deleted = 0
            LIMIT 1
        """, (message.chat.id, message.id)) as cursor:
            saved = await cursor.fetchone()
        
        if saved:
            # Отправляем уведомление пользователю
            await self.notify_deletion(saved)
            
            # Помечаем как удаленное
            await self.db.conn.execute("""
                UPDATE saved_messages SET is_deleted = 1 WHERE id = ?
            """, (saved[0],))
            await self.db.conn.commit()
    
    async def notify_deletion(self, saved_message):
        """Отправить уведомление об удалении"""
        # Формируем уведомление
        notification = f"""
🗑 <b>Сообщение удалено</b>

📍 Чат ID: <code>{saved_message[2]}</code>
🆔 Message ID: <code>{saved_message[3]}</code>
📝 Тип: {saved_message[4]}
🕒 Время: {saved_message[7]}

<b>Содержимое:</b>
{saved_message[5] or '<i>Медиа-файл</i>'}
"""
        
        # Отправляем себе (это требует реализации bot части)
        # await self.app.send_message("me", notification, parse_mode="HTML")
    
    async def start(self):
        """Запуск userbot"""
        await self.db.connect()
        await self.app.start()
        logger.info("🚀 Userbot запущен")
        
        try:
            await self.app.idle()
        finally:
            await self.app.stop()
            await self.db.close()

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    """Главная функция запуска"""
    logger.info("=" * 60)
    logger.info("🚀 MerAi & Monitoring v5.2.0 - Запуск")
    logger.info("=" * 60)
    
    # Получаем токен бота из переменных окружения
    bot_token = os.getenv("BOT_TOKEN")
    
    if not bot_token:
        logger.error("❌ BOT_TOKEN не найден! Установите в переменных окружения")
        logger.error("Пример: export BOT_TOKEN='your_bot_token'")
        sys.exit(1)
    
    # Проверяем режим работы
    mode = os.getenv("MODE", "bot").lower()
    
    try:
        if mode == "userbot":
            # Запуск в режиме userbot
            api_id = os.getenv("API_ID")
            api_hash = os.getenv("API_HASH")
            
            if not api_id or not api_hash:
                logger.error("❌ API_ID и API_HASH необходимы для userbot режима!")
                sys.exit(1)
            
            logger.info("👤 Запуск в режиме USERBOT")
            userbot = MerAiUserbot(int(api_id), api_hash)
            await userbot.start()
        
        else:
            # Запуск в режиме бота (по умолчанию)
            logger.info("🤖 Запуск в режиме BOT")
            bot = MerAiBot(bot_token)
            await bot.start()
    
    except KeyboardInterrupt:
        logger.info("⚠️ Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("👋 MerAi Bot остановлен")

if __name__ == "__main__":
    # Запуск в asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Программа завершена пользователем")
