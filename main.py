# -*- coding: utf-8 -*-
"""
MerAi GPT Enterprise Edition v8.0
The Ultimate AI Assistant Platform
Created by @mrztn
"""

import os
import sys
import asyncio
import logging
import sqlite3
import json
import time
import re
import math
import random
import hashlib
import string
import traceback
import psutil
import warnings
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Callable, Awaitable, Tuple, Type
from contextlib import suppress, asynccontextmanager
from decimal import Decimal
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from abc import ABC, abstractmethod

# Внешние библиотеки
import aiosqlite
import openai
from openai import AsyncOpenAI, OpenAIError
import aiohttp
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont
import google.generativeai as genai

# Aiogram
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandStart, CommandObject, StateFilter, MagicData
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    BufferedInputFile, InputFile, FSInputFile, URLInputFile,
    ReplyKeyboardMarkup, KeyboardButton, WebAppInfo,
    LabeledPrice, SuccessfulPayment, PreCheckoutQuery,
    Message, User, Chat, Update, CallbackQuery
)
from aiogram.utils.markdown import hbold, hcode, hpre, hitalic, hlink, hunderline, hstrikethrough, hspoiler
from aiogram.enums import ParseMode, ContentType, ChatAction
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import (
    TelegramBadRequest, TelegramAPIError, 
    TelegramNetworkError, TelegramForbiddenError,
    TelegramNotFound, TelegramConflictError
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.strategy import FSMStrategy
from aiogram.utils.token import TokenValidationError
from aiogram.utils.chat_action import ChatActionSender
from aiogram.utils.i18n import I18n, SimpleI18nMiddleware
from aiogram.utils.keyboard import InlineKeyboardBuilder

warnings.filterwarnings("ignore")

# --- КОНСТАНТЫ И КОНФИГУРАЦИЯ ---

APP_NAME = "MerAi GPT Enterprise"
VERSION = "8.0.0"
CODENAME = "Infinity"
CREATOR = "@mrztn"
CREATOR_ID = 7785371505

# Пути
DB_NAME = "merai_enterprise.db"
LOGS_DIR = "logs"
MEDIA_DIR = "media"
TEMP_DIR = "temp"

# Создание директорий
for d in [LOGS_DIR, MEDIA_DIR, TEMP_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

# Загрузка .env
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# API Keys
API_KEYS = {
    "groq": os.getenv("GROQ_API_KEY"),
    "google": os.getenv("GOOGLE_API_KEY"),
    "openrouter": os.getenv("OPENROUTER_API_KEY"),
    "zhipu": os.getenv("ZHIPU_API_KEY"),
    "openai": os.getenv("OPENAI_API_KEY"),
}

# Настройки
CONFIG = {
    "max_history": 20,
    "animation_interval": 0.5,
    "request_timeout": 120,
    "max_retries": 3,
    "fake_users_offset": 78000,
    "free_requests_limit": 50,
    "referral_bonus": 10,
    "price_premium_month": 199, # Рубли
}

# --- ЛОГИРОВАНИЕ ---

class AdvancedFormatter(logging.Formatter):
    """Продвинутый форматтер с цветами для консоли"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[34m',     # Blue
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    FORMAT = "[%(asctime)s] [%(levelname)-8s] [%(name)s:%(lineno)d] -> %(message)s"

    def format(self, record):
        log_fmt = self.COLORS.get(record.levelname, '') + self.FORMAT + self.RESET
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

def setup_logging():
    logger = logging.getLogger("MerAi")
    logger.setLevel(logging.INFO)
    
    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(AdvancedFormatter())
    logger.addHandler(ch)
    
    # File Handler (Error logs)
    fh = logging.FileHandler(f"{LOGS_DIR}/errors.log", encoding='utf-8')
    fh.setLevel(logging.ERROR)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    
    return logger

logger = setup_logging()

# --- ИСКЛЮЧЕНИЯ ---

class MerAiError(Exception): pass
class APIError(MerAiError): pass
class BalanceError(APIError): pass
class RateLimitError(APIError): pass
class ContentFilterError(APIError): pass
class DatabaseError(MerAiError): pass
class PermissionDenied(MerAiError): pass
class UserBanned(MerAiError): pass

# --- УТИЛИТЫ ---

def get_timestamp() -> int:
    return int(time.time())

def format_datetime(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime('%d.%m.%Y %H:%M')

def format_uptime(start_ts: float) -> str:
    delta = datetime.now() - datetime.fromtimestamp(start_ts)
    days = delta.days
    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{days}д")
    if hours: parts.append(f"{hours}ч")
    if minutes: parts.append(f"{minutes}м")
    if not parts: parts.append(f"{seconds}с")
    return " ".join(parts)

def humanize_number(num: int) -> str:
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return str(num)

def truncate_text(text: str, max_len: int = 4096) -> str:
    if len(text) > max_len:
        return text[:max_len-3] + "..."
    return text

def split_text(text: str, max_len: int = 4080) -> List[str]:
    """Умное разбиение текста по предложениям"""
    if len(text) <= max_len:
        return [text]
    
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        
        # Ищем последний перенос строки или точку
        split_pos = text.rfind('\n', 0, max_len)
        if split_pos == -1 or split_pos < max_len * 0.5:
            split_pos = text.rfind('. ', 0, max_len)
        if split_pos == -1:
            split_pos = max_len
        
        parts.append(text[:split_pos].strip())
        text = text[split_pos:].strip()
    
    return parts

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def generate_ref_code(user_id: int) -> str:
    """Генерация реферального кода"""
    h = hashlib.md5(str(user_id).encode())
    return h.hexdigest()[:8].upper()

def is_user_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

# --- ДЕКОРАТОРЫ ---

def error_handler(func):
    """Декоратор для обработки ошибок в хендлерах"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except TelegramForbiddenError:
            logger.warning(f"Bot blocked by user in {func.__name__}")
        except TelegramBadRequest as e:
            logger.error(f"Bad request in {func.__name__}: {e}")
        except Exception as e:
            logger.exception(f"Unhandled error in {func.__name__}: {e}")
    return wrapper

def admin_only(func):
    """Декоратор для проверки админа"""
    async def wrapper(event, *args, **kwargs):
        user_id = event.from_user.id if hasattr(event, 'from_user') else event.chat.id
        if not is_user_admin(user_id):
            if isinstance(event, CallbackQuery):
                await event.answer("⛔ Нет прав", show_alert=True)
            else:
                await event.answer("⛔ Нет прав.")
            return
        return await func(event, *args, **kwargs)
    return wrapper

# --- ENUMS ---

class UserStatus(Enum):
    ACTIVE = "active"
    BANNED = "banned"
    INACTIVE = "inactive"

class SubscriptionType(Enum):
    FREE = "free"
    PREMIUM = "premium"
    ADMIN = "admin"

class RequestType(Enum):
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    CODE = "code"

class ProviderStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"

# --- DATA CLASSES ---

@dataclass
class UserInfo:
    id: int
    username: Optional[str]
    first_name: str
    lang: str = "ru"
    status: UserStatus = UserStatus.ACTIVE
    sub_type: SubscriptionType = SubscriptionType.FREE
    balance: float = 0.0
    requests_count: int = 0
    join_date: int = field(default_factory=get_timestamp)
    last_activity: int = field(default_factory=get_timestamp)
    referrer_id: Optional[int] = None
    ref_code: str = ""

@dataclass
class ProviderInfo:
    name: str
    status: ProviderStatus
    models: List[str]
    requests_today: int = 0
    errors_today: int = 0
    last_error: Optional[str] = None

# --- БАЗА ДАННЫХ (Enterprise Level) ---

class Database:
    """Асинхронная база данных с пулом соединений"""
    
    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        self._pool = None
    
    async def connect(self):
        """Создание пула соединений"""
        logger.info("🔌 Подключение к БД...")
        # Простая реализация без внешних пулов для совместимости
        await self._create_tables()
        logger.info("✅ БД готова")
    
    @asynccontextmanager
    async def get_cursor(self):
        """Контекстный менеджер для курсора"""
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.cursor()
            try:
                yield cursor
                await db.commit()
            except Exception as e:
                logger.error(f"DB Error: {e}")
                await db.rollback()
                raise
    
    async def _create_tables(self):
        """Создание всех таблиц"""
        async with aiosqlite.connect(self.db_name) as db:
            # Таблица пользователей
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT DEFAULT 'ru',
                    status TEXT DEFAULT 'active',
                    subscription TEXT DEFAULT 'free',
                    balance REAL DEFAULT 0.0,
                    requests_count INTEGER DEFAULT 0,
                    join_date INTEGER DEFAULT (strftime('%s', 'now')),
                    last_activity INTEGER DEFAULT (strftime('%s', 'now')),
                    referrer_id INTEGER,
                    ref_code TEXT UNIQUE,
                    settings TEXT DEFAULT '{}',
                    FOREIGN KEY (referrer_id) REFERENCES users(user_id)
                )
            ''')
            
            # Таблица истории чатов
            await db.execute('''
                CREATE TABLE IF NOT EXISTS chat_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    role TEXT,
                    content TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                    tokens_used INTEGER DEFAULT 0,
                    model TEXT,
                    provider TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Индекс для быстрого поиска истории
            await db.execute('CREATE INDEX IF NOT EXISTS idx_history_user ON chat_history(user_id, id DESC)')
            
            # Таблица логов API
            await db.execute('''
                CREATE TABLE IF NOT EXISTS api_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider TEXT,
                    model TEXT,
                    user_id INTEGER,
                    tokens_in INTEGER,
                    tokens_out INTEGER,
                    latency REAL,
                    status TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s', 'now'))
                )
            ''')
            
            # Таблица транзакций
            await db.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    type TEXT,
                    description TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s', 'now'))
                )
            ''')
            
            # Таблица рассылок
            await db.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    text TEXT,
                    status TEXT DEFAULT 'pending',
                    total_sent INTEGER DEFAULT 0,
                    total_failed INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                )
            ''')
            
            await db.commit()
    
    # --- Методы пользователей ---
    
    async def add_user(self, user: User, referrer_id: Optional[int] = None) -> bool:
        """Добавление пользователя с реферальной системой"""
        async with aiosqlite.connect(self.db_name) as db:
            try:
                ref_code = generate_ref_code(user.id)
                
                # Проверяем, есть ли юзер
                cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
                if await cursor.fetchone():
                    # Обновляем активность
                    await db.execute('''
                        UPDATE users SET last_activity = strftime('%s', 'now'), 
                        username = ?, first_name = ?
                        WHERE user_id = ?
                    ''', (user.username, user.first_name, user.id))
                    return False
                
                # Создаем нового
                await db.execute('''
                    INSERT INTO users 
                    (user_id, username, first_name, last_name, language_code, referrer_id, ref_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user.id, user.username, user.first_name, user.last_name, 
                      user.language_code, referrer_id, ref_code))
                
                # Если есть реферер, даем бонус
                if referrer_id:
                    await db.execute('''
                        UPDATE users SET balance = balance + ? WHERE user_id = ?
                    ''', (CONFIG['referral_bonus'], referrer_id))
                
                await db.commit()
                return True
            except Exception as e:
                logger.error(f"Add user error: {e}")
                return False
    
    async def get_user(self, user_id: int) -> Optional[UserInfo]:
        """Получение данных пользователя"""
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            if row:
                return UserInfo(
                    id=row['user_id'],
                    username=row['username'],
                    first_name=row['first_name'],
                    lang=row['language_code'],
                    status=UserStatus(row['status']),
                    sub_type=SubscriptionType(row['subscription']),
                    balance=row['balance'],
                    requests_count=row['requests_count'],
                    join_date=row['join_date'],
                    last_activity=row['last_activity'],
                    referrer_id=row['referrer_id'],
                    ref_code=row['ref_code']
                )
            return None
    
    async def get_all_users(self, limit: int = 100, offset: int = 0) -> List[UserInfo]:
        """Получение списка пользователей"""
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM users ORDER BY join_date DESC LIMIT ? OFFSET ?", 
                (limit, offset)
            )
            rows = await cursor.fetchall()
            return [UserInfo(
                id=r['user_id'], username=r['username'], first_name=r['first_name'],
                lang=r['language_code'], status=UserStatus(r['status']),
                sub_type=SubscriptionType(r['subscription']), balance=r['balance'],
                requests_count=r['requests_count'], join_date=r['join_date'],
                last_activity=r['last_activity'], referrer_id=r['referrer_id'], ref_code=r['ref_code']
            ) for r in rows]
    
    async def get_total_users(self) -> int:
        """Общее количество пользователей"""
        async with aiosqlite.connect(self.db_name) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            count = (await cursor.fetchone())[0]
            return count + CONFIG['fake_users_offset']
    
    async def ban_user(self, user_id: int):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("UPDATE users SET status = 'banned' WHERE user_id = ?", (user_id,))
            await db.commit()
    
    async def unban_user(self, user_id: int):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("UPDATE users SET status = 'active' WHERE user_id = ?", (user_id,))
            await db.commit()
    
    # --- История чатов ---
    
    async def add_message(self, user_id: int, role: str, content: str, 
                          model: str = None, provider: str = None, tokens: int = 0):
        """Добавление сообщения в историю"""
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute('''
                INSERT INTO chat_history (user_id, role, content, model, provider, tokens_used)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, role, content, model, provider, tokens))
            
            # Удаляем старые сообщения (хранить только последние N)
            await db.execute('''
                DELETE FROM chat_history WHERE id IN (
                    SELECT id FROM chat_history WHERE user_id = ? ORDER BY id DESC LIMIT -1 OFFSET ?
                )
            ''', (user_id, CONFIG['max_history']))
            await db.commit()
    
    async def get_history(self, user_id: int, limit: int = CONFIG['max_history']) -> List[Dict]:
        """Получение контекста чата"""
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT role, content FROM chat_history WHERE user_id = ? ORDER BY id ASC LIMIT ?",
                (user_id, limit)
            )
            rows = await cursor.fetchall()
            return [{"role": r['role'], "content": r['content']} for r in rows]
    
    async def clear_history(self, user_id: int):
        """Очистка истории пользователя"""
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("DELETE FROM chat_history WHERE user_id = ?", (user_id,))
            await db.commit()
    
    async def get_user_full_history(self, user_id: int, limit: int = 100) -> List[Dict]:
        """Получение ПОЛНОЙ истории для админа"""
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT role, content, timestamp, model, provider 
                   FROM chat_history WHERE user_id = ? ORDER BY id DESC LIMIT ?""",
                (user_id, limit)
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
    
    # --- Статистика ---
    
    async def increment_requests(self, user_id: int):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute(
                "UPDATE users SET requests_count = requests_count + 1, last_activity = strftime('%s', 'now') WHERE user_id = ?",
                (user_id,)
            )
            await db.commit()
    
    async def get_stats(self) -> Dict:
        """Общая статистика для админки"""
        async with aiosqlite.connect(self.db_name) as db:
            stats = {}
            
            # Всего юзеров
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            stats['total_users'] = (await cursor.fetchone())[0]
            
            # Активных за 24ч
            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE last_activity > strftime('%s', 'now') - 86400"
            )
            stats['active_24h'] = (await cursor.fetchone())[0]
            
            # Всего сообщений
            cursor = await db.execute("SELECT COUNT(*) FROM chat_history")
            stats['total_messages'] = (await cursor.fetchone())[0]
            
            # Премиум
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE subscription = 'premium'")
            stats['premium_users'] = (await cursor.fetchone())[0]
            
            return stats

# --- AI PROVIDERS SYSTEM ---

class BaseAIProvider(ABC):
    """Абстрактный класс для AI провайдера"""
    
    def __init__(self, api_key: Optional[str], name: str, models: List[str]):
        self.api_key = api_key
        self.name = name
        self.models = models
        self.client = None
        self.status = ProviderStatus.OFFLINE
        self.requests_count = 0
        self.errors_count = 0
    
    @abstractmethod
    async def initialize(self):
        pass
    
    @abstractmethod
    async def generate(self, messages: List[Dict], model: str = None, **kwargs) -> str:
        pass
    
    @abstractmethod
    async def generate_image(self, prompt: str, **kwargs) -> Optional[str]:
        pass
    
    def is_available(self) -> bool:
        return self.status == ProviderStatus.ONLINE and self.api_key is not None
    
    async def handle_error(self, error: Exception):
        """Обработка ошибок"""
        self.errors_count += 1
        err_str = str(error)
        
        if "rate limit" in err_str.lower() or "429" in err_str:
            self.status = ProviderStatus.RATE_LIMITED
            raise RateLimitError(f"{self.name}: Rate limit")
        elif "insufficient" in err_str.lower() or "balance" in err_str.lower() or "billing" in err_str.lower():
            self.status = ProviderStatus.ERROR
            raise BalanceError(f"{self.name}: No balance")
        else:
            logger.error(f"{self.name} Error: {error}")
            raise APIError(f"{self.name}: {err_str[:100]}")

class GroqProvider(BaseAIProvider):
    """Groq Provider - Очень быстрый и бесплатный"""
    
    def __init__(self, api_key: Optional[str]):
        super().__init__(api_key, "Groq", ["llama3-70b-8192", "mixtral-8x7b-32768", "gemma-7b-it"])
    
    async def initialize(self):
        if not self.api_key:
            return
        try:
            self.client = AsyncOpenAI(
                api_key=self.api_key,
                base_url="https://api.groq.com/openai/v1"
            )
            self.status = ProviderStatus.ONLINE
            logger.info(f"✅ {self.name} initialized")
        except Exception as e:
            logger.error(f"❌ {self.name} init failed: {e}")
    
    async def generate(self, messages: List[Dict], model: str = "llama3-70b-8192", **kwargs) -> str:
        if not self.is_available():
            raise APIError("Provider offline")
        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                max_tokens=2048
            )
            self.requests_count += 1
            return response.choices[0].message.content
        except Exception as e:
            await self.handle_error(e)
        return ""
    
    async def generate_image(self, prompt: str, **kwargs) -> Optional[str]:
        raise NotImplementedError("Groq does not support image generation")

class GoogleProvider(BaseAIProvider):
    """Google Gemini Provider - Бесплатный"""
    
    def __init__(self, api_key: Optional[str]):
        super().__init__(api_key, "Google Gemini", ["gemini-pro", "gemini-1.5-flash"])
        self.gen_model = None
    
    async def initialize(self):
        if not self.api_key:
            return
        try:
            genai.configure(api_key=self.api_key)
            self.gen_model = genai.GenerativeModel('gemini-pro')
            self.status = ProviderStatus.ONLINE
            logger.info(f"✅ {self.name} initialized")
        except Exception as e:
            logger.error(f"❌ {self.name} init failed: {e}")
    
    async def generate(self, messages: List[Dict], model: str = "gemini-pro", **kwargs) -> str:
        if not self.is_available():
            raise APIError("Provider offline")
        try:
            # Конвертация формата сообщений для Gemini
            prompt = "\n".join([f"{m['role']}: {m['content']}" for m in messages])
            response = await asyncio.to_thread(self.gen_model.generate_content, prompt)
            self.requests_count += 1
            return response.text
        except Exception as e:
            await self.handle_error(e)
        return ""
    
    async def generate_image(self, prompt: str, **kwargs) -> Optional[str]:
        raise NotImplementedError("Use Gemini Vision for images")

class OpenRouterProvider(BaseAIProvider):
    """OpenRouter - Доступ ко всем моделям (Claude, Llama, etc)"""
    
    def __init__(self, api_key: Optional[str]):
        super().__init__(api_key, "OpenRouter", [
            "anthropic/claude-3-haiku", "meta-llama/llama-3-70b-instruct", 
            "openai/gpt-3.5-turbo", "google/gemini-pro"
        ])
    
    async def initialize(self):
        if not self.api_key:
            return
        try:
            self.client = AsyncOpenAI(
                api_key=self.api_key,
                base_url="https://openrouter.ai/api/v1"
            )
            self.status = ProviderStatus.ONLINE
            logger.info(f"✅ {self.name} initialized")
        except Exception as e:
            logger.error(f"❌ {self.name} init failed: {e}")
    
    async def generate(self, messages: List[Dict], model: str = "meta-llama/llama-3-70b-instruct", **kwargs) -> str:
        if not self.is_available():
            raise APIError("Provider offline")
        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7
            )
            self.requests_count += 1
            return response.choices[0].message.content
        except Exception as e:
            await self.handle_error(e)
        return ""
    
    async def generate_image(self, prompt: str, **kwargs) -> Optional[str]:
        # OpenRouter поддерживает DALL-E через openai/dall-e
        return None

class ZhipuProvider(BaseAIProvider):
    """Zhipu AI Provider"""
    
    def __init__(self, api_key: Optional[str]):
        super().__init__(api_key, "Zhipu AI", ["glm-4", "glm-4-flash", "glm-3-turbo"])
    
    async def initialize(self):
        if not self.api_key:
            return
        try:
            self.client = AsyncOpenAI(
                api_key=self.api_key,
                base_url="https://open.bigmodel.cn/api/paas/v4/"
            )
            self.status = ProviderStatus.ONLINE
            logger.info(f"✅ {self.name} initialized")
        except Exception as e:
            logger.error(f"❌ {self.name} init failed: {e}")
    
    async def generate(self, messages: List[Dict], model: str = "glm-4-flash", **kwargs) -> str:
        if not self.is_available():
            raise APIError("Provider offline")
        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7
            )
            self.requests_count += 1
            return response.choices[0].message.content
        except Exception as e:
            await self.handle_error(e)
        return ""
    
    async def generate_image(self, prompt: str, **kwargs) -> Optional[str]:
        # CogView
        try:
            response = await self.client.images.generate(
                model="cogview-3",
                prompt=prompt,
                size="1024x1024"
            )
            return response.data[0].url
        except Exception as e:
            await self.handle_error(e)
        return None

class OpenAIProvider(BaseAIProvider):
    """OpenAI Provider"""
    
    def __init__(self, api_key: Optional[str]):
        super().__init__(api_key, "OpenAI", ["gpt-3.5-turbo", "gpt-4o-mini", "gpt-4"])
    
    async def initialize(self):
        if not self.api_key:
            return
        try:
            self.client = AsyncOpenAI(api_key=self.api_key)
            self.status = ProviderStatus.ONLINE
            logger.info(f"✅ {self.name} initialized")
        except Exception as e:
            logger.error(f"❌ {self.name} init failed: {e}")
    
    async def generate(self, messages: List[Dict], model: str = "gpt-3.5-turbo", **kwargs) -> str:
        if not self.is_available():
            raise APIError("Provider offline")
        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7
            )
            self.requests_count += 1
            return response.choices[0].message.content
        except Exception as e:
            await self.handle_error(e)
        return ""
    
    async def generate_image(self, prompt: str, **kwargs) -> Optional[str]:
        try:
            response = await self.client.images.generate(
                model="dall-e-3",
                prompt=prompt,
                size="1024x1024"
            )
            return response.data[0].url
        except Exception as e:
            await self.handle_error(e)
        return None

class AIManager:
    """Менеджер управления AI провайдерами с автопереключением"""
    
    def __init__(self):
        self.providers: List[BaseAIProvider] = []
        self.current_provider: Optional[BaseAIProvider] = None
    
    def add_provider(self, provider: BaseAIProvider):
        self.providers.append(provider)
    
    async def initialize_all(self):
        """Инициализация всех провайдеров"""
        logger.info("🤖 Инициализация AI провайдеров...")
        
        tasks = [p.initialize() for p in self.providers]
        await asyncio.gather(*tasks)
        
        # Выбираем первый доступный
        for p in self.providers:
            if p.is_available():
                self.current_provider = p
                logger.info(f"🎯 Активный провайдер: {p.name}")
                return
        
        logger.warning("⚠️ Нет доступных AI провайдеров!")
    
    def get_active_provider(self) -> Optional[BaseAIProvider]:
        """Получить первый доступный провайдер"""
        for p in self.providers:
            if p.is_available():
                return p
        return None
    
    async def generate_response(self, messages: List[Dict], model: str = None) -> Tuple[str, str]:
        """Генерация ответа с переключением при ошибке"""
        tried = set()
        
        while len(tried) < len(self.providers):
            provider = self.current_provider
            
            if not provider or provider.name in tried or not provider.is_available():
                # Ищем нового
                provider = self.get_active_provider()
                if not provider:
                    return "❌ Все AI сервисы недоступны. Попробуйте позже.", "none"
            
            tried.add(provider.name)
            
            try:
                logger.info(f"🔊 Try: {provider.name}")
                result = await provider.generate(messages, model)
                return result, provider.name
            
            except (RateLimitError, BalanceError) as e:
                logger.warning(f"⚠️ {e} - Switching provider")
                self.current_provider = self.get_active_provider()
                if self.current_provider:
                    logger.info(f"⏩ Switched to {self.current_provider.name}")
            
            except APIError as e:
                logger.error(f"❌ API Error in {provider.name}: {e}")
                provider.status = ProviderStatus.ERROR
                self.current_provider = self.get_active_provider()
        
        return "⚠️ Не удалось получить ответ от всех провайдеров. Попробуйте позже.", "error"
    
    async def generate_image(self, prompt: str) -> Tuple[Optional[str], str]:
        """Генерация изображения первым доступным провайдером"""
        for p in self.providers:
            if p.is_available():
                try:
                    url = await p.generate_image(prompt)
                    if url:
                        return url, p.name
                except:
                    continue
        return None, "none"

    def get_status_list(self) -> List[Dict]:
        """Статус всех провайдеров"""
        return [
            {
                "name": p.name,
                "status": p.status.value,
                "available": p.is_available(),
                "requests": p.requests_count,
                "errors": p.errors_count
            }
            for p in self.providers
        ]

# --- КЛАВИАТУРЫ (UI) ---

class Keyboards:
    """Фабрика клавиатур"""
    
    @staticmethod
    def main_menu(user: UserInfo) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        
        # Режимы
        builder.row(InlineKeyboardButton(text="💬 Чат", callback_data="mode_chat"))
        builder.row(InlineKeyboardButton(text="🎨 Фото", callback_data="mode_img"))
        builder.row(InlineKeyboardButton(text="💻 Код", callback_data="mode_code"))
        
        # Утилиты
        builder.row(InlineKeyboardButton(text="🗑 Очистить", callback_data="clear"))
        builder.row(
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
            InlineKeyboardButton(text="💎 Премиум", callback_data="premium")
        )
        builder.row(InlineKeyboardButton(text="👨‍💻 Создатель", url=f"https://t.me/{CREATOR.strip('@')}"))
        
        return builder.as_markup()
    
    @staticmethod
    def admin_main() -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        
        builder.row(InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"))
        builder.row(
            InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"),
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")
        )
        builder.row(
            InlineKeyboardButton(text="🤖 AI Провайдеры", callback_data="admin_providers"),
            InlineKeyboardButton(text="📜 Логи", callback_data="admin_logs")
        )
        builder.row(InlineKeyboardButton(text="🔄 Перезапуск", callback_data="admin_restart"))
        
        return builder.as_markup()
    
    @staticmethod
    def admin_users_list(users: List[UserInfo], page: int = 0) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        
        for user in users[page*5 : (page+1)*5]:
            status = "🟢" if user.status == UserStatus.ACTIVE else "🔴"
            text = f"{status} {user.first_name} ({user.requests_count} req)"
            builder.row(InlineKeyboardButton(text=text, callback_data=f"admin_user_{user.id}"))
        
        # Навигация
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_upage_{page-1}"))
        nav.append(InlineKeyboardButton(text="🔙", callback_data="admin_main"))
        if len(users) > (page+1)*5:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_upage_{page+1}"))
        
        builder.row(*nav)
        return builder.as_markup()
    
    @staticmethod
    def admin_user_info(user_id: int) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        
        builder.row(InlineKeyboardButton(text="📜 История чата", callback_data=f"admin_hist_{user_id}"))
        builder.row(
            InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_ban_{user_id}"),
            InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_unban_{user_id}")
        )
        builder.row(
            InlineKeyboardButton(text="➕ Баланс", callback_data=f"admin_addbal_{user_id}"),
            InlineKeyboardButton(text="➖ Баланс", callback_data=f"admin_subbal_{user_id}")
        )
        builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users"))
        
        return builder.as_markup()

# --- FSM СОСТОЯНИЯ ---

class AdminStates(StatesGroup):
    main = State()
    broadcast = State()
    user_search = State()
    edit_balance = State()

class UserStates(StatesGroup):
    chat = State()
    image = State()
    code = State()

# --- АНИМАЦИЯ ---

class Animation:
    """Управление анимацией 'Typing...'"""
    
    FRAMES_CHAT = [
        "🧠 Анализирую запрос.", "🧠 Анализирую запрос..", "🧠 Анализирую запрос...",
        "⚡ Обрабатываю.", "⚡ Обрабатываю..", "⚡ Обрабатываю...",
        "💡 Генерирую идеи.", "💡 Генерирую идеи..", "💡 Генерирую идеи..."
    ]
    
    FRAMES_IMG = [
        "🎨 Рисую эскиз.", "🎨 Рисую эскиз..", "🎨 Рисую эскиз...",
        "🖌 Добавляю детали.", "🖌 Добавляю детали..", "🖌 Добавляю детали...",
        "✨ Финальные штрихи.", "✨ Финальные штрихи..", "✨ Финальные штрихи..."
    ]
    
    def __init__(self, bot: Bot, chat_id: int, msg_id: int, type: str = "chat"):
        self.bot = bot
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.type = type
        self.running = False
        self.task: Optional[asyncio.Task] = None
    
    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self._run())
    
    async def _run(self):
        frames = self.FRAMES_CHAT if self.type == "chat" else self.FRAMES_IMG
        i = 0
        while self.running:
            try:
                await self.bot.edit_message_text(
                    frames[i % len(frames)], 
                    self.chat_id, 
                    self.msg_id
                )
                i += 1
                await asyncio.sleep(0.6)
            except TelegramBadRequest:
                continue
            except Exception:
                break
    
    async def stop(self, text: Optional[str] = None, parse_mode=ParseMode.HTML):
        self.running = False
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        
        if text:
            try:
                await self.bot.edit_message_text(
                    text, self.chat_id, self.msg_id, parse_mode=parse_mode
                )
            except Exception:
                pass

# --- ГЛАВНЫЙ КЛАСС ПРИЛОЖЕНИЯ ---

class MerAiBot:
    """Главный класс приложения"""
    
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.db: Optional[Database] = None
        self.ai: Optional[AIManager] = None
        self.router = Router()
        self.start_time = time.time()
    
    async def setup(self):
        """Инициализация компонентов"""
        logger.info("=" * 60)
        logger.info(f"🚀 {APP_NAME} v{VERSION} ({CODENAME})")
        logger.info("=" * 60)
        
        # Проверка токена
        if not BOT_TOKEN:
            logger.critical("❌ BOT_TOKEN не найден!")
            sys.exit(1)
        
        # Инициализация БД
        self.db = Database()
        await self.db.connect()
        
        # Инициализация AI
        self.ai = AIManager()
        
        # Добавляем провайдеров в порядке приоритета
        self.ai.add_provider(GroqProvider(API_KEYS['groq']))      # Приоритет 1 (Бесплатный)
        self.ai.add_provider(GoogleProvider(API_KEYS['google']))  # Приоритет 2
        self.ai.add_provider(ZhipuProvider(API_KEYS['zhipu']))    # Приоритет 3
        self.ai.add_provider(OpenRouterProvider(API_KEYS['openrouter'])) # Приоритет 4
        self.ai.add_provider(OpenAIProvider(API_KEYS['openai']))  # Приоритет 5
        
        await self.ai.initialize_all()
        
        # Инициализация бота
        self.bot = Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        
        self.dp = Dispatcher(
            storage=MemoryStorage(),
            fsm_strategy=FSMStrategy.USER_IN_CHAT
        )
        
        self.dp.include_router(self.router)
        
        # Регистрация хендлеров
        self._register_handlers()
        
        logger.info(f"👑 Admin: {ADMIN_ID}")
    
    def _register_handlers(self):
        """Регистрация всех хендлеров"""
        r = self.router
        
        # Common
        r.message(CommandStart())(self.cmd_start)
        r.message(Command("help"))(self.cmd_help)
        r.message(Command("profile"))(self.cmd_profile)
        r.message(Command("clear"))(self.cmd_clear)
        
        # Admin
        r.message(Command("admin"))(self.cmd_admin)
        r.callback_query(F.data == "admin_main")(self.cb_admin_main)
        r.callback_query(F.data.startswith("admin_"))(self.cb_admin_handlers)
        
        # Chat logic
        r.callback_query(F.data.startswith("mode_"))(self.cb_mode_select)
        r.callback_query(F.data == "clear")(self.cb_clear)
        r.callback_query(F.data == "profile")(self.cb_profile)
        
        # Main message handler
        r.message()(self.handle_message)
    
    # --- ХЕНДЛЕРЫ ---
    
    @error_handler
    async def cmd_start(self, message: Message, state: FSMContext):
        await state.clear()
        
        user = message.from_user
        args = message.text.split()[1:] if message.text else []
        
        # Реферальная система
        referrer_id = None
        if args and args[0].startswith("ref_"):
            try:
                referrer_id = int(args[0][4:])
            except:
                pass
        
        # Добавляем юзера
        is_new = await self.db.add_user(user, referrer_id)
        
        # Проверка бана
        user_data = await self.db.get_user(user.id)
        if user_data and user_data.status == UserStatus.BANNED:
            await message.answer("⛔ Вы заблокированы.")
            return
        
        count = await self.db.get_total_users()
        
        text = (
            f"👋 <b>Привет, {hbold(user.first_name)}!</b>\n\n"
            f"🧠 Я — <b>MerAi GPT Enterprise</b>\n"
            f"🔄 Версия: {VERSION}\n"
            f"👨‍💻 Создатель: {hbold(CREATOR)}\n\n"
            f"📊 <b>Наша семья:</b> <code>{count}</code> пользователей!\n\n"
            f"💡 Я использую мощнейшие AI модели:\n"
            f"• Llama 3 (70B)\n"
            f"• Gemini Pro\n"
            f"• Claude & GPT-4\n"
            f"• И многие другие...\n\n"
            f"🔻 <b>Выберите действие в меню:</b>"
        )
        
        await message.answer(text, reply_markup=Keyboards.main_menu(user_data))
        
        if is_new:
            logger.info(f"🆕 New user: {user.id}")
    
    async def cmd_help(self, message: Message):
        text = (
            "📚 <b>Справка по MerAi</b>\n\n"
            "🤖 <b>Возможности:</b>\n"
            "• 💬 Общение на любые темы\n"
            "• 💻 Написание кода\n"
            "• 🎨 Генерация изображений\n"
            "• 📝 Перевод и анализ текста\n\n"
            "⚡ <b>Команды:</b>\n"
            "/start - Главное меню\n"
            "/profile - Ваш профиль\n"
            "/clear - Очистить память\n"
            "/help - Эта справка"
        )
        await message.answer(text)
    
    async def cmd_profile(self, message: Message):
        user_data = await self.db.get_user(message.from_user.id)
        if not user_data:
            await message.answer("❌ Профиль не найден. Напишите /start")
            return
        
        sub_emoji = "💎" if user_data.sub_type == SubscriptionType.PREMIUM else "🆓"
        status_emoji = "🟢" if user_data.status == UserStatus.ACTIVE else "🔴"
        
        text = (
            f"👤 <b>Ваш профиль</b>\n\n"
            f"🆔 ID: <code>{user_data.id}</code>\n"
            f"👤 Имя: {hbold(user_data.first_name)}\n"
            f"{status_emoji} Статус: {user_data.status.value}\n"
            f"{sub_emoji} Подписка: {user_data.sub_type.value}\n"
            f"💬 Запросов: {user_data.requests_count}\n"
            f"📅 Регистрация: {format_datetime(user_data.join_date)}\n"
        )
        
        if user_data.ref_code:
            link = f"https://t.me/{(await self.bot.me()).username}?start=ref_{user_data.ref_code}"
            text += f"\n🔗 Реферальная ссылка: {link}"
        
        await message.answer(text)
    
    async def cmd_clear(self, message: Message):
        await self.db.clear_history(message.from_user.id)
        await message.answer("✅ Память очищена.")
    
    async def cmd_admin(self, message: Message):
        if not is_user_admin(message.from_user.id):
            await message.answer("⛔ Нет прав.")
            return
        
        await message.answer(
            f"👑 <b>Админ-панель MerAi</b>\n"
            f"⏰ Uptime: {format_uptime(self.start_time)}",
            reply_markup=Keyboards.admin_main()
        )
    
    async def cb_admin_main(self, callback: CallbackQuery):
        if not is_user_admin(callback.from_user.id):
            return
        
        await callback.message.edit_text(
            f"👑 <b>Админ-панель</b>",
            reply_markup=Keyboards.admin_main()
        )
    
    async def cb_admin_handlers(self, callback: CallbackQuery, state: FSMContext):
        if not is_user_admin(callback.from_user.id):
            return
        
        data = callback.data.split("_")[1:]
        
        if data[0] == "stats":
            stats = await self.db.get_stats()
            ai_status = self.ai.get_status_list()
            
            text = (
                f"📊 <b>Статистика</b>\n\n"
                f"👥 Пользователей: {stats['total_users'] + CONFIG['fake_users_offset']}\n"
                f"👥 Реальных: {stats['total_users']}\n"
                f"🟢 Активных (24ч): {stats['active_24h']}\n"
                f"💬 Сообщений: {stats['total_messages']}\n"
                f"💎 Премиум: {stats['premium_users']}\n\n"
                f"🤖 <b>Провайдеры:</b>\n"
            )
            
            for p in ai_status:
                emoji = "✅" if p['available'] else "❌"
                text += f"{emoji} {p['name']}: {p['requests']} req\n"
            
            await callback.message.edit_text(text, reply_markup=Keyboards.admin_main())
        
        elif data[0] == "users":
            users = await self.db.get_all_users(limit=100)
            await callback.message.edit_text(
                "👥 <b>Пользователи</b>",
                reply_markup=Keyboards.admin_users_list(users)
            )
        
        elif data[0] == "upage":
            page = int(data[1])
            users = await self.db.get_all_users(limit=100)
            await callback.message.edit_text(
                "👥 <b>Пользователи</b>",
                reply_markup=Keyboards.admin_users_list(users, page)
            )
        
        elif data[0] == "user":
            user_id = int(data[1])
            user = await self.db.get_user(user_id)
            
            if not user:
                await callback.answer("User not found")
                return
            
            text = (
                f"👤 <b>Информация о пользователе</b>\n\n"
                f"🆔 ID: <code>{user.id}</code>\n"
                f"👤 Name: {user.first_name}\n"
                f"💬 Requests: {user.requests_count}\n"
                f"📅 Join: {format_datetime(user.join_date)}\n"
                f"⏰ Last active: {format_datetime(user.last_activity)}\n"
            )
            
            await callback.message.edit_text(text, reply_markup=Keyboards.admin_user_info(user_id))
        
        elif data[0] == "hist":
            user_id = int(data[1])
            history = await self.db.get_user_full_history(user_id)
            
            if not history:
                await callback.answer("История пуста", show_alert=True)
                return
            
            # Отправляем файлом, если много
            text = f"📜 <b>История чата {user_id}</b>\n\n"
            parts = []
            current_part = ""
            
            for msg in history:
                line = f"[{format_datetime(msg['timestamp'])}] {msg['role']}: {msg['content'][:100]}...\n"
                if len(current_part) + len(line) > 4000:
                    parts.append(current_part)
                    current_part = ""
                current_part += line
            parts.append(current_part)
            
            await callback.message.answer(parts[0])
            for p in parts[1:3]: # Максимум 3 части
                await callback.message.answer(p)
            
            await callback.answer()
        
        elif data[0] == "ban":
            user_id = int(data[1])
            await self.db.ban_user(user_id)
            await callback.answer(f"🚫 User {user_id} banned!")
        
        elif data[0] == "unban":
            user_id = int(data[1])
            await self.db.unban_user(user_id)
            await callback.answer(f"✅ User {user_id} unbanned!")
        
        elif data[0] == "providers":
            status = self.ai.get_status_list()
            text = "🤖 <b>Статус провайдеров:</b>\n\n"
            for p in status:
                text += f"<b>{p['name']}</b>: {p['status']}\nReqs: {p['requests']} | Errs: {p['errors']}\n\n"
            
            await callback.message.edit_text(text, reply_markup=Keyboards.admin_main())
        
        else:
            await callback.answer("In development")
    
    async def cb_mode_select(self, callback: CallbackQuery, state: FSMContext):
        mode = callback.data.split("_")[1]
        
        if mode == "chat":
            await state.set_state(UserStates.chat)
            text = "💬 <b>Режим: Чат</b>\nНапишите ваш вопрос."
        elif mode == "img":
            await state.set_state(UserStates.image)
            text = "🎨 <b>Режим: Фото</b>\nОпишите, что нужно нарисовать."
        elif mode == "code":
            await state.set_state(UserStates.code)
            text = "💻 <b>Режим: Код</b>\nОпишите задачу, я напишу код."
        else:
            text = "Выбрано."
        
        await callback.message.answer(text)
        await callback.answer()
    
    async def cb_clear(self, callback: CallbackQuery):
        await self.db.clear_history(callback.from_user.id)
        await callback.answer("✅ Память очищена!", show_alert=True)
    
    async def cb_profile(self, callback: CallbackQuery):
        user_data = await self.db.get_user(callback.from_user.id)
        # Аналогично cmd_profile
        await callback.message.answer(f"👤 Ваш профиль: ID {user_data.id}")
    
    # --- ГЛАВНЫЙ ХЕНДЛЕР СООБЩЕНИЙ ---
    
    async def handle_message(self, message: Message, state: FSMContext):
        user = message.from_user
        text = message.text or ""
        
        # Проверки
        user_data = await self.db.get_user(user.id)
        if not user_data:
            await self.db.add_user(user)
            user_data = await self.db.get_user(user.id)
        
        if user_data.status == UserStatus.BANNED:
            return
        
        # Увеличиваем счетчик
        await self.db.increment_requests(user.id)
        
        # Определяем тип запроса
        current_state = await state.get_state()
        
        is_img = False
        is_code = False
        
        if current_state == UserStates.image:
            is_img = True
        elif current_state == UserStates.code:
            is_code = True
        else:
            # Автоопределение
            img_words = ["нарисуй", "сгенерируй", "draw", "image", "picture"]
            code_words = ["напиши код", "сделай скрипт", "code", "python", "js"]
            
            if any(w in text.lower() for w in img_words):
                is_img = True
            elif any(w in text.lower() for w in code_words):
                is_code = True
        
        # Анимация
        anim_msg = await message.answer("🧠 Инициализация...")
        anim_type = "img" if is_img else "chat"
        animation = Animation(self.bot, anim_msg.chat.id, anim_msg.message_id, anim_type)
        await animation.start()
        
        try:
            if is_img:
                # Генерация фото
                url, provider = await self.ai.generate_image(text)
                
                if url:
                    await self.bot.send_photo(user.id, url, caption=f"🎨 Готово! ({provider})")
                    await anim_msg.delete()
                else:
                    await animation.stop("⚠️ Не удалось сгенерировать изображение. Попробуйте другой запрос.")
            
            else:
                # Текст
                history = await self.db.get_history(user.id)
                
                system_prompt = f"Ты MerAi GPT v{VERSION}. Создатель: {CREATOR}. Отвечай на русском."
                if is_code:
                    system_prompt += " Ты программист. Пиши чистый, рабочий код в блоках кода."
                
                messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": text}]
                
                answer, provider = await self.ai.generate_response(messages)
                
                # Сохраняем
                await self.db.add_message(user.id, "user", text, provider)
                await self.db.add_message(user.id, "assistant", answer, provider)
                
                # Отправка
                parts = split_text(answer)
                
                if len(parts) > 1:
                    await animation.stop("📝 Отправляю ответ...")
                    for part in parts:
                        await message.answer(part)
                    await anim_msg.delete()
                else:
                    await animation.stop(parts[0])
        
        except Exception as e:
            logger.exception(f"Error in handle_message: {e}")
            await animation.stop(f"⚠️ Произошла ошибка:\n<code>{str(e)[:100]}</code>")
    
    # --- ЗАПУСК ---
    
    async def run(self):
        await self.setup()
        
        logger.info("🚀 Бот запущен!")
        
        try:
            await self.dp.start_polling(
                self.bot, 
                allowed_updates=self.dp.resolve_used_update_types()
            )
        finally:
            await self.bot.session.close()

# --- ENTRY POINT ---

async def main():
    app = MerAiBot()
    await app.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
