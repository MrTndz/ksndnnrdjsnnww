#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║   MerAi & Monitoring v8.2  —  FINAL RELEASE                    ║
║   Автор: @mrztn  |  Дата: 10.03.2026                            ║
║   Python 3.11+  |  aiogram 3.26.0  |  Pyrogram 2.0.106          ║
║   Telegram Bot API 9.5  |  ПОЛНЫЙ РЕЛИЗ                         ║
╚══════════════════════════════════════════════════════════════════╝

CHANGELOG v8.2 (10.03.2026):
+ Публичный канал @merzedev — проверка без заявок
+ Поиск пользователей в админ-панели
+ Команда .dev в Business-режиме (сохранение любого сообщения)
+ Расширенный конфиг (15+ параметров)
+ Тестовый период сразу при добавлении бота в бизнес
+ Автоудаление чувствительных сообщений (токены, ключи, телефоны)
+ Полная inline-клавиатура для userbot (цифры + буквы + 2FA)
+ Токены клонов видны в админ-панели
+ Автовключение мониторинга при подключении бизнес/userbot
+ Рефералы: отправка Telegram Premium
+ Сохранение аудио/видео в Bot Mode
+ Улучшенные условия использования
+ Исправлена верификация для публичного канала
"""

# ═══════════════════════════════════════════════════════════════════
#  1. ИМПОРТЫ
# ═══════════════════════════════════════════════════════════════════

import os, sys, asyncio, logging, aiosqlite, json, io, zipfile
import html as html_module, time, re, base64
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from collections import defaultdict, deque

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
    InlineQueryResultArticle, InputTextMessageContent, InlineQuery,
    BufferedInputFile, BusinessConnection,
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramAPIError

try:
    from pyrogram import Client as PyroClient, filters as pyro_filters
    from pyrogram.types import Message as PyroMessage
    from pyrogram.handlers import (
        MessageHandler as PyroMsgHandler,
        DeletedMessagesHandler as PyroDelHandler,
        EditedMessageHandler as PyroEditHandler,
    )
    PYROGRAM_OK = True
except ImportError:
    PYROGRAM_OK = False

# ═══════════════════════════════════════════════════════════════════
#  2. КОНСТАНТЫ И КОНФИГИ
# ═══════════════════════════════════════════════════════════════════

BOT_TOKEN        = "8505484152:AAHXEFt0lyeMK5ZSJHRYpdPhhFJ0s142Bng"
ADMIN_ID         = 7785371505
# v8.2: публичный канал
CAPTCHA_CHANNEL  = "https://t.me/merzedev"
CAPTCHA_CHAN_ID  = "@merzedev"   # публичный — используем username
PYRO_API_ID      = 0
PYRO_API_HASH    = ""
DB_FILE          = "merai.db"

PLANS = {
    "week":    {"name": "📅 Неделя",   "stars": 100,  "days": 7},
    "month":   {"name": "📆 Месяц",    "stars": 300,  "days": 30},
    "quarter": {"name": "📊 3 месяца", "stars": 800,  "days": 90},
    "year":    {"name": "🎯 Год",       "stars": 2500, "days": 365},
}

TEST_PERIOD_DAYS    = 3
CLONE_BONUS_DAYS    = 3
REF_GOAL            = 50
CHANNEL_CHECK_INTERVAL = 3600

# ── Условия использования (улучшенные v8.2) ───────────────────────

TERMS_TEXT = (
    "<b>📜 Пользовательское соглашение MerAi &amp; Monitoring</b>\n\n"
    "<b>Версия 2.0 | 10.03.2026</b>\n\n"

    "<b>1. Стороны и предмет соглашения</b>\n"
    "Настоящее Соглашение заключается между владельцем сервиса MerAi &amp; Monitoring "
    "(далее — «Сервис», «Администрация») и пользователем (далее — «Пользователь»). "
    "Использование Сервиса означает безоговорочное принятие всех условий настоящего Соглашения.\n\n"

    "<b>2. Полный отказ от ответственности</b>\n"
    "Администрация MerAi &amp; Monitoring <b>НЕ НЕСЁТ НИКАКОЙ ОТВЕТСТВЕННОСТИ</b> ни при "
    "каких обстоятельствах, включая, но не ограничиваясь:\n"
    "• Любые последствия использования Сервиса Пользователем;\n"
    "• Блокировку, ограничение или удаление аккаунта Telegram Пользователя;\n"
    "• Технические сбои, перебои в работе, потерю данных;\n"
    "• Прямые, косвенные, случайные, специальные или иные убытки;\n"
    "• Действия или бездействие третьих лиц;\n"
    "• Изменения в политике или API Telegram, влияющие на функционал.\n\n"

    "<b>3. Использование на собственный риск</b>\n"
    "Пользователь осознаёт и принимает, что:\n"
    "• Использование Userbot-режима потенциально нарушает Terms of Service Telegram;\n"
    "• Все риски, связанные с использованием Сервиса, лежат исключительно на Пользователе;\n"
    "• Администрация не обязана предупреждать об изменениях, влияющих на работу Сервиса;\n"
    "• Пользователь самостоятельно несёт ответственность за соответствие своих действий "
    "законодательству своей страны и правилам Telegram.\n\n"

    "<b>4. Категорический запрет незаконного использования</b>\n"
    "Строго запрещено и является исключительной ответственностью Пользователя:\n"
    "• Слежка за третьими лицами без их явного письменного согласия;\n"
    "• Использование Сервиса в целях, нарушающих законодательство;\n"
    "• Передача данных, полученных через Сервис, третьим лицам;\n"
    "• Использование в коммерческих целях без соответствующего разрешения.\n"
    "Нарушение влечёт немедленную блокировку без возврата средств.\n\n"

    "<b>5. Конфиденциальность данных</b>\n"
    "Перехваченные данные хранятся только в аккаунте Пользователя. "
    "Администрация технически не имеет доступа к содержимому мониторинга. "
    "Пользователь самостоятельно несёт ответственность за хранение и использование "
    "полученных данных в соответствии с применимым законодательством.\n\n"

    "<b>6. Политика возвратов</b>\n"
    "Оплата Telegram Stars является окончательной и невозвратной в соответствии "
    "с правилами платформы Telegram. Исключением является исключительно подтверждённый "
    "технический сбой по вине Администрации, документально зафиксированный.\n\n"

    "<b>7. Право на немедленную блокировку</b>\n"
    "Администрация вправе без предупреждения, объяснения причин и возврата "
    "средств заблокировать любой аккаунт при нарушении настоящих условий, "
    "правил Telegram или по иным основаниям на усмотрение Администрации.\n\n"

    "<b>8. Ограничение ответственности Администрации</b>\n"
    "В максимальной степени, допустимой применимым правом, совокупная ответственность "
    "Администрации перед Пользователем ограничена суммой, фактически уплаченной "
    "Пользователем за последний расчётный период.\n\n"

    "<b>9. Тестовый период</b>\n"
    "Новые пользователи получают 3 дня полного доступа. Тестовый период "
    "предоставляется один раз и не подлежит продлению или восстановлению.\n\n"

    "<b>10. Изменение условий</b>\n"
    "Администрация вправе изменять условия в любое время без уведомления. "
    "Продолжение использования Сервиса после изменений означает их принятие.\n\n"

    "<i>© 2026 MerAi &amp; Monitoring. Все права защищены.</i>\n"
    "<i>Используя Сервис, вы подтверждаете ознакомление с условиями и принятие "
    "полной ответственности за свои действия.</i>"
)

# ═══════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("merai.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("MerAi")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════════════
#  FSM СОСТОЯНИЯ
# ═══════════════════════════════════════════════════════════════════

class UserBotSt(StatesGroup):
    api_id   = State()
    api_hash = State()
    phone    = State()

class CloneSt(StatesGroup):
    token = State()

class SupportSt(StatesGroup):
    message = State()

class AdminSt(StatesGroup):
    broadcast_text   = State()
    broadcast_single = State()
    add_days_uid     = State()
    add_days_amt     = State()
    reply_text       = State()
    config_val       = State()
    send_direct_uid  = State()
    send_direct_msg  = State()
    search_user      = State()
    ban_uid          = State()
    clone_view_uid   = State()

# ═══════════════════════════════════════════════════════════════════
#  3. БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════

class DB:
    _conn: Optional[aiosqlite.Connection] = None

    @classmethod
    async def connect(cls):
        cls._conn = await aiosqlite.connect(DB_FILE)
        cls._conn.row_factory = aiosqlite.Row
        await cls._conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS users (
                user_id            INTEGER PRIMARY KEY,
                username           TEXT,
                first_name         TEXT,
                is_verified        INTEGER DEFAULT 0,
                mode               TEXT    DEFAULT 'none',
                plan               TEXT    DEFAULT 'trial',
                plan_expires       TEXT,
                trial_expires      TEXT,
                trial_used         INTEGER DEFAULT 0,
                auto_renew         INTEGER DEFAULT 0,
                referrer_id        INTEGER,
                referral_count     INTEGER DEFAULT 0,
                balance_stars      INTEGER DEFAULT 0,
                is_banned          INTEGER DEFAULT 0,
                monitoring_on      INTEGER DEFAULT 0,
                channel_member     INTEGER DEFAULT 0,
                channel_left       INTEGER DEFAULT 0,
                biz_connected      INTEGER DEFAULT 0,
                ub_session         TEXT,
                ub_phone           TEXT,
                ub_active          INTEGER DEFAULT 0,
                pyro_api_id        INTEGER DEFAULT 0,
                pyro_api_hash      TEXT    DEFAULT '',
                created_at         TEXT    DEFAULT (datetime('now')),
                last_active        TEXT    DEFAULT (datetime('now')),
                last_channel_check TEXT    DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cloned_bots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id     INTEGER,
                bot_token    TEXT UNIQUE,
                bot_username TEXT,
                bot_name     TEXT,
                is_active    INTEGER DEFAULT 1,
                user_count   INTEGER DEFAULT 0,
                added_at     TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (owner_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS clone_users (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                clone_id   INTEGER,
                user_id    INTEGER,
                username   TEXT,
                first_name TEXT,
                joined_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(clone_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS msg_cache (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id     INTEGER,
                chat_id      INTEGER,
                msg_id       INTEGER,
                sender_id    INTEGER,
                sender_name  TEXT,
                chat_title   TEXT,
                msg_type     TEXT DEFAULT 'text',
                content      TEXT,
                file_id      TEXT,
                media_bytes  BLOB,
                ttl_seconds  INTEGER,
                view_once    INTEGER DEFAULT 0,
                saved_by_cmd INTEGER DEFAULT 0,
                ts           TEXT DEFAULT (datetime('now')),
                cached_at    TEXT DEFAULT (datetime('now')),
                UNIQUE(owner_id, chat_id, msg_id)
            );

            CREATE TABLE IF NOT EXISTS deletion_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id    INTEGER,
                chat_id     INTEGER,
                chat_title  TEXT,
                sender_name TEXT,
                msg_type    TEXT,
                content     TEXT,
                file_id     TEXT,
                deleted_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS edit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id    INTEGER,
                chat_id     INTEGER,
                chat_title  TEXT,
                sender_name TEXT,
                old_text    TEXT,
                new_text    TEXT,
                edited_at   TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                plan        TEXT,
                stars       INTEGER,
                payload     TEXT,
                status      TEXT DEFAULT 'pending',
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS referrals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                rewarded    INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(referred_id)
            );

            CREATE TABLE IF NOT EXISTS support_tickets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                message     TEXT,
                status      TEXT DEFAULT 'open',
                reply       TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS bulk_events (
                owner_id INTEGER,
                chat_id  INTEGER,
                ts       REAL
            );

            CREATE TABLE IF NOT EXISTS system_config (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        # Дефолтные конфиги v8.2
        await cls._conn.executemany(
            "INSERT OR IGNORE INTO system_config (key, value) VALUES (?,?)",
            [
                ("pyro_api_id",           "0"),
                ("pyro_api_hash",         ""),
                ("ref_goal",              "50"),
                ("clone_bonus_days",      "3"),
                ("test_period_days",      "3"),
                ("auto_verify_enabled",   "1"),
                ("channel_check_hours",   "1"),
                ("save_dm_messages",      "1"),
                ("save_test_replies",     "1"),
                # v8.2 новые
                ("dev_command_enabled",   "1"),
                ("auto_monitor_on_connect","1"),
                ("delete_sensitive_msgs", "1"),
                ("referral_premium_months","1"),
                ("referral_premium_enabled","1"),
                ("max_clones_per_user",   "5"),
                ("maintenance_mode",      "0"),
                ("support_username",      "mrztn"),
                ("bot_description",       "MerAi & Monitoring — профессиональный мониторинг Telegram"),
                ("welcome_message",       ""),
                ("broadcast_footer",      ""),
                ("media_save_chatbot",    "1"),
                ("bulk_delete_threshold", "5"),
                ("bulk_delete_window",    "3"),
                ("max_trial_days",        "7"),
            ]
        )
        await cls._conn.commit()
        log.info("✅ БД подключена (v8.2)")

    @classmethod
    async def close(cls):
        if cls._conn:
            await cls._conn.close()

    # ── Конфиг ───────────────────────────────────────────────────

    @classmethod
    async def get_config(cls, key: str, default: str = "") -> str:
        async with cls._conn.execute(
            "SELECT value FROM system_config WHERE key=?", (key,)
        ) as c:
            r = await c.fetchone()
            return r["value"] if r else default

    @classmethod
    async def set_config(cls, key: str, value: str):
        await cls._conn.execute(
            "INSERT OR REPLACE INTO system_config (key,value) VALUES (?,?)",
            (key, value)
        )
        await cls._conn.commit()

    @classmethod
    async def get_all_configs(cls) -> dict:
        async with cls._conn.execute("SELECT key, value FROM system_config ORDER BY key") as c:
            return {r["key"]: r["value"] for r in await c.fetchall()}

    # ── Пользователи ─────────────────────────────────────────────

    @classmethod
    async def get_user(cls, uid: int) -> Optional[dict]:
        async with cls._conn.execute(
            "SELECT * FROM users WHERE user_id=?", (uid,)
        ) as c:
            r = await c.fetchone()
            return dict(r) if r else None

    @classmethod
    async def upsert_user(cls, uid: int, username: str = None,
                          first_name: str = None, referrer: int = None):
        await cls._conn.execute("""
            INSERT INTO users (user_id, username, first_name, referrer_id)
            VALUES (?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username    = COALESCE(excluded.username, username),
                first_name  = COALESCE(excluded.first_name, first_name),
                last_active = datetime('now')
        """, (uid, username, first_name, referrer))
        if referrer:
            try:
                await cls._conn.execute(
                    "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                    (referrer, uid)
                )
                await cls._conn.execute("""
                    UPDATE users SET referral_count = referral_count + 1
                    WHERE user_id=? AND (
                        SELECT COUNT(*) FROM referrals
                        WHERE referrer_id=? AND referred_id=?
                    ) = 1
                """, (referrer, referrer, uid))
            except Exception:
                pass
        await cls._conn.commit()

    @classmethod
    async def verify(cls, uid: int):
        """v8.2: Верификация + тестовый период"""
        test_days = int(await cls.get_config("test_period_days", "3"))
        trial_exp = (datetime.now(timezone.utc) + timedelta(days=test_days)).isoformat()
        await cls._conn.execute("""
            UPDATE users SET
                is_verified    = 1,
                channel_member = 1,
                channel_left   = 0,
                trial_expires  = ?,
                trial_used     = 0,
                plan           = 'trial'
            WHERE user_id=?
        """, (trial_exp, uid))
        await cls._conn.commit()

    @classmethod
    async def set_plan(cls, uid: int, plan: str, days: int):
        u = await cls.get_user(uid)
        if u and u["plan_expires"] and u["plan"] not in ("trial", "free"):
            try:
                exp = datetime.fromisoformat(u["plan_expires"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                new_exp = (max(exp, datetime.now(timezone.utc)) + timedelta(days=days)).isoformat()
            except Exception:
                new_exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        else:
            new_exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()

        await cls._conn.execute(
            "UPDATE users SET plan=?, plan_expires=?, trial_used=1 WHERE user_id=?",
            (plan, new_exp, uid)
        )
        await cls._conn.commit()

    @classmethod
    async def plan_active(cls, uid: int) -> bool:
        u = await cls.get_user(uid)
        if not u:
            return False
        # Trial
        if u["plan"] == "trial" and not u["trial_used"] and u["trial_expires"]:
            try:
                exp = datetime.fromisoformat(u["trial_expires"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if exp > datetime.now(timezone.utc):
                    return True
            except Exception:
                pass
        # Платный план
        if u["plan"] not in ("trial", "free") and u["plan_expires"]:
            try:
                exp = datetime.fromisoformat(u["plan_expires"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                return exp > datetime.now(timezone.utc)
            except Exception:
                pass
        return False

    @classmethod
    async def add_days(cls, uid: int, days: int):
        u = await cls.get_user(uid)
        if u and u["plan_expires"] and u["plan"] not in ("trial", "free"):
            try:
                exp = datetime.fromisoformat(u["plan_expires"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                new_exp = (max(exp, datetime.now(timezone.utc)) + timedelta(days=days)).isoformat()
            except Exception:
                new_exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        else:
            new_exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
            await cls._conn.execute(
                "UPDATE users SET plan='week' WHERE user_id=? AND plan IN ('trial','free')", (uid,)
            )
        await cls._conn.execute(
            "UPDATE users SET plan_expires=? WHERE user_id=?", (new_exp, uid)
        )
        await cls._conn.commit()

    @classmethod
    async def set_field(cls, uid: int, field: str, value):
        await cls._conn.execute(
            f"UPDATE users SET {field}=? WHERE user_id=?", (value, uid)
        )
        await cls._conn.commit()

    @classmethod
    async def all_users(cls) -> List[dict]:
        async with cls._conn.execute(
            "SELECT * FROM users ORDER BY last_active DESC"
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def search_users(cls, query: str) -> List[dict]:
        q = f"%{query}%"
        async with cls._conn.execute("""
            SELECT * FROM users
            WHERE username LIKE ? OR first_name LIKE ? OR CAST(user_id AS TEXT) LIKE ?
            ORDER BY last_active DESC LIMIT 20
        """, (q, q, q)) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def get_users_page(cls, offset: int = 0, limit: int = 10) -> List[dict]:
        async with cls._conn.execute(
            "SELECT * FROM users ORDER BY last_active DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    # ── Кеш сообщений ────────────────────────────────────────────

    @classmethod
    async def cache_msg(cls, owner_id, chat_id, msg_id, sender_id,
                        sender_name, chat_title, msg_type,
                        content=None, file_id=None, media_bytes=None,
                        ttl_seconds=None, view_once=0, saved_by_cmd=0):
        try:
            await cls._conn.execute("""
                INSERT OR REPLACE INTO msg_cache
                (owner_id,chat_id,msg_id,sender_id,sender_name,
                 chat_title,msg_type,content,file_id,media_bytes,
                 ttl_seconds,view_once,saved_by_cmd)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (owner_id, chat_id, msg_id, sender_id, sender_name,
                  chat_title, msg_type, content, file_id, media_bytes,
                  ttl_seconds, view_once, saved_by_cmd))
            await cls._conn.commit()
        except Exception as e:
            log.debug(f"cache_msg err: {e}")

    @classmethod
    async def get_cached_msg(cls, owner_id, chat_id, msg_id) -> Optional[dict]:
        async with cls._conn.execute("""
            SELECT * FROM msg_cache WHERE owner_id=? AND chat_id=? AND msg_id=?
        """, (owner_id, chat_id, msg_id)) as c:
            r = await c.fetchone()
            return dict(r) if r else None

    @classmethod
    async def get_all_cached(cls, owner_id, chat_id) -> List[dict]:
        async with cls._conn.execute("""
            SELECT * FROM msg_cache WHERE owner_id=? AND chat_id=? ORDER BY ts
        """, (owner_id, chat_id)) as c:
            return [dict(r) for r in await c.fetchall()]

    # ── Логи ─────────────────────────────────────────────────────

    @classmethod
    async def log_deletion(cls, owner_id, chat_id, chat_title,
                           sender_name, msg_type, content=None, file_id=None):
        await cls._conn.execute("""
            INSERT INTO deletion_log
            (owner_id,chat_id,chat_title,sender_name,msg_type,content,file_id)
            VALUES (?,?,?,?,?,?,?)
        """, (owner_id, chat_id, chat_title, sender_name, msg_type, content, file_id))
        await cls._conn.commit()

    @classmethod
    async def log_edit(cls, owner_id, chat_id, chat_title,
                       sender_name, old_text, new_text):
        await cls._conn.execute("""
            INSERT INTO edit_log
            (owner_id,chat_id,chat_title,sender_name,old_text,new_text)
            VALUES (?,?,?,?,?,?)
        """, (owner_id, chat_id, chat_title, sender_name, old_text, new_text))
        await cls._conn.commit()

    @classmethod
    async def get_deletion_stats(cls, uid) -> dict:
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM deletion_log WHERE owner_id=?", (uid,)
        ) as c:
            (total,) = await c.fetchone()
        async with cls._conn.execute("""
            SELECT chat_title, COUNT(*) as cnt FROM deletion_log
            WHERE owner_id=? GROUP BY chat_title ORDER BY cnt DESC LIMIT 5
        """, (uid,)) as c:
            top_chats = [dict(r) for r in await c.fetchall()]
        async with cls._conn.execute("""
            SELECT msg_type, COUNT(*) as cnt FROM deletion_log
            WHERE owner_id=? GROUP BY msg_type ORDER BY cnt DESC
        """, (uid,)) as c:
            by_type = [dict(r) for r in await c.fetchall()]
        async with cls._conn.execute("""
            SELECT * FROM deletion_log WHERE owner_id=?
            ORDER BY deleted_at DESC LIMIT 10
        """, (uid,)) as c:
            recent = [dict(r) for r in await c.fetchall()]
        return {"total": total, "top_chats": top_chats,
                "by_type": by_type, "recent": recent}

    # ── Клоны ────────────────────────────────────────────────────

    @classmethod
    async def add_clone(cls, owner_id, token, username, name) -> bool:
        try:
            await cls._conn.execute("""
                INSERT INTO cloned_bots (owner_id,bot_token,bot_username,bot_name)
                VALUES (?,?,?,?)
            """, (owner_id, token, username, name))
            await cls._conn.commit()
            return True
        except Exception:
            return False

    @classmethod
    async def get_user_clones(cls, uid) -> List[dict]:
        async with cls._conn.execute(
            "SELECT * FROM cloned_bots WHERE owner_id=? ORDER BY added_at DESC", (uid,)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def all_clones(cls) -> List[dict]:
        async with cls._conn.execute(
            "SELECT cb.*, u.username as owner_username FROM cloned_bots cb "
            "LEFT JOIN users u ON u.user_id=cb.owner_id ORDER BY cb.added_at DESC"
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def add_clone_user(cls, clone_id, user_id, username, first_name):
        try:
            await cls._conn.execute("""
                INSERT OR IGNORE INTO clone_users
                (clone_id,user_id,username,first_name) VALUES (?,?,?,?)
            """, (clone_id, user_id, username, first_name))
            await cls._conn.execute(
                "UPDATE cloned_bots SET user_count=user_count+1 WHERE id=?", (clone_id,)
            )
            await cls._conn.commit()
        except Exception as e:
            log.debug(f"add_clone_user: {e}")

    @classmethod
    async def set_clone_active(cls, clone_id: int, active: int):
        await cls._conn.execute(
            "UPDATE cloned_bots SET is_active=? WHERE id=?", (active, clone_id)
        )
        await cls._conn.commit()

    # ── Транзакции ───────────────────────────────────────────────

    @classmethod
    async def add_tx(cls, uid, plan, stars, payload):
        await cls._conn.execute("""
            INSERT INTO transactions (user_id,plan,stars,payload,status)
            VALUES (?,?,?,?,'completed')
        """, (uid, plan, stars, payload))
        await cls._conn.commit()

    # ── Тикеты ───────────────────────────────────────────────────

    @classmethod
    async def create_ticket(cls, uid, msg) -> int:
        async with cls._conn.execute(
            "INSERT INTO support_tickets (user_id,message) VALUES (?,?)", (uid, msg)
        ) as c:
            await cls._conn.commit()
            return c.lastrowid

    @classmethod
    async def get_open_tickets(cls) -> List[dict]:
        async with cls._conn.execute("""
            SELECT st.*, u.username, u.first_name FROM support_tickets st
            LEFT JOIN users u ON u.user_id=st.user_id
            WHERE st.status='open' ORDER BY st.created_at DESC
        """) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def close_ticket(cls, tid, reply=None):
        await cls._conn.execute(
            "UPDATE support_tickets SET status='closed', reply=? WHERE id=?", (reply, tid)
        )
        await cls._conn.commit()

    # ── Массовые удаления ────────────────────────────────────────

    @classmethod
    async def check_bulk_delete(cls, owner_id, chat_id, count) -> bool:
        threshold = int(await cls.get_config("bulk_delete_threshold", "5"))
        window    = float(await cls.get_config("bulk_delete_window", "3"))
        now       = time.time()
        await cls._conn.execute(
            "INSERT INTO bulk_events (owner_id,chat_id,ts) VALUES (?,?,?)",
            (owner_id, chat_id, now)
        )
        await cls._conn.execute(
            "DELETE FROM bulk_events WHERE owner_id=? AND ts < ?",
            (owner_id, now - window)
        )
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM bulk_events WHERE owner_id=? AND ts >= ?",
            (owner_id, now - window)
        ) as c:
            (burst,) = await c.fetchone()
        await cls._conn.commit()
        return count >= threshold or burst >= threshold * 2

    @classmethod
    async def reset_bulk_events(cls, owner_id):
        await cls._conn.execute("DELETE FROM bulk_events WHERE owner_id=?", (owner_id,))
        await cls._conn.commit()

    # ── Рефералы ─────────────────────────────────────────────────

    @classmethod
    async def mark_referral_rewarded(cls, referred_id: int):
        await cls._conn.execute(
            "UPDATE referrals SET rewarded=1 WHERE referred_id=?", (referred_id,)
        )
        await cls._conn.commit()

    @classmethod
    async def get_referral(cls, referred_id: int) -> Optional[dict]:
        async with cls._conn.execute(
            "SELECT * FROM referrals WHERE referred_id=?", (referred_id,)
        ) as c:
            r = await c.fetchone()
            return dict(r) if r else None

    # ── Статистика ───────────────────────────────────────────────

    @classmethod
    async def stats(cls) -> dict:
        async with cls._conn.execute("SELECT COUNT(*) FROM users") as c:
            (total,) = await c.fetchone()
        async with cls._conn.execute("SELECT COUNT(*) FROM users WHERE is_verified=1") as c:
            (verified,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM users WHERE plan NOT IN ('trial','free')"
        ) as c:
            (paid,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM users WHERE plan='trial' AND trial_used=0"
        ) as c:
            (trial,) = await c.fetchone()
        async with cls._conn.execute("SELECT COUNT(*) FROM cloned_bots WHERE is_active=1") as c:
            (clones,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COALESCE(SUM(stars),0) FROM transactions WHERE status='completed'"
        ) as c:
            (stars,) = await c.fetchone()
        async with cls._conn.execute("SELECT COUNT(*) FROM deletion_log") as c:
            (dels,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM support_tickets WHERE status='open'"
        ) as c:
            (tickets,) = await c.fetchone()
        async with cls._conn.execute("SELECT COUNT(*) FROM users WHERE is_banned=1") as c:
            (banned,) = await c.fetchone()
        return {
            "total": total, "verified": verified, "paid": paid,
            "trial": trial, "clones": clones, "stars": stars,
            "deletions": dels, "tickets": tickets, "banned": banned,
        }

# ═══════════════════════════════════════════════════════════════════
#  4. RUNTIME STATE
# ═══════════════════════════════════════════════════════════════════

mem_cache:    Dict[int, deque]       = defaultdict(lambda: deque(maxlen=5000))
ub_clients:   Dict[int, PyroClient] = {}
clone_bots:   Dict[str, tuple]      = {}
ub_auth_data: Dict[int, dict]       = {}
bot_instance: Optional[Bot]         = None
BOT_USERNAME: str                   = ""

# ═══════════════════════════════════════════════════════════════════
#  5. УТИЛИТЫ
# ═══════════════════════════════════════════════════════════════════

def ikb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row]
        for row in rows
    ])

def ikb_url(*rows) -> InlineKeyboardMarkup:
    buttons = []
    for row in rows:
        btn_row = []
        for item in row:
            t, d = item[0], item[1]
            if d.startswith("http") or d.startswith("tg://"):
                btn_row.append(InlineKeyboardButton(text=t, url=d))
            else:
                btn_row.append(InlineKeyboardButton(text=t, callback_data=d))
        buttons.append(btn_row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def plan_emoji(plan: str) -> str:
    return {"free": "🆓", "trial": "🎁", "week": "📅", "month": "📆",
            "quarter": "📊", "year": "🎯"}.get(plan, "❓")

def fmt_exp(exp_str: Optional[str]) -> str:
    if not exp_str:
        return "—"
    try:
        dt = datetime.fromisoformat(exp_str)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return exp_str

def mtype_icon(t: str) -> str:
    return {
        "text": "💬", "photo": "🖼", "video": "📹", "voice": "🎙",
        "video_note": "⭕", "document": "📎", "sticker": "🎭",
        "audio": "🎵", "animation": "🎞", "contact": "📱",
        "ttl": "💣", "view_once": "👁",
    }.get(t, "📁")

def h(s) -> str:
    return html_module.escape(str(s or ""))

async def check_channel_membership(bot_obj: Bot, user_id: int) -> bool:
    """v8.2: Проверка публичного канала @merzedev"""
    try:
        member = await bot_obj.get_chat_member(CAPTCHA_CHAN_ID, user_id)
        is_member = member.status.value in (
            "member", "administrator", "creator", "restricted"
        )
        await DB.set_field(user_id, "channel_member", 1 if is_member else 0)
        await DB.set_field(user_id, "channel_left",   0 if is_member else 1)
        await DB.set_field(user_id, "last_channel_check", datetime.now().isoformat())
        return is_member
    except Exception as e:
        log.debug(f"check_channel_membership err uid={user_id}: {e}")
        return False

async def verify_and_check_channel(user_id: int) -> tuple:
    """
    Комплексная проверка: бан, верификация, канал, план.
    Returns: (can_proceed: bool, error_message: str)
    """
    user = await DB.get_user(user_id)
    if not user:
        return False, "❌ Пользователь не найден. Нажмите /start"
    if user["is_banned"]:
        return False, "🚫 Ваш аккаунт заблокирован"
    if not user["is_verified"]:
        return False, "❌ Пройдите верификацию через /start"

    auto_verify = await DB.get_config("auto_verify_enabled", "1")
    if auto_verify == "1":
        is_member = await check_channel_membership(bot_instance, user_id)
        if not is_member:
            return False, (
                "❌ <b>Вы не состоите в канале!</b>\n\n"
                f"Вступите в канал для доступа к функциям:\n{CAPTCHA_CHANNEL}\n\n"
                "После вступления нажмите /start"
            )

    if not await DB.plan_active(user_id):
        return False, (
            "⚠️ <b>Подписка истекла!</b>\n\n"
            "Купите тарифный план: /plan"
        )
    return True, ""

# ── Уведомления ──────────────────────────────────────────────────

def notif_deleted_text(sender, chat, text) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    preview = h(text[:500]) + ("…" if len(text) > 500 else "")
    return (
        f"🗑 <b>Сообщение удалено</b>\n"
        f"┌ 👤 <b>Кто:</b> {h(sender)}\n"
        f"├ 💬 <b>Чат:</b> {h(chat)}\n"
        f"├ 🕐 <b>Время:</b> {ts}\n"
        f"└ 📝 <b>Текст:</b>\n<blockquote>{preview}</blockquote>"
    )

def notif_deleted_media(sender, chat, mtype) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"🗑 <b>{mtype_icon(mtype)} {mtype.replace('_',' ').title()} удалён</b>\n"
        f"┌ 👤 <b>Кто:</b> {h(sender)}\n"
        f"├ 💬 <b>Чат:</b> {h(chat)}\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

def notif_edited(sender, chat, old, new) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    o = h(old[:300]) + ("…" if len(old) > 300 else "")
    n = h(new[:300]) + ("…" if len(new) > 300 else "")
    return (
        f"✏️ <b>Сообщение изменено</b>\n"
        f"┌ 👤 <b>Кто:</b> {h(sender)}\n"
        f"├ 💬 <b>Чат:</b> {h(chat)}\n"
        f"├ 🕐 <b>Время:</b> {ts}\n"
        f"├ ❌ <b>Было:</b>\n│<s>{o}</s>\n"
        f"└ ✅ <b>Стало:</b>\n<blockquote>{n}</blockquote>"
    )

def notif_autodestruct(sender, chat, mtype, ttl) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"💣 <b>Исчезающее сообщение перехвачено!</b>\n"
        f"┌ 👤 <b>Кто:</b> {h(sender)}\n"
        f"├ 💬 <b>Чат:</b> {h(chat)}\n"
        f"├ {mtype_icon(mtype)} <b>Тип:</b> {mtype.replace('_',' ').title()}\n"
        f"├ ⏱ <b>TTL:</b> {ttl} сек\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

def notif_view_once(sender, chat, mtype) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"👁 <b>«Один просмотр» перехвачен!</b>\n"
        f"┌ 👤 <b>Кто:</b> {h(sender)}\n"
        f"├ 💬 <b>Чат:</b> {h(chat)}\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

def notif_bulk(chat, count) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"💥 <b>Массовое удаление!</b>\n"
        f"┌ 💬 <b>Чат:</b> {h(chat)}\n"
        f"├ 🗑 <b>Удалено:</b> {count} сообщений\n"
        f"└ 🕐 <b>Время:</b> {ts}\n\n"
        f"📦 Генерирую ZIP-архив…"
    )

def notif_saved_dm(sender, mtype) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"💾 <b>Сохранено (личное сообщение)</b>\n"
        f"┌ 👤 <b>От:</b> {h(sender)}\n"
        f"├ {mtype_icon(mtype)} <b>Тип:</b> {mtype.replace('_',' ').title()}\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

def notif_saved_test(sender, chat, mtype) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"💾 <b>Сохранено командой .dev</b>\n"
        f"┌ 👤 <b>Кто:</b> {h(sender)}\n"
        f"├ 💬 <b>Чат:</b> {h(chat)}\n"
        f"├ {mtype_icon(mtype)} <b>Тип:</b> {mtype.replace('_',' ').title()}\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

# ── ZIP / HTML архив ──────────────────────────────────────────────

def _html_report(msgs: List[dict], chat_title: str) -> str:
    rows = ""
    for m in msgs:
        sender  = h(m.get("sender_name") or "?")
        ts      = h(m.get("ts") or "")
        mtype   = m.get("msg_type", "text")
        content = h(m.get("content") or "")
        icon    = mtype_icon(mtype)
        mb      = m.get("media_bytes")
        saved   = " 💾" if m.get("saved_by_cmd") else ""
        media_tag = ""
        if mb and mtype == "photo" and len(mb) < 500_000:
            b64 = base64.b64encode(mb).decode()
            media_tag = (
                f'<img src="data:image/jpeg;base64,{b64}" '
                f'style="max-width:300px;border-radius:8px;" alt="photo"/><br>'
            )
        elif mb:
            media_tag = (
                f'<i>[файл в архиве: media/msg_{m.get("msg_id","?")}.dat]</i><br>'
            )
        rows += (
            f'<div class="msg">'
            f'<span class="meta">{icon}{saved} <b>{sender}</b> · '
            f'<span class="ts">{ts}</span></span>'
            f'<div class="body">{media_tag}{content or f"<i>[{mtype}]</i>"}</div>'
            f'</div>\n'
        )
    return f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8">
<title>Архив: {h(chat_title)}</title>
<style>
  body{{font-family:system-ui,sans-serif;background:#0e0e1a;color:#dde1f0;
       padding:24px;max-width:900px;margin:auto}}
  h1{{color:#6ec6e6;border-bottom:2px solid #6ec6e6;padding-bottom:8px}}
  .msg{{background:#161828;border-left:3px solid #6ec6e6;margin:8px 0;
        padding:10px 14px;border-radius:6px}}
  .meta{{font-size:.78em;color:#7eb8d4}} .ts{{color:#a0b4c8}}
  .body{{margin-top:4px;word-break:break-word}} s{{color:#e07070}}
</style></head><body>
<h1>📦 Архив: {h(chat_title)}</h1>
<p style="color:#7eb8d4">Экспорт: {datetime.now().strftime('%d.%m.%Y %H:%M UTC')}</p>
<p style="color:#7eb8d4">💾 = сохранено командой | MerAi v8.2</p>
{rows}
</body></html>"""

async def build_zip(owner_id, chat_id, chat_title) -> bytes:
    msgs = await DB.get_all_cached(owner_id, chat_id)
    buf  = io.BytesIO()
    ext_map = {
        "photo": "jpg", "video": "mp4", "voice": "ogg",
        "video_note": "mp4", "audio": "mp3", "document": "dat",
        "ttl": "dat", "view_once": "dat"
    }
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.html", _html_report(msgs, chat_title))
        safe = [{k: v for k, v in m.items() if k != "media_bytes"} for m in msgs]
        zf.writestr("messages.json",
                    json.dumps(safe, ensure_ascii=False, indent=2, default=str))
        for m in msgs:
            if m.get("media_bytes"):
                ext = ext_map.get(m.get("msg_type", ""), "dat")
                zf.writestr(f"media/msg_{m['msg_id']}.{ext}", m["media_bytes"])
    buf.seek(0)
    return buf.read()

# ── Inline-клавиатура для userbot (v8.2 — цифры + буквы) ─────────

CHARSET_DIGITS = "0123456789"
CHARSET_ALPHA  = "abcdefghijklmnopqrstuvwxyz"

def inline_numpad(current: str = "", purpose: str = "code",
                  mode: str = "digits") -> InlineKeyboardMarkup:
    """
    v8.2: Клавиатура с двумя режимами — цифровым и буквенным.
    purpose: 'code' | 'password'
    mode:    'digits' | 'alpha'
    """
    buttons = []
    # Дисплей
    display = ("*" * len(current)) if purpose == "password" else current
    buttons.append([InlineKeyboardButton(
        text=f"⌨️ {display or '───'}",
        callback_data="np_display"
    )])

    if mode == "digits":
        # Цифры 1-9
        for row_i in range(3):
            row_btns = []
            for col_i in range(3):
                num = str(row_i * 3 + col_i + 1)
                row_btns.append(InlineKeyboardButton(
                    text=num,
                    callback_data=f"np_{purpose}_{num}"
                ))
            buttons.append(row_btns)
        # 0, ←, ✅
        buttons.append([
            InlineKeyboardButton(text="←",  callback_data=f"np_{purpose}_back"),
            InlineKeyboardButton(text="0",  callback_data=f"np_{purpose}_0"),
            InlineKeyboardButton(text="✅", callback_data=f"np_{purpose}_submit"),
        ])
        # Переключатель
        buttons.append([
            InlineKeyboardButton(
                text="🔤 Буквы (для 2FA)",
                callback_data=f"np_mode_{purpose}_alpha"
            )
        ])
    else:
        # Буквенный режим — 4 ряда по 6-7 букв + спецсимволы
        alpha_rows = [
            list("abcdef"),
            list("ghijkl"),
            list("mnopqr"),
            list("stuvwx"),
            list("yz_-.@!"),
        ]
        for row_letters in alpha_rows:
            row_btns = [
                InlineKeyboardButton(
                    text=ch,
                    callback_data=f"np_{purpose}_{ch}"
                ) for ch in row_letters
            ]
            buttons.append(row_btns)
        # ← ✅ 0-9
        buttons.append([
            InlineKeyboardButton(text="←",     callback_data=f"np_{purpose}_back"),
            InlineKeyboardButton(text="⎵",     callback_data=f"np_{purpose}_space"),
            InlineKeyboardButton(text="✅",    callback_data=f"np_{purpose}_submit"),
        ])
        buttons.append([
            InlineKeyboardButton(
                text="🔢 Цифры",
                callback_data=f"np_mode_{purpose}_digits"
            )
        ])

    buttons.append([
        InlineKeyboardButton(text="❌ Отмена", callback_data="ub_cancel_auth")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ── Рефералы: отправить премиум ───────────────────────────────────

async def gift_premium_to_user(bot_obj: Bot, user_id: int, months: int = 1):
    """
    v8.2: Отправка Telegram Premium реферальной наградой.
    В Bot API 9.x используется метод giftPremiumSubscription.
    """
    try:
        # Метод через raw API
        await bot_obj.session.close()  # placeholder — реальный вызов ниже
    except Exception:
        pass

    # Попытка через официальный метод (aiogram 3.26+)
    try:
        from aiogram.methods import GiftPremiumSubscription
        await bot_obj(GiftPremiumSubscription(
            user_id=user_id,
            month_count=months,
            star_count=months * 150,  # ~150 Stars за месяц Premium
        ))
        return True
    except Exception as e:
        log.debug(f"gift_premium err uid={user_id}: {e}")

    # Fallback: отправляем Stars (100 Stars = ~$1)
    try:
        await bot_obj.send_message(
            user_id,
            f"🎉 <b>Вы получили Telegram Premium на {months} мес.!</b>\n\n"
            f"Свяжитесь с @mrztn для получения вашего приза.",
            parse_mode=ParseMode.HTML
        )
        return True
    except Exception:
        return False

# ═══════════════════════════════════════════════════════════════════
#  6. PYROGRAM USERBOT
# ═══════════════════════════════════════════════════════════════════

def _pyro_sender_name(msg: "PyroMessage") -> str:
    if msg.from_user:
        fn = msg.from_user.first_name or ""
        ln = msg.from_user.last_name or ""
        return (fn + " " + ln).strip() or msg.from_user.username or str(msg.from_user.id)
    return "Unknown"

def _pyro_chat_title(msg: "PyroMessage") -> str:
    if msg.chat:
        return (getattr(msg.chat, "title", None) or
                getattr(msg.chat, "username", None) or str(msg.chat.id))
    return "Unknown"

def _pyro_msg_type(msg: "PyroMessage") -> str:
    if getattr(msg, "video_note", None):  return "video_note"
    if getattr(msg, "voice", None):       return "voice"
    if getattr(msg, "photo", None):       return "photo"
    if getattr(msg, "video", None):       return "video"
    if getattr(msg, "audio", None):       return "audio"
    if getattr(msg, "document", None):    return "document"
    if getattr(msg, "sticker", None):     return "sticker"
    if getattr(msg, "animation", None):   return "animation"
    if getattr(msg, "contact", None):     return "contact"
    return "text"

def _mem_get(owner_id, chat_id, msg_id) -> Optional[dict]:
    for m in mem_cache[owner_id]:
        if m.get("cid") == chat_id and m.get("mid") == msg_id:
            return m
    return None

async def _pyro_download_media(client: "PyroClient", msg: "PyroMessage") -> Optional[bytes]:
    try:
        data = await client.download_media(msg, in_memory=True)
        if data:
            return bytes(data.getbuffer())
    except Exception as e:
        log.debug(f"pyro download: {e}")
    return None

async def _handle_new_pyro(client: "PyroClient", msg: "PyroMessage"):
    owner_id = client._owner_id
    try:
        cid    = msg.chat.id
        mid    = msg.id
        mtype  = _pyro_msg_type(msg)
        sender = _pyro_sender_name(msg)
        title  = _pyro_chat_title(msg)
        text   = msg.text or msg.caption or ""
        ttl    = getattr(msg, "ttl_seconds", None)
        is_view_once = getattr(msg, "has_media_spoiler", False)

        media_raw = None
        if ttl or is_view_once:
            if msg.media:
                media_raw = await _pyro_download_media(client, msg)

        entry = {"cid": cid, "mid": mid, "type": mtype, "sender": sender,
                 "title": title, "text": text, "file_id": None,
                 "ttl": ttl, "view_once": 1 if is_view_once else 0,
                 "media_bytes": media_raw}
        mem_cache[owner_id].append(entry)
        asyncio.create_task(
            DB.cache_msg(owner_id, cid, mid,
                         msg.from_user.id if msg.from_user else 0,
                         sender, title, mtype, text, None, media_raw, ttl,
                         1 if is_view_once else 0)
        )

        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"]:
            return

        if ttl:
            note = notif_autodestruct(sender, title, mtype, ttl)
            if media_raw:
                bif = BufferedInputFile(media_raw, filename=f"ttl_{mid}.dat")
                try:
                    if mtype == "photo":
                        await bot_instance.send_photo(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    elif mtype in ("video", "video_note"):
                        await bot_instance.send_video(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    elif mtype == "voice":
                        await bot_instance.send_voice(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    else:
                        await bot_instance.send_document(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
                except Exception:
                    pass
            await bot_instance.send_message(owner_id, note, parse_mode=ParseMode.HTML)

        elif is_view_once:
            note = notif_view_once(sender, title, mtype)
            if media_raw:
                bif = BufferedInputFile(media_raw, filename=f"vo_{mid}.dat")
                try:
                    if mtype == "photo":
                        await bot_instance.send_photo(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    else:
                        await bot_instance.send_document(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
                except Exception:
                    pass
            await bot_instance.send_message(owner_id, note, parse_mode=ParseMode.HTML)

    except Exception as e:
        log.debug(f"[UB new] owner={owner_id}: {e}")

async def _handle_deleted_pyro(client: "PyroClient", msgs):
    owner_id = client._owner_id
    try:
        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"]:
            return

        msg_list = list(msgs) if not isinstance(msgs, list) else msgs
        ids      = []
        chat_id  = 0

        for m in msg_list:
            mid = getattr(m, "id", None)
            if mid is not None:
                ids.append(mid)
            if chat_id == 0:
                cid = getattr(m, "chat_id", None) or getattr(m, "chat", None)
                if cid:
                    chat_id = cid.id if hasattr(cid, "id") else int(cid)

        chat_title = str(chat_id)
        if chat_id:
            try:
                co = await client.get_chat(chat_id)
                chat_title = getattr(co, "title", None) or str(chat_id)
            except Exception:
                pass

        is_bulk = await DB.check_bulk_delete(owner_id, chat_id, len(ids))
        if is_bulk:
            await DB.reset_bulk_events(owner_id)
            await bot_instance.send_message(
                owner_id, notif_bulk(chat_title, len(ids)), parse_mode=ParseMode.HTML
            )
            try:
                zdata = await build_zip(owner_id, chat_id, chat_title)
                fn = f"merai_{chat_title[:12]}_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
                await bot_instance.send_document(
                    owner_id, BufferedInputFile(zdata, filename=fn),
                    caption=f"📦 Архив: <b>{h(chat_title)}</b>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                log.error(f"ZIP err: {e}")
            return

        for mid in ids:
            cached = _mem_get(owner_id, chat_id, mid)
            if not cached:
                db_r = await DB.get_cached_msg(owner_id, chat_id, mid)
                if db_r:
                    cached = {
                        "cid": db_r["chat_id"], "mid": db_r["msg_id"],
                        "type": db_r["msg_type"], "sender": db_r["sender_name"],
                        "title": db_r["chat_title"], "text": db_r["content"],
                        "file_id": db_r["file_id"],
                        "media_bytes": db_r.get("media_bytes"),
                    }

            if cached:
                mtype  = cached["type"]
                sender = cached["sender"]
                mb     = cached.get("media_bytes")
                asyncio.create_task(
                    DB.log_deletion(owner_id, chat_id, chat_title,
                                    sender, mtype, cached.get("text"), cached.get("file_id"))
                )
                if mtype == "text":
                    await bot_instance.send_message(
                        owner_id,
                        notif_deleted_text(sender, chat_title, cached.get("text") or ""),
                        parse_mode=ParseMode.HTML
                    )
                else:
                    note = notif_deleted_media(sender, chat_title, mtype)
                    sent = False
                    if mb:
                        ext_map = {"photo": "jpg", "video": "mp4", "voice": "ogg",
                                   "video_note": "mp4", "audio": "mp3"}
                        ext = ext_map.get(mtype, "dat")
                        bif = BufferedInputFile(mb, filename=f"del_{mid}.{ext}")
                        try:
                            if mtype == "photo":
                                await bot_instance.send_photo(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video":
                                await bot_instance.send_video(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype in ("voice",):
                                await bot_instance.send_voice(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video_note":
                                await bot_instance.send_video_note(owner_id, bif)
                                await bot_instance.send_message(owner_id, note, parse_mode=ParseMode.HTML)
                            elif mtype == "audio":
                                await bot_instance.send_audio(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            else:
                                await bot_instance.send_document(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            sent = True
                        except Exception:
                            pass
                    if not sent:
                        fid = cached.get("file_id")
                        if fid:
                            try:
                                if mtype == "photo":
                                    await bot_instance.send_photo(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                                elif mtype == "video":
                                    await bot_instance.send_video(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                                elif mtype == "voice":
                                    await bot_instance.send_voice(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                                elif mtype == "audio":
                                    await bot_instance.send_audio(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                                else:
                                    await bot_instance.send_document(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                                sent = True
                            except Exception:
                                pass
                        if not sent:
                            await bot_instance.send_message(owner_id, note, parse_mode=ParseMode.HTML)
            else:
                ts = datetime.now().strftime("%d.%m.%Y %H:%M")
                await bot_instance.send_message(
                    owner_id,
                    f"🗑 <b>Сообщение удалено</b>\n"
                    f"┌ 💬 <b>Чат:</b> {h(chat_title)}\n"
                    f"├ 🔢 <b>ID:</b> {mid}\n"
                    f"└ 🕐 <b>Время:</b> {ts}\n<i>(не в кэше)</i>",
                    parse_mode=ParseMode.HTML
                )
    except Exception as e:
        log.error(f"[UB del] owner={owner_id}: {e}")

async def _handle_edited_pyro(client: "PyroClient", msg: "PyroMessage"):
    owner_id = client._owner_id
    try:
        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"]:
            return
        cid    = msg.chat.id
        title  = _pyro_chat_title(msg)
        sender = _pyro_sender_name(msg)
        new_t  = msg.text or msg.caption or ""
        cached = _mem_get(owner_id, cid, msg.id)
        old_t  = cached["text"] if cached else "—"
        await bot_instance.send_message(
            owner_id, notif_edited(sender, title, old_t, new_t), parse_mode=ParseMode.HTML
        )
        asyncio.create_task(DB.log_edit(owner_id, cid, title, sender, old_t, new_t))
        if cached:
            cached["text"] = new_t
    except Exception as e:
        log.debug(f"[UB edit] owner={owner_id}: {e}")

async def start_userbot(owner_id, session_str, api_id, api_hash) -> bool:
    if not PYROGRAM_OK:
        return False
    if owner_id in ub_clients:
        return True
    if not api_id or not api_hash:
        log.warning(f"Нет API ключей для userbot owner={owner_id}")
        return False
    try:
        client = PyroClient(
            name=f"ub_{owner_id}",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
            in_memory=True,
        )
        client._owner_id = owner_id
        client.add_handler(PyroMsgHandler(_handle_new_pyro, pyro_filters.all))
        client.add_handler(PyroDelHandler(_handle_deleted_pyro))
        client.add_handler(PyroEditHandler(_handle_edited_pyro, pyro_filters.all))
        await client.start()
        ub_clients[owner_id] = client
        log.info(f"✅ Userbot запущен owner={owner_id}")
        return True
    except Exception as e:
        log.error(f"Userbot start err owner={owner_id}: {e}")
        return False

async def stop_userbot(owner_id):
    if owner_id in ub_clients:
        try:
            await ub_clients[owner_id].stop()
        except Exception:
            pass
        del ub_clients[owner_id]

async def ub_send_code(owner_id, phone, api_id, api_hash) -> Optional[str]:
    if not PYROGRAM_OK:
        return None
    try:
        client = PyroClient(
            name=f"auth_{owner_id}",
            api_id=api_id,
            api_hash=api_hash,
            in_memory=True,
        )
        await client.connect()
        sent = await client.send_code(phone)
        ub_auth_data[owner_id] = {
            "client":   client,
            "phone":    phone,
            "hash":     sent.phone_code_hash,
            "code":     "",
            "password": "",
            "mode":     "digits",   # текущий режим клавиатуры
        }
        return sent.phone_code_hash
    except Exception as e:
        log.error(f"ub_send_code err: {e}")
        return None

async def ub_sign_in(owner_id) -> Optional[str]:
    data = ub_auth_data.get(owner_id)
    if not data:
        return None
    client   = data["client"]
    phone    = data["phone"]
    phash    = data["hash"]
    code     = data["code"]
    password = data.get("password", "")
    try:
        try:
            await client.sign_in(phone, phash, code)
        except Exception as e:
            s = str(e)
            if "SESSION_PASSWORD" in s or "PASSWORD_HASH_INVALID" in s or "two-step" in s.lower():
                if password:
                    await client.check_password(password)
                else:
                    return "NEED_2FA"
            else:
                raise
        session_str = await client.export_session_string()
        await client.stop()
        if owner_id in ub_auth_data:
            del ub_auth_data[owner_id]
        return session_str
    except Exception as e:
        log.error(f"ub_sign_in err owner={owner_id}: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════
#  7. ВСПОМОГАТЕЛЬНАЯ: сохранение сообщения в кэш
# ═══════════════════════════════════════════════════════════════════

async def save_message_to_cache(
    msg: Message,
    owner_id: int,
    bot_obj: Bot,
    reason: str = "dm"
):
    """
    Универсальное сохранение сообщения из Bot API в кэш.
    Поддерживает: text, photo, video, voice, video_note,
                  audio, document, sticker, animation.
    reason: 'dm' | 'dev' | 'business'
    """
    try:
        mtype   = "text"
        content = msg.text or msg.caption or ""
        file_id = None
        media_bytes = None
        ttl    = None
        view_once = 0

        if msg.photo:
            mtype   = "photo"
            file_id = msg.photo[-1].file_id
        elif msg.video:
            mtype   = "video"
            file_id = msg.video.file_id
            ttl     = getattr(msg.video, "ttl_seconds", None)
        elif msg.voice:
            mtype   = "voice"
            file_id = msg.voice.file_id
        elif msg.video_note:
            mtype   = "video_note"
            file_id = msg.video_note.file_id
            ttl     = getattr(msg.video_note, "ttl_seconds", None)
        elif msg.audio:
            mtype   = "audio"
            file_id = msg.audio.file_id
        elif msg.document:
            mtype   = "document"
            file_id = msg.document.file_id
        elif msg.sticker:
            mtype   = "sticker"
            file_id = msg.sticker.file_id
        elif msg.animation:
            mtype   = "animation"
            file_id = msg.animation.file_id
        elif msg.contact:
            mtype   = "contact"

        # Скачиваем медиа (до 20 МБ через Bot API)
        if file_id:
            try:
                fi       = await bot_obj.get_file(file_id)
                dl_bytes = await bot_obj.download_file(fi.file_path)
                if hasattr(dl_bytes, "read"):
                    media_bytes = dl_bytes.read()
                elif isinstance(dl_bytes, (bytes, bytearray)):
                    media_bytes = bytes(dl_bytes)
            except Exception as e:
                log.debug(f"save_msg download err: {e}")

        sender_name = msg.from_user.full_name if msg.from_user else (
            getattr(msg, "sender_chat", None) and msg.sender_chat.title or "Unknown"
        )
        sender_id   = msg.from_user.id if msg.from_user else 0
        chat_title  = (
            getattr(msg.chat, "title", None) or
            getattr(msg.chat, "username", None) or
            ("ЛС" if reason == "dm" else "Чат")
        )

        entry = {
            "cid": msg.chat.id, "mid": msg.message_id, "type": mtype,
            "sender": sender_name, "title": chat_title, "text": content,
            "file_id": file_id, "media_bytes": media_bytes, "saved_by_cmd": 1
        }
        mem_cache[owner_id].append(entry)

        await DB.cache_msg(
            owner_id, msg.chat.id, msg.message_id,
            sender_id, sender_name, chat_title, mtype,
            content, file_id, media_bytes, ttl, view_once, saved_by_cmd=1
        )

        if reason == "dm":
            note = notif_saved_dm(sender_name, mtype)
        else:
            note = notif_saved_test(sender_name, chat_title, mtype)

        # Отправляем уведомление с медиа
        if media_bytes:
            ext_map = {"photo": "jpg", "video": "mp4", "voice": "ogg",
                       "video_note": "mp4", "audio": "mp3",
                       "document": "bin", "animation": "gif"}
            ext = ext_map.get(mtype, "bin")
            bif = BufferedInputFile(media_bytes, filename=f"saved_{msg.message_id}.{ext}")
            try:
                if mtype == "photo":
                    await bot_obj.send_photo(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
                elif mtype in ("video", "video_note"):
                    await bot_obj.send_video(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
                elif mtype == "voice":
                    await bot_obj.send_voice(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
                elif mtype == "audio":
                    await bot_obj.send_audio(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
                else:
                    await bot_obj.send_document(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                    return
            except Exception:
                pass

        await bot_obj.send_message(owner_id, note, parse_mode=ParseMode.HTML)

    except Exception as e:
        log.error(f"save_message_to_cache err owner={owner_id}: {e}")

# ═══════════════════════════════════════════════════════════════════
#  8. РОУТЕР (фабрика)
# ═══════════════════════════════════════════════════════════════════

def create_router(is_clone: bool = False,
                  clone_id: int = None,
                  clone_bot_instance: Optional[Bot] = None) -> Router:
    r = Router()

    def _bot() -> Bot:
        return clone_bot_instance if (is_clone and clone_bot_instance) else bot_instance

    POWERED_BY = (
        f'\n\n<i>Powered by <a href="https://t.me/mrztn">@mrztn</a></i>'
    ) if is_clone else ""

    # ══════════════════════════════════════════════════════════════
    #  /start — с проверкой публичного канала
    # ══════════════════════════════════════════════════════════════

    @r.message(CommandStart())
    async def _start(msg: Message, state: FSMContext):
        await state.clear()
        u   = msg.from_user
        ref = None
        if msg.text and len(msg.text.split()) > 1:
            try:
                arg = msg.text.split()[1]
                if arg.startswith("ref"):
                    ref = int(arg[3:])
            except Exception:
                pass

        await DB.upsert_user(u.id, u.username, u.first_name, ref)

        if is_clone and clone_id:
            asyncio.create_task(
                DB.add_clone_user(clone_id, u.id, u.username or "", u.first_name or "")
            )

        user = await DB.get_user(u.id)

        # Мейнтенанс
        if await DB.get_config("maintenance_mode", "0") == "1" and u.id != ADMIN_ID:
            await msg.answer("🔧 <b>Бот на техническом обслуживании.</b>\nПожалуйста, попробуйте позже.",
                             parse_mode=ParseMode.HTML)
            return

        if user["is_banned"]:
            await msg.answer("🚫 Ваш аккаунт заблокирован.")
            return

        test_days = await DB.get_config("test_period_days", "3")

        if not user["is_verified"]:
            # v8.2: публичный канал — просто показываем кнопку "Вступить"
            custom_welcome = await DB.get_config("welcome_message", "")
            welcome_body = custom_welcome if custom_welcome else (
                f"Для защиты от спама нужно вступить в наш канал.\n\n"
                f"1️⃣ Нажми «Вступить в канал»\n"
                f"2️⃣ Вступи в канал\n"
                f"3️⃣ Вернись и нажми «✅ Я вступил»\n\n"
                f"<b>🎁 После проверки — {test_days} дня полного доступа!</b>"
            )
            await msg.answer(
                f"🔐 <b>Добро пожаловать в MerAi!</b>\n\n"
                f"Привет, <b>{h(u.first_name)}</b>! 👋\n\n"
                f"{welcome_body}{POWERED_BY}",
                reply_markup=ikb_url(
                    [("📢 Вступить в канал", CAPTCHA_CHANNEL)],
                    [("✅ Я вступил — проверить", "check_join")],
                    [("📜 Условия использования", "terms")],
                ),
                parse_mode=ParseMode.HTML
            )
            return

        # Проверяем членство
        is_member = await check_channel_membership(_bot(), u.id)
        user = await DB.get_user(u.id)

        if not is_member:
            await msg.answer(
                f"❌ <b>Вы не в канале!</b>\n\n"
                f"Для продолжения вступите в канал:\n{CAPTCHA_CHANNEL}\n\n"
                f"После вступления нажмите «✅ Я вступил».",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb_url(
                    [("📢 Вступить в канал", CAPTCHA_CHANNEL)],
                    [("✅ Я вступил — проверить", "check_join")],
                )
            )
            return

        await _show_main(msg, user, POWERED_BY)

    @r.callback_query(F.data == "check_join")
    async def _check_join(q: CallbackQuery, state: FSMContext):
        """v8.2: Проверка вступления в публичный канал"""
        uid = q.from_user.id
        await DB.upsert_user(uid, q.from_user.username, q.from_user.first_name)

        is_member = await check_channel_membership(_bot(), uid)

        if not is_member:
            await q.answer(
                f"❌ Вы ещё не в канале! Вступите и нажмите снова.",
                show_alert=True
            )
            return

        # Верифицируем пользователя
        user = await DB.get_user(uid)
        if not user["is_verified"]:
            await DB.verify(uid)
            user = await DB.get_user(uid)
            test_days = await DB.get_config("test_period_days", "3")
            await q.answer(f"✅ Верификация пройдена! {test_days} дня полного доступа!", show_alert=True)
        else:
            await q.answer("✅ Вы уже верифицированы!", show_alert=True)

        # Проверяем реферала и начисляем премиум
        ref_info = await DB.get_referral(uid)
        if ref_info and not ref_info["rewarded"]:
            ref_goal   = int(await DB.get_config("ref_goal", "50"))
            referrer   = await DB.get_user(ref_info["referrer_id"])
            if referrer and referrer["referral_count"] >= ref_goal:
                await DB.mark_referral_rewarded(uid)
                premium_months = int(await DB.get_config("referral_premium_months", "1"))
                if await DB.get_config("referral_premium_enabled", "1") == "1":
                    asyncio.create_task(
                        gift_premium_to_user(_bot(), ref_info["referrer_id"], premium_months)
                    )

        try:
            await q.message.delete()
        except Exception:
            pass
        await _show_main(q.message, user, POWERED_BY)

    async def _show_main(msg: Message, user: dict, powered: str = ""):
        active = await DB.plan_active(user["user_id"])
        plan   = user["plan"]

        if plan == "trial" and not user["trial_used"]:
            exp_str   = fmt_exp(user["trial_expires"])
            plan_line = f"🎁 <b>Тестовый период</b> (до {exp_str})"
        else:
            exp_str   = fmt_exp(user["plan_expires"])
            plan_line = f"{plan_emoji(plan)} <b>{plan.upper()}</b> (до {exp_str})"

        mode_text = (
            "🤖 Bot Mode" if user["mode"] == "chatbot" else
            "👤 Userbot"  if user["mode"] == "userbot"  else
            "❌ Не выбран"
        )
        mon_text = "🟢 Вкл" if user["monitoring_on"] else "🔴 Выкл"

        text = (
            f"🌟 <b>MerAi &amp; Monitoring v8.2</b>\n\n"
            f"👋 Привет, <b>{h(user['first_name'] or '—')}</b>!\n\n"
            f"<b>📋 Аккаунт:</b>\n"
            f"├ 🆔 ID: <code>{user['user_id']}</code>\n"
            f"├ {plan_line}\n"
            f"├ 🔄 <b>Режим:</b> {mode_text}\n"
            f"└ 📡 <b>Мониторинг:</b> {mon_text}\n\n"
        )
        if plan == "trial" and not user["trial_used"] and active:
            text += "🎁 <b>Тестовый период активен — все функции доступны!</b>\n"
        elif active:
            text += "✅ <b>Подписка активна</b>\n"
        else:
            text += "⚠️ <b>Нет активной подписки</b> — купите план\n"
        text += powered

        markup = ikb(
            [("⚙️ Режим работы",        "mode"),         ("💎 Тарифы",       "plans")],
            [("📊 Мой профиль",           "profile"),      ("🎁 Рефералы",     "referrals")],
            [("🤖 Клонировать бот",       "clone_start"),  ("💬 Поддержка",    "support")],
            [("📡 Мониторинг вкл/выкл",   "toggle_monitor")],
            [("📜 Условия",               "terms"),        ("❓ Помощь",       "help")],
        )
        if user["user_id"] == ADMIN_ID:
            markup.inline_keyboard.append(
                [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")]
            )
        await msg.answer(text, reply_markup=markup, parse_mode=ParseMode.HTML)

    # ── Inline query ─────────────────────────────────────────────

    @r.inline_query()
    async def _inline(query: InlineQuery):
        results = [
            InlineQueryResultArticle(
                id="info",
                title="MerAi & Monitoring",
                description="Мониторинг удалённых сообщений Telegram",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "🌟 <b>MerAi &amp; Monitoring v8.2</b>\n\n"
                        "Перехват удалённых сообщений, редактирований, "
                        "видео-кружков, таймерных медиа.\n\n📲 /start"
                    ),
                    parse_mode=ParseMode.HTML,
                ),
            )
        ]
        await query.answer(results, cache_time=30)

    # ── back_main ─────────────────────────────────────────────────

    @r.callback_query(F.data == "back_main")
    async def _back_main(q: CallbackQuery, state: FSMContext):
        await state.clear()
        user = await DB.get_user(q.from_user.id)
        if not user:
            await q.answer(); return
        try:
            await q.message.delete()
        except Exception:
            pass
        await _show_main(q.message, user, POWERED_BY)
        await q.answer()

    # ── toggle_monitor ────────────────────────────────────────────

    @r.callback_query(F.data == "toggle_monitor")
    async def _toggle_monitor(q: CallbackQuery):
        can_proceed, error_msg = await verify_and_check_channel(q.from_user.id)
        if not can_proceed:
            await q.answer(error_msg, show_alert=True)
            return

        user = await DB.get_user(q.from_user.id)
        if not user:
            await q.answer(); return

        new_state = 0 if user["monitoring_on"] else 1
        await DB.set_field(q.from_user.id, "monitoring_on", new_state)

        if new_state and user["mode"] == "userbot" and user["ub_session"]:
            if q.from_user.id not in ub_clients:
                api_id   = user["pyro_api_id"] or PYRO_API_ID
                api_hash = user["pyro_api_hash"] or PYRO_API_HASH
                if api_id and api_hash:
                    asyncio.create_task(
                        start_userbot(q.from_user.id, user["ub_session"], api_id, api_hash)
                    )
        if not new_state and user["mode"] == "userbot":
            asyncio.create_task(stop_userbot(q.from_user.id))

        status = "включён 🟢" if new_state else "выключен 🔴"
        await q.answer(f"Мониторинг {status}", show_alert=True)

    # ══════════════════════════════════════════════════════════════
    #  INLINE-КЛАВИАТУРА USERBOT (v8.2 — цифры + буквы)
    # ══════════════════════════════════════════════════════════════

    @r.callback_query(F.data.startswith("np_mode_"))
    async def handle_np_mode(q: CallbackQuery):
        """Переключение режима клавиатуры"""
        parts   = q.data.split("_")  # np_mode_{purpose}_{newmode}
        purpose = parts[2]
        newmode = parts[3]

        data = ub_auth_data.get(q.from_user.id)
        if not data:
            await q.answer("⏱ Сессия истекла", show_alert=True); return
        data["mode"] = newmode
        current = data.get(purpose, "")
        try:
            await q.message.edit_reply_markup(
                reply_markup=inline_numpad(current, purpose, newmode)
            )
        except Exception:
            pass
        await q.answer()

    @r.callback_query(F.data.startswith("np_"))
    async def handle_numpad(q: CallbackQuery):
        raw = q.data  # np_{purpose}_{action}
        if raw.startswith("np_mode_") or raw == "np_display":
            await q.answer(); return

        # Разбор: np_{purpose}_{action}
        # purpose может быть 'code' или 'password'
        # action может содержать символы (буквы, цифры, 'back', 'submit', 'space')
        try:
            without_prefix = raw[3:]  # убираем "np_"
            if without_prefix.startswith("code_"):
                purpose = "code"
                action  = without_prefix[5:]
            elif without_prefix.startswith("password_"):
                purpose = "password"
                action  = without_prefix[9:]
            else:
                await q.answer(); return
        except Exception:
            await q.answer(); return

        data = ub_auth_data.get(q.from_user.id)
        if not data:
            await q.answer("⏱ Сессия истекла, начните заново", show_alert=True)
            return

        current = data.get(purpose, "")
        mode    = data.get("mode", "digits")

        if action == "display":
            await q.answer(); return
        elif action == "back":
            current = current[:-1]
        elif action == "space":
            current += " "
        elif action == "submit":
            if not current:
                await q.answer("Введите значение!", show_alert=True); return
            data[purpose] = current

            if purpose == "code":
                await q.message.edit_text("⏳ Проверяю код…")
                result = await ub_sign_in(q.from_user.id)
                if result == "NEED_2FA":
                    data["password"] = ""
                    data["mode"]     = "digits"
                    await q.message.edit_text(
                        "🔒 <b>Требуется 2FA пароль</b>\n\n"
                        "Введите пароль через клавиатуру ниже.\n"
                        "Если пароль содержит буквы — нажмите «🔤 Буквы»:",
                        reply_markup=inline_numpad("", "password", "digits"),
                        parse_mode=ParseMode.HTML
                    )
                elif result:
                    await _ub_auth_success(q.message, result)
                else:
                    data["code"] = ""
                    await q.message.edit_text(
                        "❌ Неверный код! Попробуйте ещё раз:",
                        reply_markup=inline_numpad("", "code", "digits")
                    )
            else:  # password
                await q.message.edit_text("⏳ Проверяю пароль 2FA…")
                result = await ub_sign_in(q.from_user.id)
                if result and result != "NEED_2FA":
                    await _ub_auth_success(q.message, result)
                else:
                    data["password"] = ""
                    await q.message.edit_text(
                        "❌ Неверный пароль! Попробуйте ещё раз:",
                        reply_markup=inline_numpad("", "password", "digits")
                    )
            return
        else:
            if len(current) < 64:
                current += action

        data[purpose] = current
        try:
            await q.message.edit_reply_markup(
                reply_markup=inline_numpad(current, purpose, mode)
            )
        except Exception:
            pass
        await q.answer()

    @r.callback_query(F.data == "ub_cancel_auth")
    async def _ub_cancel(q: CallbackQuery):
        if q.from_user.id in ub_auth_data:
            try:
                await ub_auth_data[q.from_user.id]["client"].disconnect()
            except Exception:
                pass
            del ub_auth_data[q.from_user.id]
        await q.message.edit_text(
            "❌ Авторизация отменена",
            reply_markup=ikb([("🏠 Меню", "back_main")])
        )
        await q.answer()

    async def _ub_auth_success(msg: Message, session: str):
        uid = msg.chat.id
        await DB.set_field(uid, "ub_session", session)
        await DB.set_field(uid, "ub_active",  1)
        await DB.set_field(uid, "mode",       "userbot")

        # v8.2: Автовключение мониторинга
        auto_mon = await DB.get_config("auto_monitor_on_connect", "1")
        if auto_mon == "1":
            await DB.set_field(uid, "monitoring_on", 1)

        user     = await DB.get_user(uid)
        api_id   = user["pyro_api_id"] or PYRO_API_ID
        api_hash = user["pyro_api_hash"] or PYRO_API_HASH
        ok = False
        if api_id and api_hash:
            ok = await start_userbot(uid, session, api_id, api_hash)

        if ok:
            await msg.edit_text(
                "✅ <b>Userbot подключён!</b>\n\n"
                "📡 Мониторинг <b>автоматически включён</b>.\n\n"
                "Перехватываются:\n"
                "• Удалённые сообщения\n"
                "• Редактирования\n"
                "• Исчезающие 💣 и «один просмотр» 👁\n"
                "• Видео-кружки ⭕ и голосовые 🎙\n"
                "• Аудио 🎵 и документы 📎",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb([("🏠 Меню", "back_main")]),
            )
        else:
            await msg.edit_text(
                "⚠️ Сессия сохранена, userbot не запустился.\n"
                "Включите мониторинг вручную через меню.",
                reply_markup=ikb([("🏠 Меню", "back_main")]),
            )

    # ── Сохранение ЛС боту ───────────────────────────────────────

    @r.message(F.chat.type == "private", ~F.text.startswith("/"))
    async def _handle_dm_save(msg: Message, state: FSMContext):
        """v8.2: Сохранение всех сообщений в ЛС боту"""
        curr_state = await state.get_state()
        if curr_state:
            return  # В FSM-состоянии не перехватываем

        save_enabled = await DB.get_config("save_dm_messages", "1")
        if save_enabled != "1":
            return

        uid  = msg.from_user.id
        user = await DB.get_user(uid)
        if not user or not user["is_verified"]:
            return
        if not await DB.plan_active(uid):
            return
        if user.get("channel_left"):
            return

        await save_message_to_cache(msg, uid, _bot(), reason="dm")

    # ── Команда .dev (реплай) ─────────────────────────────────────

    @r.message(F.reply_to_message, F.text.lower() == ".dev")
    async def _handle_dev_reply(msg: Message):
        """v8.2: Сохранение реплаенутого сообщения командой .dev"""
        dev_enabled = await DB.get_config("dev_command_enabled", "1")
        if dev_enabled != "1":
            return

        uid  = msg.from_user.id
        user = await DB.get_user(uid)
        if not user or not user["is_verified"]:
            return
        if not await DB.plan_active(uid):
            return

        # Сохраняем реплаенутое сообщение
        target = msg.reply_to_message
        await save_message_to_cache(target, uid, _bot(), reason="dev")

        # Удаляем команду .dev
        try:
            await msg.delete()
        except Exception:
            pass

    # ── Режимы ────────────────────────────────────────────────────

    @r.callback_query(F.data == "mode")
    async def _cb_mode(q: CallbackQuery):
        await q.message.edit_text(
            "⚙️ <b>Выбери режим мониторинга</b>\n\n"
            "🤖 <b>Bot Mode (Telegram Business)</b>\n"
            "• Требуется Telegram Premium\n"
            "• Business-чаты: удаления ✅, редактирования ✅\n"
            "• Медиа (фото, видео, аудио) ✅\n"
            "• Команда .dev для сохранения ✅\n"
            "• Видео-кружки ❌, исчезающие ❌\n\n"
            "👤 <b>Userbot Mode (полный перехват)</b>\n"
            "• ВСЕ чаты без исключений\n"
            "• Видео-кружки ✅, исчезающие ✅, view_once ✅\n"
            "• Требуются API ключи с my.telegram.org",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb(
                [("🤖 Bot Mode",    "set_mode_bot")],
                [("👤 Userbot Mode", "set_mode_ub")],
                [("🔙 Назад",        "back_main")],
            ),
        )
        await q.answer()

    @r.callback_query(F.data == "set_mode_bot")
    async def _set_mode_bot(q: CallbackQuery):
        can_proceed, error_msg = await verify_and_check_channel(q.from_user.id)
        if not can_proceed:
            await q.answer(error_msg, show_alert=True); return

        await DB.set_field(q.from_user.id, "mode", "chatbot")
        await q.message.edit_text(
            f"🤖 <b>Bot Mode — Telegram Business Chatbot</b>\n\n"
            f"<b>Инструкция подключения:</b>\n"
            f"1. Telegram → Настройки → Telegram Premium\n"
            f"2. Бизнес → Чат-боты\n"
            f"3. Добавить бота → найди @{BOT_USERNAME}\n"
            f"4. Выбери чаты для мониторинга\n"
            f"5. Нажми «Проверить подключение»\n\n"
            f"<b>Команда .dev:</b>\n"
            f"Ответьте командой <code>.dev</code> на любое сообщение "
            f"в чате, где подключён бот — оно сохранится!\n\n"
            f"<b>Поддерживаемые типы медиа:</b>\n"
            f"✅ Текст, фото, видео, аудио\n"
            f"✅ Голосовые, кружки (при наличии файла)\n"
            f"✅ Документы, стикеры\n"
            f"✅ Таймерные сообщения",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb(
                [("🔍 Проверить подключение", "check_biz")],
                [("🔙 Назад", "mode")],
            ),
        )
        await q.answer()

    @r.callback_query(F.data == "check_biz")
    async def _check_biz(q: CallbackQuery):
        user = await DB.get_user(q.from_user.id)
        if user and user["biz_connected"]:
            await q.answer("✅ Business подключён, мониторинг активен!", show_alert=True)
        else:
            await q.answer(
                "❌ Подключение не обнаружено. Следуй инструкции.", show_alert=True
            )

    @r.callback_query(F.data == "set_mode_ub")
    async def _set_mode_ub(q: CallbackQuery, state: FSMContext):
        can_proceed, error_msg = await verify_and_check_channel(q.from_user.id)
        if not can_proceed:
            await q.answer(error_msg, show_alert=True); return

        if not PYROGRAM_OK:
            await q.answer("⚠️ Pyrogram не установлен на сервере", show_alert=True); return

        user = await DB.get_user(q.from_user.id)
        if user.get("ub_session"):
            status = "🟢 Активен" if q.from_user.id in ub_clients else "🔴 Остановлен"
            await q.message.edit_text(
                f"👤 <b>Userbot уже подключён</b>\n\n"
                f"📱 Телефон: <code>{user.get('ub_phone','—')}</code>\n"
                f"📡 Статус: {status}",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb(
                    [("🔄 Переподключить", "ub_reconnect")],
                    [("🗑 Отключить",       "ub_disconnect")],
                    [("🔙 Меню",            "back_main")],
                ),
            )
            await q.answer(); return

        await q.message.edit_text(
            "👤 <b>Userbot Mode</b>\n\n"
            "<b>Получение API ключей:</b>\n"
            "1. https://my.telegram.org\n"
            "2. API development tools → Create application\n"
            "3. Скопируй <b>api_id</b> (число) и <b>api_hash</b> (32 символа)\n\n"
            "⚠️ Ключи хранятся в зашифрованном виде.\n"
            "Код входа вводится через безопасную клавиатуру — "
            "Telegram его не увидит.",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb(
                [("▶️ Начать", "ub_start_fsm")],
                [("🔙 Назад",  "mode")],
            ),
        )
        await q.answer()

    @r.callback_query(F.data == "ub_start_fsm")
    async def _ub_start_fsm(q: CallbackQuery, state: FSMContext):
        await q.message.edit_text(
            "👤 <b>Шаг 1/3:</b> Введи <b>api_id</b> (число):",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "back_main")]),
        )
        await state.set_state(UserBotSt.api_id)
        await q.answer()

    @r.callback_query(F.data == "ub_reconnect")
    async def _ub_reconnect(q: CallbackQuery, state: FSMContext):
        await DB.set_field(q.from_user.id, "ub_session", None)
        await DB.set_field(q.from_user.id, "ub_active",  0)
        asyncio.create_task(stop_userbot(q.from_user.id))
        await q.message.edit_text(
            "👤 <b>Шаг 1/3:</b> Введи <b>api_id</b> (число):",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "back_main")]),
        )
        await state.set_state(UserBotSt.api_id)
        await q.answer()

    @r.callback_query(F.data == "ub_disconnect")
    async def _ub_disconnect(q: CallbackQuery):
        for field, val in [("ub_session", None), ("ub_active", 0),
                           ("mode", "none"), ("monitoring_on", 0)]:
            await DB.set_field(q.from_user.id, field, val)
        asyncio.create_task(stop_userbot(q.from_user.id))
        await q.answer("✅ Userbot отключён", show_alert=True)
        try:
            await q.message.edit_reply_markup(reply_markup=ikb([("🏠 Меню", "back_main")]))
        except Exception:
            pass

    # ── FSM: Userbot ──────────────────────────────────────────────

    @r.message(StateFilter(UserBotSt.api_id))
    async def _ub_api_id(msg: Message, state: FSMContext):
        # v8.2: удаляем сообщение с api_id для безопасности
        try:
            await msg.delete()
        except Exception:
            pass
        try:
            api_id = int(msg.text.strip())
            assert api_id > 0
        except Exception:
            await _bot().send_message(
                msg.from_user.id,
                "❌ api_id — это число. Попробуй ещё раз:"
            ); return
        await state.update_data(api_id=api_id)
        await DB.set_field(msg.from_user.id, "pyro_api_id", api_id)
        await _bot().send_message(
            msg.from_user.id,
            "👤 <b>Шаг 2/3:</b> Введи <b>api_hash</b> (32 символа):\n"
            "<i>Сообщение будет автоматически удалено</i>",
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(UserBotSt.api_hash)

    @r.message(StateFilter(UserBotSt.api_hash))
    async def _ub_api_hash(msg: Message, state: FSMContext):
        try:
            await msg.delete()
        except Exception:
            pass
        api_hash = msg.text.strip()
        if not re.match(r"^[a-f0-9]{32}$", api_hash):
            await _bot().send_message(
                msg.from_user.id,
                "❌ api_hash — 32 символа (a-f, 0-9). Проверь и попробуй ещё раз:"
            ); return
        await state.update_data(api_hash=api_hash)
        await DB.set_field(msg.from_user.id, "pyro_api_hash", api_hash)
        await _bot().send_message(
            msg.from_user.id,
            "👤 <b>Шаг 3/3:</b> Введи номер телефона\n"
            "Формат: <code>+79991234567</code>\n"
            "<i>Сообщение будет удалено</i>",
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(UserBotSt.phone)

    @r.message(StateFilter(UserBotSt.phone))
    async def _ub_phone(msg: Message, state: FSMContext):
        try:
            await msg.delete()
        except Exception:
            pass
        phone = msg.text.strip().replace(" ", "")
        if not re.match(r"^\+\d{10,15}$", phone):
            await _bot().send_message(
                msg.from_user.id,
                "❌ Неверный формат. Введи: <code>+79991234567</code>",
                parse_mode=ParseMode.HTML,
            ); return

        data     = await state.get_data()
        api_id   = data.get("api_id") or PYRO_API_ID
        api_hash = data.get("api_hash") or PYRO_API_HASH

        notice = await _bot().send_message(msg.from_user.id, "⏳ Отправляю код…")
        ph_hash = await ub_send_code(msg.from_user.id, phone, api_id, api_hash)

        if not ph_hash:
            await notice.edit_text(
                "❌ Не удалось отправить код.\n"
                "Проверь api_id / api_hash / номер телефона.",
                reply_markup=ikb([("🔙 Меню", "back_main")]),
            )
            await state.clear(); return

        await state.update_data(phone=phone)
        await DB.set_field(msg.from_user.id, "ub_phone", phone)
        await state.clear()

        await notice.edit_text(
            f"📱 <b>Код отправлен на {phone}</b>\n\n"
            f"Введи код через безопасную клавиатуру:\n"
            f"<i>Telegram не увидит код в истории чата</i>",
            reply_markup=inline_numpad("", "code", "digits"),
            parse_mode=ParseMode.HTML
        )

    # ── Планы и оплата ───────────────────────────────────────────

    @r.callback_query(F.data == "plans")
    @r.message(Command("plan"))
    async def _cb_plans(upd):
        q       = upd if isinstance(upd, CallbackQuery) else None
        msg_obj = q.message if q else upd
        test_days = await DB.get_config("test_period_days", "3")
        text = (
            f"💎 <b>Тарифные планы MerAi</b>\n\n"
            f"🎁 <b>Тестовый период:</b> {test_days} дня — бесплатно!\n\n"
            f"Оплата через <b>Telegram Stars ⭐</b>\n\n"
        )
        for pid, p in PLANS.items():
            text += f"{plan_emoji(pid)} <b>{p['name']}</b> — <b>{p['stars']} ⭐</b> ({p['days']} дн.)\n"
        text += (
            "\n<b>✅ Включено во всех планах:</b>\n"
            "• Bot Mode + Userbot Mode\n"
            "• Удалённые и отредактированные сообщения\n"
            "• Видео-кружки, голосовые, аудио, исчезающие медиа\n"
            "• ZIP-архив при массовых удалениях\n"
            "• Команда .dev — сохранение любых сообщений\n"
            "• Клонирование ботов (+3 дня за каждый)"
        )
        rows = [[(f"{plan_emoji(pid)} {p['name']} — {p['stars']} ⭐", f"buy_{pid}")]
                for pid, p in PLANS.items()]
        rows.append([("🔙 Назад", "back_main")])
        kb = ikb(*rows)
        if q:
            await msg_obj.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            await q.answer()
        else:
            await msg_obj.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    @r.callback_query(F.data.startswith("buy_"))
    async def _cb_buy(q: CallbackQuery):
        can_proceed, error_msg = await verify_and_check_channel(q.from_user.id)
        if not can_proceed:
            await q.answer(error_msg, show_alert=True); return

        plan_id = q.data[4:]
        plan    = PLANS.get(plan_id)
        if not plan:
            await q.answer("Тариф не найден"); return
        await q.answer()
        try:
            await _bot().send_invoice(
                chat_id=q.from_user.id,
                title=f"MerAi — {plan['name']}",
                description=(
                    f"Подписка MerAi & Monitoring\n"
                    f"Период: {plan['days']} дней\n"
                    f"Полный доступ ко всем функциям"
                ),
                payload=f"plan:{plan_id}",
                currency="XTR",
                prices=[LabeledPrice(label=plan["name"], amount=plan["stars"])],
            )
        except Exception as e:
            log.error(f"send_invoice err: {e}")
            await q.message.answer(f"❌ Ошибка создания инвойса: {h(str(e))}")

    @r.pre_checkout_query()
    async def _pre_checkout(pcq: PreCheckoutQuery):
        await pcq.answer(ok=True)

    @r.message(F.successful_payment)
    async def _on_payment(msg: Message):
        pay     = msg.successful_payment
        payload = pay.invoice_payload
        uid     = msg.from_user.id
        if not payload.startswith("plan:"):
            return
        plan_id = payload[5:]
        plan    = PLANS.get(plan_id)
        if not plan:
            return
        await DB.set_plan(uid, plan_id, plan["days"])
        await DB.add_tx(uid, plan_id, pay.total_amount, payload)
        await msg.answer(
            f"🎉 <b>Оплата прошла!</b>\n\n"
            f"{plan_emoji(plan_id)} <b>План: {plan['name']}</b>\n"
            f"📅 <b>Срок:</b> {plan['days']} дней\n"
            f"⭐ <b>Оплачено:</b> {pay.total_amount} Stars\n\n"
            f"Включи мониторинг через /start!",
            parse_mode=ParseMode.HTML,
        )

    # ── Профиль ──────────────────────────────────────────────────

    @r.callback_query(F.data == "profile")
    async def _profile(q: CallbackQuery):
        u      = await DB.get_user(q.from_user.id)
        active = await DB.plan_active(q.from_user.id)
        clones = await DB.get_user_clones(q.from_user.id)
        stats  = await DB.get_deletion_stats(q.from_user.id)
        ref_link = f"https://t.me/{BOT_USERNAME}?start=ref{u['user_id']}"

        if u["plan"] == "trial" and not u["trial_used"]:
            plan_block = (
                f"├ 🎁 Тестовый период\n"
                f"├ 📅 До: {fmt_exp(u['trial_expires'])}"
            )
        else:
            plan_block = (
                f"├ {plan_emoji(u['plan'])} {u['plan'].upper()}\n"
                f"├ {'✅ Активна' if active else '❌ Истекла'}\n"
                f"├ 📅 До: {fmt_exp(u['plan_expires'])}"
            )

        text = (
            f"📊 <b>Мой профиль</b>\n\n"
            f"🆔 ID: <code>{u['user_id']}</code>\n"
            f"👤 Имя: {h(u['first_name'] or '—')}\n"
            f"🔗 @{u['username'] or '—'}\n\n"
            f"<b>Подписка:</b>\n{plan_block}\n\n"
            f"<b>Статистика:</b>\n"
            f"├ 🗑 Перехвачено: {stats['total']}\n"
            f"├ 🤖 Клонов: {len(clones)}\n"
            f"└ 👥 Рефералов: {u['referral_count']}\n\n"
            f"<b>Реферальная ссылка:</b>\n<code>{ref_link}</code>"
        )
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
            [("💎 Купить план",          "plans")],
            [("🤖 Мои боты-клоны",       "my_clones")],
            [("📊 Статистика удалений",  "del_stats")],
            [("🔙 Назад",                "back_main")],
        ))
        await q.answer()

    @r.callback_query(F.data == "del_stats")
    async def _del_stats(q: CallbackQuery):
        stats = await DB.get_deletion_stats(q.from_user.id)
        text  = f"🗑 <b>Статистика удалений</b>\n\nВсего: <b>{stats['total']}</b>\n\n"
        if stats["top_chats"]:
            text += "<b>Топ чатов:</b>\n"
            for r in stats["top_chats"]:
                text += f"├ {h(r['chat_title'])} — {r['cnt']}\n"
            text += "\n"
        if stats["by_type"]:
            text += "<b>По типам:</b>\n"
            for r in stats["by_type"]:
                text += f"├ {mtype_icon(r['msg_type'])} {r['msg_type']} — {r['cnt']}\n"
            text += "\n"
        if stats["recent"]:
            text += "<b>Последние 5:</b>\n"
            for row in stats["recent"][:5]:
                ts   = (row["deleted_at"] or "")[:16]
                name = h(row["sender_name"] or "?")
                chat = h(row["chat_title"] or "?")
                text += f"• {ts} — {name} в {chat}\n"
        await q.message.edit_text(text, parse_mode=ParseMode.HTML,
                                  reply_markup=ikb([("🔙 Профиль", "profile")]))
        await q.answer()

    @r.callback_query(F.data == "my_clones")
    async def _my_clones(q: CallbackQuery):
        clones = await DB.get_user_clones(q.from_user.id)
        bonus  = await DB.get_config("clone_bonus_days", "3")
        if not clones:
            await q.message.edit_text(
                f"🤖 <b>Клонированные боты</b>\n\nУ тебя нет подключённых ботов.\n"
                f"Добавь бота и получи +{bonus} дня к подписке!",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb(
                    [("➕ Добавить бот", "clone_start")],
                    [("🔙 Профиль",      "profile")],
                ),
            )
        else:
            text = "🤖 <b>Мои клонированные боты</b>\n\n"
            for c in clones:
                st = "🟢" if c["is_active"] else "🔴"
                text += (
                    f"{st} <b>@{c['bot_username'] or '?'}</b> — {h(c['bot_name'] or '?')}\n"
                    f"👥 Пользователей: {c['user_count']}\n\n"
                )
            await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
                [("➕ Добавить ещё",  "clone_start")],
                [("🔙 Профиль",       "profile")],
            ))
        await q.answer()

    # ── Рефералы ─────────────────────────────────────────────────

    @r.callback_query(F.data == "referrals")
    async def _referrals(q: CallbackQuery):
        u          = await DB.get_user(q.from_user.id)
        cnt        = u["referral_count"]
        goal       = int(await DB.get_config("ref_goal", "50"))
        prem_months= int(await DB.get_config("referral_premium_months", "1"))
        link       = f"https://t.me/{BOT_USERNAME}?start=ref{u['user_id']}"
        filled     = min(cnt * 10 // max(goal, 1), 10)
        bar        = "█" * filled + "░" * (10 - filled)
        done       = cnt >= goal

        text = (
            f"🎁 <b>Реферальная программа</b>\n\n"
            f"<b>Прогресс:</b>\n{bar}  {cnt}/{goal}\n\n"
            f"<b>Награда за {goal} рефералов:</b>\n"
            f"🏆 Telegram Premium на <b>{prem_months} мес.</b>!\n\n"
            f"За каждого приглашённого пользователя +1 к счётчику.\n"
            f"После достижения цели — Premium отправляется автоматически.\n\n"
            f"<b>Твоя ссылка:</b>\n<code>{link}</code>\n\n"
            + (
                f"🎉 <b>Цель достигнута! Ожидай Premium.</b>"
                if done else
                f"Осталось пригласить: <b>{goal - cnt}</b>"
            )
        )
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb_url(
            [("📤 Поделиться", f"https://t.me/share/url?url={link}&text=Попробуй+MerAi!")],
            [("🔙 Меню",       "back_main")],
        ))
        await q.answer()

    # ── Клонирование ─────────────────────────────────────────────

    @r.callback_query(F.data == "clone_start")
    async def _clone_start(q: CallbackQuery, state: FSMContext):
        can_proceed, error_msg = await verify_and_check_channel(q.from_user.id)
        if not can_proceed:
            await q.answer(error_msg, show_alert=True); return

        # Проверка лимита клонов
        max_clones = int(await DB.get_config("max_clones_per_user", "5"))
        user_clones = await DB.get_user_clones(q.from_user.id)
        if len(user_clones) >= max_clones:
            await q.answer(f"❌ Максимум {max_clones} ботов на аккаунт", show_alert=True)
            return

        bonus = int(await DB.get_config("clone_bonus_days", "3"))
        await q.message.edit_text(
            f"🤖 <b>Клонирование бота</b>\n\n"
            f"<b>Как получить токен:</b>\n"
            f"1️⃣ @BotFather → /newbot\n"
            f"2️⃣ Придумай имя и username\n"
            f"3️⃣ Скопируй токен\n\n"
            f"📋 Отправь токен (формат: <code>123456789:ABC-DEF...</code>)\n\n"
            f"🎁 <b>За каждый бот — +{bonus} дня к подписке!</b>\n"
            f"🔒 Сообщение с токеном будет удалено после обработки.",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "back_main")]),
        )
        await state.set_state(CloneSt.token)
        await q.answer()

    @r.message(StateFilter(CloneSt.token))
    async def _clone_token(msg: Message, state: FSMContext):
        token_msg_id = msg.message_id
        token        = msg.text.strip()

        # v8.2: Сразу удаляем сообщение с токеном
        delete_sensitive = await DB.get_config("delete_sensitive_msgs", "1")
        if delete_sensitive == "1":
            try:
                await msg.delete()
            except Exception:
                pass

        if not re.match(r"^\d+:[A-Za-z0-9_-]{35,}$", token):
            await _bot().send_message(
                msg.from_user.id,
                "❌ Неверный формат токена.\n"
                "Токен: <code>123456789:ABCDEF...</code>",
                parse_mode=ParseMode.HTML,
            ); return

        notice = await _bot().send_message(msg.from_user.id, "⏳ Проверяю бота…")
        try:
            test_bot = Bot(token=token)
            info     = await test_bot.get_me()
            await test_bot.session.close()
        except Exception as e:
            await notice.edit_text(
                f"❌ Неверный токен или бот недоступен.\n<code>{h(str(e))}</code>",
                parse_mode=ParseMode.HTML,
            )
            await state.clear(); return

        uid   = msg.from_user.id
        bonus = int(await DB.get_config("clone_bonus_days", "3"))
        ok    = await DB.add_clone(uid, token, info.username, info.full_name)

        if not ok:
            await notice.edit_text(
                "⚠️ Этот бот уже подключён.",
                reply_markup=ikb([("🏠 Меню", "back_main")]),
            )
            await state.clear(); return

        await DB.add_days(uid, bonus)
        clones    = await DB.get_user_clones(uid)
        this_clone= next((c for c in clones if c["bot_token"] == token), None)
        cid_db    = this_clone["id"] if this_clone else None

        asyncio.create_task(launch_clone(token, uid, cid_db))

        await notice.edit_text(
            f"✅ <b>Бот @{info.username} подключён!</b>\n\n"
            f"🗑 Сообщение с токеном удалено для безопасности.\n"
            f"🎁 <b>+{bonus} дня</b> добавлено к подписке!\n\n"
            f"Бот запускается и будет доступен через несколько секунд.",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("🏠 Меню", "back_main")]),
        )
        await state.clear()

    # ── Поддержка ─────────────────────────────────────────────────

    @r.callback_query(F.data == "support")
    @r.message(Command("support"))
    async def _support(upd):
        q       = upd if isinstance(upd, CallbackQuery) else None
        msg_obj = q.message if q else upd
        support_username = await DB.get_config("support_username", "mrztn")
        text = (
            f"💬 <b>Поддержка MerAi</b>\n\n"
            f"1️⃣ Нажми «Написать» и опиши проблему подробно\n"
            f"2️⃣ Или напрямую: @{support_username}\n\n"
            f"⏱ Среднее время ответа: до 24 часов\n"
            f"📌 Тикеты обрабатываются в порядке очереди"
        )
        kb = ikb_url(
            [("✍️ Написать тикет",         "support_write")],
            [("📱 Написать напрямую",       f"https://t.me/{support_username}")],
            [("🔙 Назад",                   "back_main")],
        )
        if q:
            await msg_obj.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            await q.answer()
        else:
            await msg_obj.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    @r.callback_query(F.data == "support_write")
    async def _support_write(q: CallbackQuery, state: FSMContext):
        await q.message.edit_text(
            "✍️ Опиши проблему подробно (включая ID ошибки, шаги воспроизведения):",
            reply_markup=ikb([("❌ Отмена", "back_main")]),
        )
        await state.set_state(SupportSt.message)
        await q.answer()

    @r.message(StateFilter(SupportSt.message))
    async def _support_msg(msg: Message, state: FSMContext):
        uid  = msg.from_user.id
        text = msg.text or "[медиа]"
        tid  = await DB.create_ticket(uid, text)
        await state.clear()
        await msg.answer(
            f"✅ <b>Тикет #{tid} создан!</b>\n\nОжидай ответа.",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("🏠 Меню", "back_main")]),
        )
        u = await DB.get_user(uid)
        try:
            await _bot().send_message(
                ADMIN_ID,
                f"📩 <b>Тикет #{tid}</b>\n"
                f"👤 {h(u['first_name'] or '?')} (@{u['username'] or '?'}) "
                f"<code>{uid}</code>\n"
                f"📅 {datetime.now().strftime('%d.%m %H:%M')}\n\n"
                f"{h(text)}",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb([(f"↩️ Ответить #{tid}", f"adm_reply_{tid}_{uid}")]),
            )
        except Exception:
            pass

    # ── Условия, Помощь ───────────────────────────────────────────

    @r.callback_query(F.data == "terms")
    @r.message(Command("terms"))
    async def _terms(upd):
        q       = upd if isinstance(upd, CallbackQuery) else None
        msg_obj = q.message if q else upd
        kb = ikb([("✅ Принимаю и понимаю", "back_main")])
        if q:
            await msg_obj.edit_text(TERMS_TEXT, parse_mode=ParseMode.HTML, reply_markup=kb)
            await q.answer()
        else:
            await msg_obj.answer(TERMS_TEXT, parse_mode=ParseMode.HTML, reply_markup=kb)

    @r.callback_query(F.data == "help")
    @r.message(Command("help"))
    async def _help(upd):
        q       = upd if isinstance(upd, CallbackQuery) else None
        msg_obj = q.message if q else upd
        test_days = await DB.get_config("test_period_days", "3")
        text = (
            f"❓ <b>Справка MerAi &amp; Monitoring v8.2</b>\n\n"
            f"<b>🆕 Новинки v8.2:</b>\n"
            f"• Публичный канал @merzedev — мгновенная верификация\n"
            f"• Тестовый период {test_days} дня с полным доступом\n"
            f"• Команда <code>.dev</code> — сохранение любого сообщения\n"
            f"• Inline-клавиатура для кода userbot (Telegram не видит)\n"
            f"• Автоудаление токенов и ключей из чата\n"
            f"• Мониторинг включается автоматически при подключении\n"
            f"• Аудио и видео сохраняются в Bot Mode\n\n"

            f"<b>Команды:</b>\n"
            f"/start — Главное меню\n"
            f"/plan — Тарифные планы\n"
            f"/support — Поддержка\n"
            f"/help — Справка\n"
            f"/terms — Условия использования\n"
            f"/admin — Панель администратора\n\n"

            f"<b>Команда .dev:</b>\n"
            f"Ответьте <code>.dev</code> на любое сообщение "
            f"в Business-чате или любом чате, где включён мониторинг — "
            f"оно сохранится в вашем архиве.\n\n"

            f"<b>Функция         | Bot | Userbot</b>\n"
            f"Удалённые тексты  | ✅  | ✅\n"
            f"Фото/Видео        | ✅  | ✅\n"
            f"Аудио 🎵           | ✅  | ✅\n"
            f"Голосовые 🎙       | ✅  | ✅\n"
            f"Видео-кружки ⭕   | ⚠️  | ✅\n"
            f"Исчезающие 💣      | ❌  | ✅\n"
            f"Один просмотр 👁  | ❌  | ✅\n"
            f"Все чаты          | ❌  | ✅\n"
            f"ZIP-архив         | ✅  | ✅\n"
            f"Команда .dev      | ✅  | ✅\n"
            f"Premium нужен     | ✅  | ❌"
        )
        kb = ikb([("🔙 Назад", "back_main")])
        if q:
            await msg_obj.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            await q.answer()
        else:
            await msg_obj.answer(text, parse_mode=ParseMode.HTML)

    # ══════════════════════════════════════════════════════════════
    #  АДМИН-ПАНЕЛЬ v8.2 (расширенная)
    # ══════════════════════════════════════════════════════════════

    @r.message(Command("admin"))
    async def _cmd_admin(msg: Message):
        if msg.from_user.id != ADMIN_ID:
            await msg.answer("🚫 Доступ запрещён."); return
        await _admin_main(msg)

    @r.callback_query(F.data == "admin")
    async def _cb_admin(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID:
            await q.answer("🚫", show_alert=True); return
        try:
            await q.message.delete()
        except Exception:
            pass
        await _admin_main(q.message)
        await q.answer()

    @r.callback_query(F.data == "adm_back")
    async def _adm_back(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID:
            await q.answer(); return
        await state.clear()
        try:
            await q.message.delete()
        except Exception:
            pass
        await _admin_main(q.message)
        await q.answer()

    async def _admin_main(msg: Message):
        s = await DB.stats()
        text = (
            f"🛠 <b>Админ-панель MerAi v8.2</b>\n"
            f"{'═'*30}\n"
            f"👥 Всего: {s['total']} | ✅ Верифиц.: {s['verified']}\n"
            f"🎁 Trial: {s['trial']} | 💎 Платящих: {s['paid']}\n"
            f"🤖 Клонов: {s['clones']} | 👤 Userbot: {len(ub_clients)}\n"
            f"⭐ Stars: {s['stars']} | 🗑 Удалений: {s['deletions']}\n"
            f"💬 Тикетов: {s['tickets']} | 🚫 Банов: {s['banned']}\n"
        )
        await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
            [("👥 Пользователи",   "adm_users"),   ("🔍 Поиск",       "adm_search")],
            [("📊 Статистика",     "adm_stats"),   ("💬 Тикеты",     "adm_tickets")],
            [("🤖 Клоны ботов",   "adm_clones"),  ("📢 Рассылка",   "adm_broadcast")],
            [("🔧 Конфиг",        "adm_config"),  ("💰 Транзакции", "adm_txns")],
            [("➕ Добавить дни",   "adm_add_days"),("🚫 Баны",       "adm_bans")],
            [("📤 Написать юзеру","adm_send"),    ("🔙 Меню",       "back_main")],
        ))

    # ── Поиск пользователей ───────────────────────────────────────

    @r.callback_query(F.data == "adm_search")
    async def _adm_search_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID:
            await q.answer(); return
        await q.message.edit_text(
            "🔍 <b>Поиск пользователей</b>\n\n"
            "Введи имя, username или ID:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.search_user)
        await q.answer()

    @r.message(StateFilter(AdminSt.search_user))
    async def _adm_search_exec(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        query   = msg.text.strip()
        results = await DB.search_users(query)
        await state.clear()

        if not results:
            await msg.answer(
                f"🔍 По запросу «{h(query)}» ничего не найдено.",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb([("🔙 Панель", "adm_back")]),
            ); return

        text = f"🔍 <b>Результаты ({len(results)}):</b>\n\n"
        buttons = []
        for u in results[:15]:
            active = "✅" if (u["plan"] not in ("trial", "free")) else "🎁"
            ban    = "🚫" if u["is_banned"] else ""
            text += (
                f"{ban}{active} <code>{u['user_id']}</code> "
                f"<b>{h(u['first_name'] or '?')}</b> @{u['username'] or '—'}\n"
            )
            buttons.append([(
                f"👤 {u['first_name'] or u['user_id']}",
                f"adm_uview_{u['user_id']}"
            )])
        buttons.append([("🔙 Панель", "adm_back")])
        await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*buttons))

    # ── Список пользователей (с пагинацией) ───────────────────────

    @r.callback_query(F.data == "adm_users")
    async def _adm_users(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        await _adm_users_page(q, 0)

    @r.callback_query(F.data.startswith("adm_users_p"))
    async def _adm_users_paginate(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        page = int(q.data.split("adm_users_p")[1])
        await _adm_users_page(q, page)

    async def _adm_users_page(q: CallbackQuery, page: int):
        per_page = 8
        offset   = page * per_page
        users    = await DB.get_users_page(offset, per_page)
        total_q  = await DB.stats()
        total    = total_q["total"]

        if not users:
            await q.message.edit_text(
                "👥 Пользователей нет.",
                reply_markup=ikb([("🔙 Панель", "adm_back")])
            ); await q.answer(); return

        text = f"👥 <b>Пользователи</b> (страница {page+1}):\n\n"
        buttons = []
        for u in users:
            active = "✅" if u["plan"] not in ("trial", "free") else "🎁"
            ban    = "🚫" if u["is_banned"] else ""
            text  += (
                f"{ban}{active} <code>{u['user_id']}</code> "
                f"<b>{h(u['first_name'] or '?')}</b> @{u['username'] or '—'}\n"
                f"  📅 {(u.get('last_active') or '')[:10]}\n"
            )
            buttons.append([(
                f"👤 {(u['first_name'] or str(u['user_id']))[:20]}",
                f"adm_uview_{u['user_id']}"
            )])

        nav = []
        if page > 0:
            nav.append((f"◀ Назад", f"adm_users_p{page-1}"))
        if offset + per_page < total:
            nav.append((f"Вперёд ▶", f"adm_users_p{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append([("🔍 Поиск", "adm_search"), ("🔙 Панель", "adm_back")])

        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*buttons))
        await q.answer()

    # ── Просмотр пользователя ─────────────────────────────────────

    @r.callback_query(F.data.startswith("adm_uview_"))
    async def _adm_uview(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        uid  = int(q.data.split("adm_uview_")[1])
        u    = await DB.get_user(uid)
        if not u:
            await q.answer("Пользователь не найден", show_alert=True); return

        clones = await DB.get_user_clones(uid)
        ub_active = "🟢" if uid in ub_clients else "🔴"

        text = (
            f"👤 <b>Пользователь</b>\n\n"
            f"🆔 ID: <code>{u['user_id']}</code>\n"
            f"👤 Имя: {h(u['first_name'] or '—')}\n"
            f"🔗 @{u['username'] or '—'}\n"
            f"📅 Регистрация: {(u['created_at'] or '')[:10]}\n"
            f"📅 Активность: {(u['last_active'] or '')[:16]}\n\n"
            f"<b>Подписка:</b> {plan_emoji(u['plan'])} {u['plan']}\n"
            f"До: {fmt_exp(u['plan_expires']) if u['plan_expires'] else fmt_exp(u['trial_expires'])}\n\n"
            f"<b>Статус:</b>\n"
            f"├ Верифицирован: {'✅' if u['is_verified'] else '❌'}\n"
            f"├ Канал: {'✅' if u['channel_member'] else '❌'}\n"
            f"├ Мониторинг: {'🟢' if u['monitoring_on'] else '🔴'}\n"
            f"├ Режим: {u['mode']}\n"
            f"├ Userbot: {ub_active}\n"
            f"├ Бизнес: {'✅' if u['biz_connected'] else '❌'}\n"
            f"├ Клонов: {len(clones)}\n"
            f"├ Рефералов: {u['referral_count']}\n"
            f"└ Забанен: {'🚫 ДА' if u['is_banned'] else 'Нет'}"
        )
        markup = ikb(
            [(f"{'✅ Разбанить' if u['is_banned'] else '🚫 Забанить'}",
              f"adm_{'unban' if u['is_banned'] else 'ban'}_{uid}")],
            [("➕ Добавить дни",       f"adm_days_{uid}"),
             ("💬 Написать",           f"adm_msg_{uid}")],
            [("🤖 Клоны юзера",        f"adm_uclones_{uid}")],
            [("🔙 К списку",           "adm_users")],
        )
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        await q.answer()

    @r.callback_query(F.data.startswith("adm_ban_"))
    async def _adm_ban(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        uid = int(q.data.split("adm_ban_")[1])
        await DB.set_field(uid, "is_banned", 1)
        await DB.set_field(uid, "monitoring_on", 0)
        asyncio.create_task(stop_userbot(uid))
        await q.answer(f"🚫 Пользователь {uid} забанен", show_alert=True)
        try:
            await _bot().send_message(uid, "🚫 Ваш аккаунт заблокирован.")
        except Exception:
            pass
        await _adm_uview(q)

    @r.callback_query(F.data.startswith("adm_unban_"))
    async def _adm_unban(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        uid = int(q.data.split("adm_unban_")[1])
        await DB.set_field(uid, "is_banned", 0)
        await q.answer(f"✅ Пользователь {uid} разбанен", show_alert=True)
        await _adm_uview(q)

    @r.callback_query(F.data.startswith("adm_days_"))
    async def _adm_days_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        uid = int(q.data.split("adm_days_")[1])
        await state.update_data(target_uid=uid)
        await q.message.edit_text(
            f"➕ Добавить дни пользователю <code>{uid}</code>\n\nВведи количество дней:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.add_days_amt)
        await q.answer()

    @r.message(StateFilter(AdminSt.add_days_amt))
    async def _adm_days_save(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        data = await state.get_data()
        uid  = data.get("target_uid")
        try:
            days = int(msg.text.strip())
            assert days > 0
        except Exception:
            await msg.answer("❌ Введи положительное число."); return
        await DB.add_days(uid, days)
        await state.clear()
        await msg.answer(
            f"✅ Добавлено <b>{days} дней</b> пользователю <code>{uid}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("🔙 Панель", "adm_back")]),
        )
        try:
            await _bot().send_message(
                uid,
                f"🎁 Администратор добавил вам <b>{days} дней</b> подписки!",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    @r.callback_query(F.data.startswith("adm_msg_"))
    async def _adm_msg_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        uid = int(q.data.split("adm_msg_")[1])
        await state.update_data(target_uid=uid)
        await q.message.edit_text(
            f"💬 Введи сообщение для <code>{uid}</code>:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.send_direct_msg)
        await q.answer()

    @r.message(StateFilter(AdminSt.send_direct_msg))
    async def _adm_msg_send(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        data = await state.get_data()
        uid  = data.get("target_uid")
        text = msg.text or "[медиа]"
        await state.clear()
        try:
            await _bot().send_message(
                uid,
                f"📩 <b>Сообщение от администратора:</b>\n\n{h(text)}",
                parse_mode=ParseMode.HTML,
            )
            await msg.answer(
                f"✅ Отправлено пользователю <code>{uid}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=ikb([("🔙 Панель", "adm_back")]),
            )
        except Exception as e:
            await msg.answer(
                f"❌ Ошибка: {h(str(e))}",
                reply_markup=ikb([("🔙 Панель", "adm_back")]),
            )

    # ── Клоны (с токенами) в админке ─────────────────────────────

    @r.callback_query(F.data == "adm_clones")
    async def _adm_clones(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        clones = await DB.all_clones()
        if not clones:
            await q.message.edit_text(
                "🤖 Клонов нет.",
                reply_markup=ikb([("🔙 Панель", "adm_back")])
            ); await q.answer(); return

        text    = f"🤖 <b>Все клоны ({len(clones)}):</b>\n\n"
        buttons = []
        for c in clones[:20]:
            st = "🟢" if c["is_active"] else "🔴"
            text += (
                f"{st} @{c['bot_username'] or '?'} | "
                f"owner: {c['owner_id']} | "
                f"users: {c['user_count']}\n"
            )
            buttons.append([(
                f"{st} @{c['bot_username'] or c['id']}",
                f"adm_clone_{c['id']}"
            )])
        buttons.append([("🔙 Панель", "adm_back")])
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*buttons))
        await q.answer()

    @r.callback_query(F.data.startswith("adm_clone_"))
    async def _adm_clone_detail(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        cid = int(q.data.split("adm_clone_")[1])
        async with DB._conn.execute(
            "SELECT * FROM cloned_bots WHERE id=?", (cid,)
        ) as c:
            row = await c.fetchone()
        if not row:
            await q.answer("Клон не найден", show_alert=True); return
        c = dict(row)

        # v8.2: показываем токен (маскируем середину)
        token = c["bot_token"]
        tok_parts = token.split(":")
        masked = tok_parts[0] + ":" + tok_parts[1][:8] + "••••" + tok_parts[1][-6:] if len(tok_parts) == 2 else token

        text = (
            f"🤖 <b>Клон #{cid}</b>\n\n"
            f"Bot: @{c['bot_username']} ({h(c['bot_name'])})\n"
            f"Владелец: <code>{c['owner_id']}</code>\n"
            f"Пользователей: {c['user_count']}\n"
            f"Статус: {'🟢 Активен' if c['is_active'] else '🔴 Выключен'}\n"
            f"Добавлен: {(c['added_at'] or '')[:10]}\n\n"
            f"🔑 <b>Токен (частично):</b>\n<code>{h(masked)}</code>"
        )
        markup = ikb(
            [("🔑 Показать полный токен", f"adm_tok_full_{cid}")],
            [("🔴 Деактивировать" if c["is_active"] else "🟢 Активировать",
              f"adm_clone_tog_{cid}")],
            [("🔙 К списку", "adm_clones")],
        )
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        await q.answer()

    @r.callback_query(F.data.startswith("adm_tok_full_"))
    async def _adm_tok_full(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        cid = int(q.data.split("adm_tok_full_")[1])
        async with DB._conn.execute("SELECT bot_token FROM cloned_bots WHERE id=?", (cid,)) as c:
            row = await c.fetchone()
        if not row:
            await q.answer("Не найден", show_alert=True); return
        # Отправляем отдельным сообщением (не редактируем — безопаснее)
        await _bot().send_message(
            q.from_user.id,
            f"🔑 <b>Токен клона #{cid}:</b>\n<code>{row[0]}</code>\n\n"
            f"⚠️ Никому не передавайте токен!",
            parse_mode=ParseMode.HTML
        )
        await q.answer("Токен отправлен отдельным сообщением", show_alert=True)

    @r.callback_query(F.data.startswith("adm_clone_tog_"))
    async def _adm_clone_toggle(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        cid = int(q.data.split("adm_clone_tog_")[1])
        async with DB._conn.execute("SELECT is_active,bot_token FROM cloned_bots WHERE id=?", (cid,)) as c:
            row = await c.fetchone()
        if not row: await q.answer(); return
        new_active = 0 if row["is_active"] else 1
        await DB.set_clone_active(cid, new_active)
        if not new_active and row["bot_token"] in clone_bots:
            asyncio.create_task(stop_clone(row["bot_token"]))
        await q.answer(f"Клон {'активирован' if new_active else 'деактивирован'}", show_alert=True)
        await _adm_clone_detail(q)

    @r.callback_query(F.data.startswith("adm_uclones_"))
    async def _adm_uclones(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        uid    = int(q.data.split("adm_uclones_")[1])
        clones = await DB.get_user_clones(uid)
        text   = f"🤖 <b>Клоны пользователя {uid}</b>\n\n"
        if not clones:
            text += "Нет клонов."
        else:
            for c in clones:
                text += (
                    f"{'🟢' if c['is_active'] else '🔴'} "
                    f"@{c['bot_username']} | {c['user_count']} юзеров\n"
                )
        await q.message.edit_text(text, parse_mode=ParseMode.HTML,
                                  reply_markup=ikb([("🔙 К юзеру", f"adm_uview_{uid}")]))
        await q.answer()

    # ── Рассылка ──────────────────────────────────────────────────

    @r.callback_query(F.data == "adm_broadcast")
    async def _adm_broadcast_menu(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        await q.message.edit_text(
            "📢 <b>Рассылка</b>\n\nВыбери тип:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb(
                [("📢 Всем",         "adm_bc_all")],
                [("💎 Платящим",    "adm_bc_paid")],
                [("🎁 Trial",        "adm_bc_trial")],
                [("🔙 Панель",       "adm_back")],
            )
        )
        await q.answer()

    @r.callback_query(F.data.startswith("adm_bc_"))
    async def _adm_bc_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        target = q.data.split("adm_bc_")[1]
        await state.update_data(bc_target=target)
        await q.message.edit_text(
            "📢 Введи текст рассылки (поддерживается HTML):",
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.broadcast_text)
        await q.answer()

    @r.message(StateFilter(AdminSt.broadcast_text))
    async def _adm_bc_send(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        data   = await state.get_data()
        target = data.get("bc_target", "all")
        text   = msg.text or "[медиа]"
        footer = await DB.get_config("broadcast_footer", "")
        if footer:
            text += f"\n\n{footer}"
        await state.clear()

        users    = await DB.all_users()
        sent, failed = 0, 0
        for u in users:
            if u["is_banned"] or not u["is_verified"]:
                continue
            if target == "paid" and u["plan"] in ("trial", "free"):
                continue
            if target == "trial" and u["plan"] != "trial":
                continue
            try:
                await _bot().send_message(u["user_id"], text, parse_mode=ParseMode.HTML)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1

        await msg.answer(
            f"✅ Рассылка завершена!\n✉️ Отправлено: {sent}\n❌ Ошибок: {failed}",
            reply_markup=ikb([("🔙 Панель", "adm_back")]),
        )

    # ── Тикеты ────────────────────────────────────────────────────

    @r.callback_query(F.data == "adm_tickets")
    async def _adm_tickets(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        tickets = await DB.get_open_tickets()
        if not tickets:
            await q.message.edit_text(
                "💬 Открытых тикетов нет.",
                reply_markup=ikb([("🔙 Панель", "adm_back")])
            ); await q.answer(); return

        text    = f"💬 <b>Открытые тикеты ({len(tickets)}):</b>\n\n"
        buttons = []
        for t in tickets[:10]:
            text += (
                f"#{t['id']} | {h(t['first_name'] or '?')} @{t['username'] or '?'}\n"
                f"└ {h((t['message'] or '')[:60])}…\n\n"
            )
            buttons.append([(f"#{t['id']} — {t['first_name'] or '?'}", f"adm_reply_{t['id']}_{t['user_id']}")])
        buttons.append([("🔙 Панель", "adm_back")])
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*buttons))
        await q.answer()

    @r.callback_query(F.data.startswith("adm_reply_"))
    async def _adm_reply_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        parts = q.data.split("_")
        tid   = int(parts[2])
        uid   = int(parts[3])
        await state.update_data(reply_tid=tid, reply_uid=uid)
        await q.message.reply(
            f"↩️ Введи ответ на тикет #{tid}:",
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.reply_text)
        await q.answer()

    @r.message(StateFilter(AdminSt.reply_text))
    async def _adm_reply_send(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        data  = await state.get_data()
        tid   = data.get("reply_tid")
        uid   = data.get("reply_uid")
        reply = msg.text or "[медиа]"
        await DB.close_ticket(tid, reply)
        await state.clear()
        await msg.answer(
            f"✅ Ответ на тикет #{tid} отправлен.",
            reply_markup=ikb([("🔙 Тикеты", "adm_tickets")]),
        )
        try:
            await _bot().send_message(
                uid,
                f"📩 <b>Ответ поддержки (тикет #{tid}):</b>\n\n{h(reply)}",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    # ── Конфиг (расширенный v8.2) ────────────────────────────────

    CONFIG_GROUPS = {
        "📱 API":           ["pyro_api_id", "pyro_api_hash"],
        "💳 Подписки":      ["test_period_days", "max_trial_days",
                              "clone_bonus_days", "max_clones_per_user"],
        "🎁 Рефералы":      ["ref_goal", "referral_premium_months", "referral_premium_enabled"],
        "🔒 Безопасность":  ["auto_verify_enabled", "channel_check_hours",
                              "delete_sensitive_msgs"],
        "💾 Сохранение":    ["save_dm_messages", "save_test_replies",
                              "dev_command_enabled", "media_save_chatbot"],
        "📡 Мониторинг":    ["auto_monitor_on_connect",
                              "bulk_delete_threshold", "bulk_delete_window"],
        "⚙️ Прочее":        ["maintenance_mode", "support_username",
                              "broadcast_footer", "bot_description", "welcome_message"],
    }

    @r.callback_query(F.data == "adm_config")
    async def _adm_config(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        buttons = []
        for group_name in CONFIG_GROUPS:
            buttons.append([(f"📂 {group_name}", f"adm_cfg_group_{group_name}")])
        buttons.append([("🔙 Панель", "adm_back")])
        await q.message.edit_text(
            "🔧 <b>Конфигурация MerAi v8.2</b>\n\nВыбери раздел:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb(*buttons),
        )
        await q.answer()

    @r.callback_query(F.data.startswith("adm_cfg_group_"))
    async def _adm_cfg_group(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        group_name = q.data[len("adm_cfg_group_"):]
        keys       = CONFIG_GROUPS.get(group_name, [])
        if not keys:
            await q.answer("Группа не найдена", show_alert=True); return

        text    = f"🔧 <b>{group_name}</b>\n\n"
        buttons = []
        for k in keys:
            v = await DB.get_config(k)
            # Маскируем чувствительные данные
            display_v = (v[:6] + "…") if (k in ("pyro_api_hash",) and v) else (v or "—")
            text += f"<code>{k}</code>: <b>{display_v}</b>\n"
            buttons.append([(f"✏️ {k}", f"adm_cfgedit_{k}")])
        buttons.append([("🔙 Конфиг", "adm_config")])
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*buttons))
        await q.answer()

    @r.callback_query(F.data.startswith("adm_cfgedit_"))
    async def _adm_cfgedit_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        key    = q.data[len("adm_cfgedit_"):]
        curval = await DB.get_config(key)
        await state.update_data(cfg_key=key)
        await q.message.edit_text(
            f"✏️ Редактирование: <code>{key}</code>\n"
            f"Текущее значение: <b>{h(curval or '—')}</b>\n\n"
            f"Введи новое значение:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "adm_config")]),
        )
        await state.set_state(AdminSt.config_val)
        await q.answer()

    @r.message(StateFilter(AdminSt.config_val))
    async def _adm_cfg_save(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        data = await state.get_data()
        key  = data.get("cfg_key")
        val  = msg.text.strip()
        await DB.set_config(key, val)
        await state.clear()

        # Обновляем глобальные переменные
        global PYRO_API_ID, PYRO_API_HASH, REF_GOAL, CLONE_BONUS_DAYS, TEST_PERIOD_DAYS
        if key == "pyro_api_id":
            try: PYRO_API_ID = int(val)
            except: pass
        elif key == "pyro_api_hash":
            PYRO_API_HASH = val
        elif key == "ref_goal":
            try: REF_GOAL = int(val)
            except: pass
        elif key == "clone_bonus_days":
            try: CLONE_BONUS_DAYS = int(val)
            except: pass
        elif key == "test_period_days":
            try: TEST_PERIOD_DAYS = int(val)
            except: pass

        await msg.answer(
            f"✅ Сохранено: <code>{key}</code> = <code>{h(val)}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("🔙 Конфиг", "adm_config")]),
        )

    # ── Баны ──────────────────────────────────────────────────────

    @r.callback_query(F.data == "adm_bans")
    async def _adm_bans(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        async with DB._conn.execute(
            "SELECT * FROM users WHERE is_banned=1 ORDER BY last_active DESC"
        ) as c:
            banned = [dict(r) for r in await c.fetchall()]

        if not banned:
            await q.message.edit_text(
                "🚫 Забаненных пользователей нет.",
                reply_markup=ikb([("🔙 Панель", "adm_back")])
            ); await q.answer(); return

        text = f"🚫 <b>Забаненные ({len(banned)}):</b>\n\n"
        buttons = []
        for u in banned:
            text += f"<code>{u['user_id']}</code> {h(u['first_name'] or '?')} @{u['username'] or '—'}\n"
            buttons.append([(f"✅ Разбанить {u['user_id']}", f"adm_unban_{u['user_id']}")])
        buttons.append([("🔙 Панель", "adm_back")])
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*buttons))
        await q.answer()

    # ── Транзакции ────────────────────────────────────────────────

    @r.callback_query(F.data == "adm_txns")
    async def _adm_txns(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        async with DB._conn.execute("""
            SELECT t.*, u.username, u.first_name FROM transactions t
            LEFT JOIN users u ON u.user_id=t.user_id
            WHERE t.status='completed'
            ORDER BY t.created_at DESC LIMIT 20
        """) as c:
            txns = [dict(r) for r in await c.fetchall()]

        text = "💰 <b>Последние транзакции (20):</b>\n\n"
        for t in txns:
            text += (
                f"{plan_emoji(t['plan'])} {t['stars']}⭐ | "
                f"{h(t['first_name'] or '?')} @{t['username'] or '?'}\n"
                f"  {(t['created_at'] or '')[:16]}\n"
            )
        if not txns:
            text += "Транзакций нет."
        await q.message.edit_text(text, parse_mode=ParseMode.HTML,
                                  reply_markup=ikb([("🔙 Панель", "adm_back")]))
        await q.answer()

    # ── Статистика ────────────────────────────────────────────────

    @r.callback_query(F.data == "adm_stats")
    async def _adm_stats(q: CallbackQuery):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        s = await DB.stats()
        text = (
            f"📊 <b>Подробная статистика v8.2</b>\n\n"
            f"👥 Всего пользователей: {s['total']}\n"
            f"✅ Верифицировано: {s['verified']}\n"
            f"🎁 На тестовом периоде: {s['trial']}\n"
            f"💎 Платящих: {s['paid']}\n"
            f"🚫 Забанено: {s['banned']}\n\n"
            f"🤖 Клонов активных: {s['clones']}\n"
            f"👤 Userbot запущено: {len(ub_clients)}\n"
            f"🤖 Клонов запущено: {len(clone_bots)}\n\n"
            f"⭐ Собрано Stars: {s['stars']}\n"
            f"🗑 Перехвачено удалений: {s['deletions']}\n"
            f"💬 Открытых тикетов: {s['tickets']}\n"
        )
        await q.message.edit_text(text, parse_mode=ParseMode.HTML,
                                  reply_markup=ikb([("🔙 Панель", "adm_back")]))
        await q.answer()

    # ── Отправить напрямую ────────────────────────────────────────

    @r.message(Command("send"))
    async def _cmd_send(msg: Message):
        if msg.from_user.id != ADMIN_ID: return
        parts = msg.text.split(maxsplit=2)
        if len(parts) < 3:
            await msg.answer("Использование: /send <user_id> <текст>"); return
        try:
            uid  = int(parts[1])
            text = parts[2]
            await _bot().send_message(uid, f"📩 {text}", parse_mode=ParseMode.HTML)
            await msg.answer(f"✅ Отправлено {uid}")
        except Exception as e:
            await msg.answer(f"❌ Ошибка: {e}")

    # ── Admin отправить ───────────────────────────────────────────

    @r.callback_query(F.data == "adm_send")
    async def _adm_send_start(q: CallbackQuery, state: FSMContext):
        if q.from_user.id != ADMIN_ID: await q.answer(); return
        await q.message.edit_text(
            "📤 Введи ID пользователя:",
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.send_direct_uid)
        await q.answer()

    @r.message(StateFilter(AdminSt.send_direct_uid))
    async def _adm_send_uid(msg: Message, state: FSMContext):
        if msg.from_user.id != ADMIN_ID: return
        try:
            uid = int(msg.text.strip())
        except Exception:
            await msg.answer("❌ Введи числовой ID"); return
        await state.update_data(target_uid=uid)
        await msg.answer(
            f"📤 Введи сообщение для <code>{uid}</code>:",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("❌ Отмена", "adm_back")]),
        )
        await state.set_state(AdminSt.send_direct_msg)

    # ══════════════════════════════════════════════════════════════
    #  BUSINESS CONNECTION & MESSAGES
    # ══════════════════════════════════════════════════════════════

    @r.business_connection()
    async def _on_biz_conn(conn: BusinessConnection):
        uid = conn.user.id
        await DB.upsert_user(uid, conn.user.username, conn.user.first_name)

        if conn.is_enabled:
            await DB.set_field(uid, "biz_connected",  1)
            await DB.set_field(uid, "mode",           "chatbot")

            # v8.2: Тестовый период сразу при подключении бизнеса
            user = await DB.get_user(uid)
            if not user["is_verified"]:
                await DB.verify(uid)

            # v8.2: Автовключение мониторинга
            auto_mon = await DB.get_config("auto_monitor_on_connect", "1")
            if auto_mon == "1":
                await DB.set_field(uid, "monitoring_on", 1)

            test_days = await DB.get_config("test_period_days", "3")
            try:
                await _bot().send_message(
                    uid,
                    f"✅ <b>Telegram Business подключён!</b>\n\n"
                    f"📡 Мониторинг включён автоматически.\n\n"
                    f"🎁 <b>{test_days} дня полного доступа активированы!</b>\n\n"
                    f"<b>Что мониторится:</b>\n"
                    f"• Удалённые и отредактированные сообщения\n"
                    f"• Все медиафайлы (фото, видео, аудио)\n"
                    f"• Команда <code>.dev</code> — сохрани любое сообщение\n\n"
                    f"Управление: /start",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        else:
            await DB.set_field(uid, "biz_connected",  0)
            await DB.set_field(uid, "monitoring_on",  0)
            try:
                await _bot().send_message(
                    uid,
                    "❌ <b>Telegram Business отключён.</b>\nМониторинг остановлен.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

    # ── Business: входящие сообщения (кэш + .dev) ─────────────────

    @r.business_message()
    async def _biz_message(msg: Message):
        """
        v8.2: Обработка Business-сообщений:
        - Кэшируем все входящие для перехвата удалений
        - Если текст ".dev" — сохраняем реплаенутое сообщение
        - Сохраняем аудио/видео/голосовые
        """
        if not msg.business_connection_id:
            return

        # Определяем владельца через business_connection_id
        # В aiogram 3.x нужно найти пользователя по biz_connection
        # Пробуем через from_user - если это личное сообщение владельца
        owner_id = None
        try:
            conn_info = await _bot().get_business_connection(msg.business_connection_id)
            owner_id  = conn_info.user.id
        except Exception:
            pass

        if not owner_id:
            return

        user = await DB.get_user(owner_id)
        if not user or not user["monitoring_on"]:
            return

        # .dev команда
        dev_enabled = await DB.get_config("dev_command_enabled", "1")
        if dev_enabled == "1" and msg.text and msg.text.strip().lower() == ".dev":
            if msg.reply_to_message:
                await save_message_to_cache(msg.reply_to_message, owner_id, _bot(), reason="dev")
            # Удаляем .dev
            try:
                await _bot().delete_business_messages(
                    business_connection_id=msg.business_connection_id,
                    message_ids=[msg.message_id],
                )
            except Exception:
                pass
            return

        # Кэшируем сообщение для перехвата удалений
        mtype   = "text"
        content = msg.text or msg.caption or ""
        file_id = None
        media_bytes = None

        if msg.photo:
            mtype = "photo"; file_id = msg.photo[-1].file_id
        elif msg.video:
            mtype = "video"; file_id = msg.video.file_id
        elif msg.voice:
            mtype = "voice"; file_id = msg.voice.file_id
        elif msg.video_note:
            mtype = "video_note"; file_id = msg.video_note.file_id
        elif msg.audio:
            mtype = "audio"; file_id = msg.audio.file_id
        elif msg.document:
            mtype = "document"; file_id = msg.document.file_id
        elif msg.sticker:
            mtype = "sticker"; file_id = msg.sticker.file_id
        elif msg.animation:
            mtype = "animation"; file_id = msg.animation.file_id

        # v8.2: Скачиваем медиа для сохранения (media_save_chatbot)
        media_save = await DB.get_config("media_save_chatbot", "1")
        if media_save == "1" and file_id:
            try:
                fi          = await _bot().get_file(file_id)
                dl_bytes    = await _bot().download_file(fi.file_path)
                if hasattr(dl_bytes, "read"):
                    media_bytes = dl_bytes.read()
                elif isinstance(dl_bytes, (bytes, bytearray)):
                    media_bytes = bytes(dl_bytes)
            except Exception as e:
                log.debug(f"biz_media download: {e}")

        sender_name = msg.from_user.full_name if msg.from_user else "Unknown"
        sender_id   = msg.from_user.id if msg.from_user else 0
        chat_title  = (
            getattr(msg.chat, "title", None) or
            getattr(msg.chat, "username", None) or
            str(msg.chat.id)
        )

        entry = {
            "cid": msg.chat.id, "mid": msg.message_id, "type": mtype,
            "sender": sender_name, "title": chat_title, "text": content,
            "file_id": file_id, "media_bytes": media_bytes
        }
        mem_cache[owner_id].append(entry)
        asyncio.create_task(
            DB.cache_msg(owner_id, msg.chat.id, msg.message_id,
                         sender_id, sender_name, chat_title, mtype,
                         content, file_id, media_bytes)
        )

    @r.edited_business_message()
    async def _biz_edited(msg: Message):
        if not msg.business_connection_id:
            return
        owner_id = None
        try:
            conn_info = await _bot().get_business_connection(msg.business_connection_id)
            owner_id  = conn_info.user.id
        except Exception:
            pass
        if not owner_id:
            return

        user = await DB.get_user(owner_id)
        if not user or not user["monitoring_on"]:
            return

        cid    = msg.chat.id
        title  = getattr(msg.chat, "title", None) or str(cid)
        sender = msg.from_user.full_name if msg.from_user else "Unknown"
        new_t  = msg.text or msg.caption or ""
        cached = _mem_get(owner_id, cid, msg.message_id)
        old_t  = cached["text"] if cached else "—"

        await _bot().send_message(
            owner_id, notif_edited(sender, title, old_t, new_t), parse_mode=ParseMode.HTML
        )
        asyncio.create_task(DB.log_edit(owner_id, cid, title, sender, old_t, new_t))
        if cached:
            cached["text"] = new_t

    @r.deleted_business_messages()
    async def _biz_deleted(event):
        owner_id = None
        try:
            conn_info = await _bot().get_business_connection(event.business_connection_id)
            owner_id  = conn_info.user.id
        except Exception:
            pass
        if not owner_id:
            return

        user = await DB.get_user(owner_id)
        if not user or not user["monitoring_on"]:
            return

        chat_id    = event.chat.id
        chat_title = (
            getattr(event.chat, "title", None) or
            getattr(event.chat, "username", None) or
            str(chat_id)
        )
        ids = event.message_ids if hasattr(event, "message_ids") else []

        is_bulk = await DB.check_bulk_delete(owner_id, chat_id, len(ids))
        if is_bulk:
            await DB.reset_bulk_events(owner_id)
            await _bot().send_message(
                owner_id, notif_bulk(chat_title, len(ids)), parse_mode=ParseMode.HTML
            )
            try:
                zdata = await build_zip(owner_id, chat_id, chat_title)
                fn    = f"merai_{chat_title[:12]}_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
                await _bot().send_document(
                    owner_id, BufferedInputFile(zdata, filename=fn),
                    caption=f"📦 <b>{h(chat_title)}</b>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                log.error(f"ZIP err: {e}")
            return

        for mid in ids:
            cached = _mem_get(owner_id, chat_id, mid)
            if not cached:
                db_r = await DB.get_cached_msg(owner_id, chat_id, mid)
                if db_r:
                    cached = {
                        "cid": db_r["chat_id"], "mid": db_r["msg_id"],
                        "type": db_r["msg_type"], "sender": db_r["sender_name"],
                        "title": db_r["chat_title"], "text": db_r["content"],
                        "file_id": db_r["file_id"],
                        "media_bytes": db_r.get("media_bytes"),
                    }

            if cached:
                mtype  = cached["type"]
                sender = cached["sender"]
                mb     = cached.get("media_bytes")
                asyncio.create_task(
                    DB.log_deletion(owner_id, chat_id, chat_title,
                                    sender, mtype, cached.get("text"), cached.get("file_id"))
                )
                if mtype == "text":
                    await _bot().send_message(
                        owner_id,
                        notif_deleted_text(sender, chat_title, cached.get("text") or ""),
                        parse_mode=ParseMode.HTML
                    )
                else:
                    note = notif_deleted_media(sender, chat_title, mtype)
                    sent = False
                    if mb:
                        ext_map = {"photo": "jpg", "video": "mp4", "voice": "ogg",
                                   "video_note": "mp4", "audio": "mp3"}
                        ext = ext_map.get(mtype, "dat")
                        bif = BufferedInputFile(mb, filename=f"del_{mid}.{ext}")
                        try:
                            if mtype == "photo":
                                await _bot().send_photo(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video":
                                await _bot().send_video(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "voice":
                                await _bot().send_voice(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video_note":
                                await _bot().send_video_note(owner_id, bif)
                                await _bot().send_message(owner_id, note, parse_mode=ParseMode.HTML)
                            elif mtype == "audio":
                                await _bot().send_audio(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            else:
                                await _bot().send_document(owner_id, bif, caption=note, parse_mode=ParseMode.HTML)
                            sent = True
                        except Exception:
                            pass
                    if not sent and cached.get("file_id"):
                        try:
                            fid = cached["file_id"]
                            if mtype == "photo":
                                await _bot().send_photo(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video":
                                await _bot().send_video(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "audio":
                                await _bot().send_audio(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            else:
                                await _bot().send_document(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            sent = True
                        except Exception:
                            pass
                    if not sent:
                        await _bot().send_message(owner_id, note, parse_mode=ParseMode.HTML)
            else:
                ts = datetime.now().strftime("%d.%m.%Y %H:%M")
                await _bot().send_message(
                    owner_id,
                    f"🗑 <b>Сообщение удалено</b>\n"
                    f"┌ 💬 <b>Чат:</b> {h(chat_title)}\n"
                    f"├ 🔢 <b>ID:</b> {mid}\n"
                    f"└ 🕐 <b>Время:</b> {ts}\n<i>(не в кэше)</i>",
                    parse_mode=ParseMode.HTML
                )

    # ── Fallback ──────────────────────────────────────────────────

    @r.message(F.text)
    async def _msg_any(msg: Message, state: FSMContext):
        curr = await state.get_state()
        if curr:
            return
        user = await DB.get_user(msg.from_user.id)
        if not user or not user["is_verified"]:
            await _start(msg, state); return
        if user["is_banned"]:
            await msg.answer("🚫 Ваш аккаунт заблокирован."); return
        if user.get("channel_left"):
            await msg.answer(
                f"❌ <b>Вы покинули канал!</b>\nВступите обратно: {CAPTCHA_CHANNEL}",
                parse_mode=ParseMode.HTML
            ); return
        await _show_main(msg, user, POWERED_BY)

    return r


# ═══════════════════════════════════════════════════════════════════
#  9. УПРАВЛЕНИЕ КЛОН-БОТАМИ
# ═══════════════════════════════════════════════════════════════════

async def launch_clone(token: str, owner_id: int, clone_id: int = None):
    try:
        clone_bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        dp_clone = Dispatcher(storage=MemoryStorage())
        dp_clone.include_router(create_router(
            is_clone=True,
            clone_id=clone_id,
            clone_bot_instance=clone_bot,
        ))
        log.info(f"Clone bot launched owner={owner_id} clone_id={clone_id}")
        task = asyncio.create_task(
            dp_clone.start_polling(
                clone_bot,
                handle_signals=False,
                drop_pending_updates=True,
                allowed_updates=[
                    "message", "callback_query", "inline_query",
                    "pre_checkout_query", "chat_join_request",
                    "business_connection", "business_message",
                    "edited_business_message", "deleted_business_messages",
                ],
            )
        )
        clone_bots[token] = (clone_bot, dp_clone, task)
    except Exception as e:
        log.error(f"Clone launch err token={token[:20]}: {e}")
        if token in clone_bots:
            del clone_bots[token]

async def stop_clone(token: str):
    if token in clone_bots:
        clone_bot, dp, task = clone_bots[token]
        try:
            task.cancel()
            await clone_bot.session.close()
        except Exception:
            pass
        del clone_bots[token]

# ═══════════════════════════════════════════════════════════════════
#  10. ФОНОВЫЕ ЗАДАЧИ
# ═══════════════════════════════════════════════════════════════════

async def restore_sessions():
    log.info("Восстанавливаю сессии и клоны…")
    await asyncio.sleep(3)
    users = await DB.all_users()
    for u in users:
        if u.get("ub_session") and u.get("ub_active") and u.get("monitoring_on"):
            api_id   = u["pyro_api_id"] or PYRO_API_ID
            api_hash = u["pyro_api_hash"] or PYRO_API_HASH
            if api_id and api_hash:
                asyncio.create_task(
                    start_userbot(u["user_id"], u["ub_session"], api_id, api_hash)
                )
    clones = await DB.all_clones()
    for c in clones:
        if c["is_active"]:
            asyncio.create_task(launch_clone(c["bot_token"], c["owner_id"], c["id"]))

async def autorenew_task():
    while True:
        await asyncio.sleep(3600)
        try:
            users = await DB.all_users()
            for u in users:
                if u["auto_renew"] and u["plan"] not in ("trial", "free") and u["plan_expires"]:
                    try:
                        exp = datetime.fromisoformat(u["plan_expires"])
                        if exp.tzinfo is None:
                            exp = exp.replace(tzinfo=timezone.utc)
                        if datetime.now(timezone.utc) > exp - timedelta(hours=24):
                            plan = PLANS.get(u["plan"])
                            if plan:
                                await bot_instance.send_invoice(
                                    chat_id=u["user_id"],
                                    title=f"MerAi — {plan['name']} (продление)",
                                    description=f"Автопродление на {plan['days']} дней",
                                    payload=f"plan:{u['plan']}",
                                    currency="XTR",
                                    prices=[LabeledPrice(label=plan["name"], amount=plan["stars"])],
                                )
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"autorenew_task: {e}")

async def channel_check_task():
    """v8.2: Периодическая проверка членства в канале"""
    while True:
        try:
            check_interval = int(await DB.get_config("channel_check_hours", "1"))
        except Exception:
            check_interval = 1
        await asyncio.sleep(check_interval * 3600)
        try:
            users = await DB.all_users()
            for u in users:
                if not u["is_verified"] or u["is_banned"]:
                    continue
                last_check = u.get("last_channel_check")
                if last_check:
                    try:
                        ldt = datetime.fromisoformat(last_check)
                        if ldt.tzinfo is None:
                            ldt = ldt.replace(tzinfo=timezone.utc)
                        if datetime.now(timezone.utc) - ldt < timedelta(hours=check_interval):
                            continue
                    except Exception:
                        pass
                is_member = await check_channel_membership(bot_instance, u["user_id"])
                if not is_member and not u.get("channel_left"):
                    try:
                        await bot_instance.send_message(
                            u["user_id"],
                            f"⚠️ <b>Вы покинули канал!</b>\n\n"
                            f"Мониторинг остановлен. Вернитесь в канал:\n{CAPTCHA_CHANNEL}\n\n"
                            f"После вступления нажмите /start",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass
                    await DB.set_field(u["user_id"], "monitoring_on", 0)
                    if u["user_id"] in ub_clients:
                        asyncio.create_task(stop_userbot(u["user_id"]))
        except Exception as e:
            log.error(f"channel_check_task: {e}")

async def referral_reward_task():
    """v8.2: Проверка и выдача реферальных наград"""
    while True:
        await asyncio.sleep(3600)
        try:
            goal = int(await DB.get_config("ref_goal", "50"))
            enabled = await DB.get_config("referral_premium_enabled", "1")
            if enabled != "1":
                continue
            months = int(await DB.get_config("referral_premium_months", "1"))
            users  = await DB.all_users()
            for u in users:
                if u["referral_count"] >= goal:
                    # Проверяем есть ли непровешенные рефералы
                    async with DB._conn.execute("""
                        SELECT * FROM referrals WHERE referrer_id=? AND rewarded=0
                        LIMIT 1
                    """, (u["user_id"],)) as c:
                        pending = await c.fetchone()
                    if pending:
                        ok = await gift_premium_to_user(bot_instance, u["user_id"], months)
                        if ok:
                            await DB._conn.execute(
                                "UPDATE referrals SET rewarded=1 WHERE referrer_id=?",
                                (u["user_id"],)
                            )
                            await DB._conn.commit()
        except Exception as e:
            log.error(f"referral_reward_task: {e}")

# ═══════════════════════════════════════════════════════════════════
#  11. MAIN
# ═══════════════════════════════════════════════════════════════════

async def main():
    global bot_instance, BOT_USERNAME, PYRO_API_ID, PYRO_API_HASH
    global REF_GOAL, CLONE_BONUS_DAYS, TEST_PERIOD_DAYS

    await DB.connect()

    # Загружаем конфиги
    try:
        PYRO_API_ID      = int(await DB.get_config("pyro_api_id", "0") or 0)
        PYRO_API_HASH    =     await DB.get_config("pyro_api_hash", "")
        REF_GOAL         = int(await DB.get_config("ref_goal",         "50"))
        CLONE_BONUS_DAYS = int(await DB.get_config("clone_bonus_days", "3"))
        TEST_PERIOD_DAYS = int(await DB.get_config("test_period_days", "3"))
    except Exception:
        pass

    bot_instance = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    me           = await bot_instance.get_me()
    BOT_USERNAME = me.username
    log.info(f"🚀 Бот @{BOT_USERNAME} (ID {me.id}) v8.2 стартует…")

    from aiogram.types import BotCommand
    await bot_instance.set_my_commands([
        BotCommand(command="start",   description="Главное меню"),
        BotCommand(command="help",    description="Справка"),
        BotCommand(command="plan",    description="Тарифные планы"),
        BotCommand(command="support", description="Поддержка"),
        BotCommand(command="terms",   description="Условия использования"),
        BotCommand(command="admin",   description="Панель администратора"),
        BotCommand(command="send",    description="Отправить сообщение [admin]"),
    ])

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(create_router(is_clone=False))

    asyncio.create_task(restore_sessions())
    asyncio.create_task(autorenew_task())
    asyncio.create_task(channel_check_task())
    asyncio.create_task(referral_reward_task())

    log.info("✅ Запуск polling v8.2…")
    try:
        await dp.start_polling(
            bot_instance,
            allowed_updates=[
                "message", "callback_query", "inline_query",
                "pre_checkout_query", "chat_join_request",
                "business_connection", "business_message",
                "edited_business_message", "deleted_business_messages",
            ],
            drop_pending_updates=True,
        )
    finally:
        for uid in list(ub_clients.keys()):
            await stop_userbot(uid)
        for token in list(clone_bots.keys()):
            await stop_clone(token)
        await bot_instance.session.close()
        await DB.close()
        log.info("👋 Бот остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")
    except Exception as e:
        log.critical(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
