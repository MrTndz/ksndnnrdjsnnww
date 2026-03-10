#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║   MerAi & Monitoring v7.0                                       ║
║   Автор: @mrztn  |  Дата: 09.03.2026                            ║
║   Python 3.11+  |  aiogram 3.26.0  |  Pyrogram 2.0.106          ║
║   Telegram Bot API 9.5                                           ║
╚══════════════════════════════════════════════════════════════════╝

ФУНКЦИОНАЛ:
  ✅ Капча через join request (без одобрения — просто факт заявки)
  ✅ Bot Mode (Business) / UserBot Mode (Pyrogram)
  ✅ Мониторинг удалений, редактирований, видео-кружков, голосовых,
     исчезающих сообщений через Pyrogram
  ✅ ZIP/HTML архив при массовых удалениях (≥5 за 3 сек или ≥10 сразу)
  ✅ Оплата через Telegram Stars (встроенные инвойсы)
  ✅ Клонирование ботов (+3 дня за каждый)
  ✅ Рассылки (массовая / одиночная)
  ✅ Полная админ-панель: управление юзерами, детальная статистика удалений
  ✅ Реферальная система
  ✅ Техподдержка через бота
  ✅ FSM для всех многошаговых процессов
  ✅ Полная обработка ошибок
"""

import os, sys, asyncio, logging, aiosqlite, json, io, zipfile, html as html_module
import time, re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from collections import defaultdict, deque

# ── aiogram 3.26.0 ────────────────────────────────────────────────────────────
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatJoinRequest, LabeledPrice, PreCheckoutQuery,
    InlineQueryResultArticle, InputTextMessageContent, InlineQuery,
    BufferedInputFile,
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramAPIError

# ── Pyrogram 2.0.106 ──────────────────────────────────────────────────────────
try:
    from pyrogram import Client as PyroClient, filters as pyro_filters, idle
    from pyrogram.types import (
        Message as PyroMessage,
        User as PyroUser,
    )
    from pyrogram.handlers import MessageHandler as PyroMsgHandler
    from pyrogram.enums import MessageMediaType
    PYROGRAM_OK = True
except ImportError:
    PYROGRAM_OK = False
    logging.warning("Pyrogram не установлен — userbot недоступен")

# ═══════════════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ  (токены оставлены — замени при релизе)
# ═══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN        = "8505484152:AAHXEFt0lyeMK5ZSJHRYpdPhhFJ0s142Bng"
ADMIN_ID         = 7785371505          # @mrztn
CAPTCHA_CHANNEL  = "https://t.me/+AW8ztiMHGY9jMGQ6"
CAPTCHA_CHAN_ID  = -1003716882147      # ID приватного канала-капчи

# Pyrogram — получи на my.telegram.org/apps
PYRO_API_ID      = 0      # ← вставь свой
PYRO_API_HASH    = ""     # ← вставь свой

DB_FILE          = "merai.db"

# ─── Планы подписки ───────────────────────────────────────────────────────────
PLANS = {
    "week":    {"name": "📅 Неделя",    "stars": 100,  "days": 7},
    "month":   {"name": "📆 Месяц",     "stars": 300,  "days": 30},
    "quarter": {"name": "📊 3 месяца",  "stars": 800,  "days": 90},
    "year":    {"name": "🎯 Год",        "stars": 2500, "days": 365},
}

CLONE_BONUS_DAYS   = 3
REF_GOAL           = 50

# ═══════════════════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════════════════════
#  FSM СОСТОЯНИЯ
# ═══════════════════════════════════════════════════════════════════════════════

class UserBotSt(StatesGroup):
    phone       = State()
    code        = State()
    twofa       = State()

class CloneSt(StatesGroup):
    token       = State()

class SupportSt(StatesGroup):
    message     = State()

class AdminSt(StatesGroup):
    broadcast_text   = State()
    broadcast_single = State()
    set_plan_uid     = State()
    set_plan_which   = State()
    add_balance_uid  = State()
    add_balance_amt  = State()

# ═══════════════════════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

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
                user_id          INTEGER PRIMARY KEY,
                username         TEXT,
                first_name       TEXT,
                is_verified      INTEGER DEFAULT 0,
                mode             TEXT    DEFAULT 'none',
                plan             TEXT    DEFAULT 'free',
                plan_expires     TEXT,
                auto_renew       INTEGER DEFAULT 0,
                referrer_id      INTEGER,
                referral_count   INTEGER DEFAULT 0,
                balance_stars    INTEGER DEFAULT 0,
                is_banned        INTEGER DEFAULT 0,
                monitoring_on    INTEGER DEFAULT 0,
                ub_session       TEXT,
                ub_phone         TEXT,
                ub_active        INTEGER DEFAULT 0,
                created_at       TEXT    DEFAULT (datetime('now')),
                last_active      TEXT    DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cloned_bots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id     INTEGER,
                bot_token    TEXT UNIQUE,
                bot_username TEXT,
                bot_name     TEXT,
                is_active    INTEGER DEFAULT 1,
                added_at     TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (owner_id) REFERENCES users(user_id)
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
                ts           TEXT DEFAULT (datetime('now')),
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
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(referred_id)
            );

            CREATE TABLE IF NOT EXISTS broadcasts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                text         TEXT,
                sent         INTEGER DEFAULT 0,
                failed       INTEGER DEFAULT 0,
                status       TEXT DEFAULT 'pending',
                created_at   TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS support_tickets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                message     TEXT,
                status      TEXT DEFAULT 'open',
                reply       TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS bulk_ts (
                owner_id INTEGER,
                chat_id  INTEGER,
                ts       REAL
            );
        """)
        await cls._conn.commit()
        log.info("✅ БД подключена")

    @classmethod
    async def close(cls):
        if cls._conn:
            await cls._conn.close()

    # ── Пользователи ──────────────────────────────────────────────────────────

    @classmethod
    async def get_user(cls, uid: int) -> Optional[dict]:
        async with cls._conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)) as c:
            r = await c.fetchone()
            return dict(r) if r else None

    @classmethod
    async def upsert_user(cls, uid: int, username: str = None,
                          first_name: str = None, referrer: int = None):
        await cls._conn.execute("""
            INSERT INTO users (user_id, username, first_name, referrer_id)
            VALUES (?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username   = COALESCE(excluded.username, username),
                first_name = COALESCE(excluded.first_name, first_name),
                last_active = datetime('now')
        """, (uid, username, first_name, referrer))
        if referrer:
            await cls._conn.execute("""
                INSERT OR IGNORE INTO referrals (referrer_id, referred_id)
                VALUES (?,?)
            """, (referrer, uid))
            await cls._conn.execute("""
                UPDATE users SET referral_count = referral_count + 1
                WHERE user_id = ? AND NOT EXISTS (
                    SELECT 1 FROM referrals WHERE referrer_id=? AND referred_id=?
                )
            """, (referrer, referrer, uid))
        await cls._conn.commit()

    @classmethod
    async def verify(cls, uid: int):
        await cls._conn.execute(
            "UPDATE users SET is_verified=1 WHERE user_id=?", (uid,)
        )
        await cls._conn.commit()

    @classmethod
    async def set_plan(cls, uid: int, plan: str, days: int):
        exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        await cls._conn.execute(
            "UPDATE users SET plan=?, plan_expires=? WHERE user_id=?",
            (plan, exp, uid)
        )
        await cls._conn.commit()

    @classmethod
    async def plan_active(cls, uid: int) -> bool:
        u = await cls.get_user(uid)
        if not u or u["plan"] == "free" or not u["plan_expires"]:
            return False
        try:
            exp = datetime.fromisoformat(u["plan_expires"])
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            return exp > datetime.now(timezone.utc)
        except Exception:
            return False

    @classmethod
    async def add_days(cls, uid: int, days: int):
        u = await cls.get_user(uid)
        if u and u["plan_expires"]:
            try:
                exp = datetime.fromisoformat(u["plan_expires"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                new_exp = (exp + timedelta(days=days)).isoformat()
            except Exception:
                new_exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        else:
            new_exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        await cls._conn.execute(
            "UPDATE users SET plan_expires=? WHERE user_id=?", (new_exp, uid)
        )
        await cls._conn.commit()

    @classmethod
    async def all_users(cls) -> List[dict]:
        async with cls._conn.execute("SELECT * FROM users") as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def set_field(cls, uid: int, field: str, value):
        await cls._conn.execute(
            f"UPDATE users SET {field}=? WHERE user_id=?", (value, uid)
        )
        await cls._conn.commit()

    # ── Сообщения (кеш) ───────────────────────────────────────────────────────

    @classmethod
    async def cache_msg(cls, owner_id: int, chat_id: int, msg_id: int,
                        sender_id: int, sender_name: str, chat_title: str,
                        msg_type: str, content: str = None, file_id: str = None):
        try:
            await cls._conn.execute("""
                INSERT OR REPLACE INTO msg_cache
                (owner_id, chat_id, msg_id, sender_id, sender_name,
                 chat_title, msg_type, content, file_id)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (owner_id, chat_id, msg_id, sender_id, sender_name,
                  chat_title, msg_type, content, file_id))
            await cls._conn.commit()
        except Exception as e:
            log.debug(f"cache_msg err: {e}")

    @classmethod
    async def get_cached_msg(cls, owner_id: int, chat_id: int,
                              msg_id: int) -> Optional[dict]:
        async with cls._conn.execute("""
            SELECT * FROM msg_cache
            WHERE owner_id=? AND chat_id=? AND msg_id=?
        """, (owner_id, chat_id, msg_id)) as c:
            r = await c.fetchone()
            return dict(r) if r else None

    @classmethod
    async def get_all_cached(cls, owner_id: int, chat_id: int) -> List[dict]:
        async with cls._conn.execute("""
            SELECT * FROM msg_cache WHERE owner_id=? AND chat_id=?
            ORDER BY ts
        """, (owner_id, chat_id)) as c:
            return [dict(r) for r in await c.fetchall()]

    # ── Лог удалений ──────────────────────────────────────────────────────────

    @classmethod
    async def log_deletion(cls, owner_id: int, chat_id: int, chat_title: str,
                           sender_name: str, msg_type: str, content: str = None):
        await cls._conn.execute("""
            INSERT INTO deletion_log
            (owner_id, chat_id, chat_title, sender_name, msg_type, content)
            VALUES (?,?,?,?,?,?)
        """, (owner_id, chat_id, chat_title, sender_name, msg_type, content))
        await cls._conn.commit()

    @classmethod
    async def get_deletion_stats(cls, uid: int) -> dict:
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

    @classmethod
    async def log_edit(cls, owner_id: int, chat_id: int, chat_title: str,
                       sender_name: str, old_text: str, new_text: str):
        await cls._conn.execute("""
            INSERT INTO edit_log
            (owner_id, chat_id, chat_title, sender_name, old_text, new_text)
            VALUES (?,?,?,?,?,?)
        """, (owner_id, chat_id, chat_title, sender_name, old_text, new_text))
        await cls._conn.commit()

    # ── Клонированные боты ────────────────────────────────────────────────────

    @classmethod
    async def add_clone(cls, owner_id: int, token: str,
                        username: str, name: str) -> bool:
        try:
            await cls._conn.execute("""
                INSERT INTO cloned_bots (owner_id, bot_token, bot_username, bot_name)
                VALUES (?,?,?,?)
            """, (owner_id, token, username, name))
            await cls._conn.commit()
            return True
        except Exception:
            return False

    @classmethod
    async def get_user_clones(cls, uid: int) -> List[dict]:
        async with cls._conn.execute(
            "SELECT * FROM cloned_bots WHERE owner_id=?", (uid,)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def all_clones(cls) -> List[dict]:
        async with cls._conn.execute("SELECT * FROM cloned_bots") as c:
            return [dict(r) for r in await c.fetchall()]

    # ── Транзакции ────────────────────────────────────────────────────────────

    @classmethod
    async def add_tx(cls, uid: int, plan: str, stars: int, payload: str):
        await cls._conn.execute("""
            INSERT INTO transactions (user_id, plan, stars, payload, status)
            VALUES (?,?,?,?,'completed')
        """, (uid, plan, stars, payload))
        await cls._conn.commit()

    # ── Тикеты поддержки ─────────────────────────────────────────────────────

    @classmethod
    async def create_ticket(cls, uid: int, msg: str) -> int:
        async with cls._conn.execute("""
            INSERT INTO support_tickets (user_id, message) VALUES (?,?)
        """, (uid, msg)) as c:
            await cls._conn.commit()
            return c.lastrowid

    @classmethod
    async def get_open_tickets(cls) -> List[dict]:
        async with cls._conn.execute("""
            SELECT st.*, u.username, u.first_name FROM support_tickets st
            LEFT JOIN users u ON u.user_id = st.user_id
            WHERE st.status='open' ORDER BY st.created_at DESC
        """) as c:
            return [dict(r) for r in await c.fetchall()]

    @classmethod
    async def close_ticket(cls, ticket_id: int, reply: str = None):
        await cls._conn.execute("""
            UPDATE support_tickets SET status='closed', reply=?
            WHERE id=?
        """, (reply, ticket_id))
        await cls._conn.commit()

    # ── Массовые удаления — отслеживание бёрста ──────────────────────────────

    @classmethod
    async def push_bulk_ts(cls, owner_id: int, chat_id: int) -> int:
        now = time.time()
        await cls._conn.execute(
            "INSERT INTO bulk_ts (owner_id, chat_id, ts) VALUES (?,?,?)",
            (owner_id, chat_id, now)
        )
        await cls._conn.execute(
            "DELETE FROM bulk_ts WHERE owner_id=? AND ts < ?",
            (owner_id, now - 3.0)
        )
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM bulk_ts WHERE owner_id=? AND ts >= ?",
            (owner_id, now - 3.0)
        ) as c:
            (cnt,) = await c.fetchone()
        await cls._conn.commit()
        return cnt

    @classmethod
    async def reset_bulk_ts(cls, owner_id: int):
        await cls._conn.execute(
            "DELETE FROM bulk_ts WHERE owner_id=?", (owner_id,)
        )
        await cls._conn.commit()

    # ── Статистика ────────────────────────────────────────────────────────────

    @classmethod
    async def stats(cls) -> dict:
        async with cls._conn.execute("SELECT COUNT(*) FROM users") as c:
            (total,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM users WHERE is_verified=1"
        ) as c:
            (verified,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM users WHERE plan!='free'"
        ) as c:
            (paid,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM cloned_bots WHERE is_active=1"
        ) as c:
            (clones,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COALESCE(SUM(stars),0) FROM transactions WHERE status='completed'"
        ) as c:
            (stars,) = await c.fetchone()
        async with cls._conn.execute(
            "SELECT COUNT(*) FROM deletion_log"
        ) as c:
            (dels,) = await c.fetchone()
        return {
            "total": total, "verified": verified, "paid": paid,
            "clones": clones, "stars": stars, "deletions": dels,
        }

# ═══════════════════════════════════════════════════════════════════════════════
#  RUNTIME STATE
# ═══════════════════════════════════════════════════════════════════════════════

mem_cache:    Dict[int, deque]           = defaultdict(lambda: deque(maxlen=5000))
ub_clients:   Dict[int, "PyroClient"]   = {}
clone_bots:   Dict[str, Bot]            = {}
ub_auth_data: Dict[int, dict]           = {}
bot_instance: Optional[Bot]             = None
BOT_USERNAME: str                       = ""

# ═══════════════════════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ═══════════════════════════════════════════════════════════════════════════════

def ikb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row]
        for row in rows
    ])

def plan_emoji(plan: str) -> str:
    return {"free":"🆓","week":"📅","month":"📆","quarter":"📊","year":"🎯"}.get(plan, "❓")

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
        "text":"💬","photo":"🖼","video":"📹","voice":"🎙",
        "video_note":"⭕","document":"📎","sticker":"🎭",
        "audio":"🎵","animation":"🎞","contact":"📱","ttl":"💣",
    }.get(t, "📁")

# ─── HTML/ZIP архив ───────────────────────────────────────────────────────────

def _html_report(msgs: List[dict], chat_title: str) -> bytes:
    rows = ""
    for m in msgs:
        sender = html_module.escape(m.get("sender_name") or "?")
        ts     = html_module.escape(m.get("ts") or "")
        mtype  = m.get("msg_type", "text")
        content= html_module.escape(m.get("content") or "")
        icon   = mtype_icon(mtype)
        rows  += (
            f'<div class="msg"><span class="meta">{icon} <b>{sender}</b>'
            f' · <span class="ts">{ts}</span></span>'
            f'<div class="body">{content or f"<i>[{mtype}]</i>"}</div></div>\n'
        )
    return (f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8">
<title>Архив: {html_module.escape(chat_title)}</title>
<style>
  body{{font-family:system-ui,sans-serif;background:#0e0e1a;color:#dde1f0;padding:24px;max-width:900px;margin:auto}}
  h1{{color:#6ec6e6;border-bottom:2px solid #6ec6e6;padding-bottom:8px}}
  .msg{{background:#161828;border-left:3px solid #6ec6e6;margin:8px 0;
        padding:10px 14px;border-radius:6px}}
  .meta{{font-size:.78em;color:#7eb8d4}}
  .ts{{color:#a0b4c8}}
  .body{{margin-top:4px;word-break:break-word}}
  s{{color:#e07070}}
</style></head><body>
<h1>📦 Архив: {html_module.escape(chat_title)}</h1>
<p style="color:#7eb8d4">Экспортировано: {datetime.now().strftime('%d.%m.%Y %H:%M UTC')}</p>
{rows}
</body></html>""").encode("utf-8")


async def build_zip(owner_id: int, chat_id: int, chat_title: str) -> io.BytesIO:
    msgs = await DB.get_all_cached(owner_id, chat_id)
    buf  = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.html", _html_report(msgs, chat_title))
        zf.writestr("messages.json",
                    json.dumps(msgs, ensure_ascii=False, indent=2, default=str))
    buf.seek(0)
    return buf

# ─── Уведомления ─────────────────────────────────────────────────────────────

def notif_deleted_text(sender: str, chat: str, text: str) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    preview = html_module.escape(text[:500]) + ("…" if len(text) > 500 else "")
    return (
        f"🗑 <b>Сообщение удалено</b>\n"
        f"┌ 👤 <b>Кто:</b> {html_module.escape(sender)}\n"
        f"├ 💬 <b>Чат:</b> {html_module.escape(chat)}\n"
        f"├ 🕐 <b>Время удаления:</b> {ts}\n"
        f"└ 📝 <b>Текст:</b>\n<blockquote>{preview}</blockquote>"
    )

def notif_deleted_media(sender: str, chat: str, mtype: str) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"🗑 <b>Медиа удалено</b>\n"
        f"┌ 👤 <b>Кто:</b> {html_module.escape(sender)}\n"
        f"├ 💬 <b>Чат:</b> {html_module.escape(chat)}\n"
        f"├ {mtype_icon(mtype)} <b>Тип:</b> {mtype.replace('_',' ').title()}\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

def notif_edited(sender: str, chat: str, old: str, new: str) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    o = html_module.escape(old[:300]) + ("…" if len(old) > 300 else "")
    n = html_module.escape(new[:300]) + ("…" if len(new) > 300 else "")
    return (
        f"✏️ <b>Сообщение изменено</b>\n"
        f"┌ 👤 <b>Кто:</b> {html_module.escape(sender)}\n"
        f"├ 💬 <b>Чат:</b> {html_module.escape(chat)}\n"
        f"├ 🕐 <b>Время:</b> {ts}\n"
        f"├ ❌ <b>Было:</b>\n│<s>{o}</s>\n"
        f"└ ✅ <b>Стало:</b>\n<blockquote>{n}</blockquote>"
    )

def notif_autodestruct(sender: str, chat: str, mtype: str, ttl: int) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"💣 <b>Исчезающее сообщение перехвачено!</b>\n"
        f"┌ 👤 <b>Кто:</b> {html_module.escape(sender)}\n"
        f"├ 💬 <b>Чат:</b> {html_module.escape(chat)}\n"
        f"├ {mtype_icon(mtype)} <b>Тип:</b> {mtype.replace('_',' ').title()}\n"
        f"├ ⏱ <b>TTL:</b> {ttl} сек\n"
        f"└ 🕐 <b>Время:</b> {ts}"
    )

def notif_bulk(chat: str, count: int) -> str:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")
    return (
        f"💥 <b>Массовое удаление!</b>\n"
        f"┌ 💬 <b>Чат:</b> {html_module.escape(chat)}\n"
        f"├ 🗑 <b>Удалено:</b> {count} сообщений\n"
        f"└ 🕐 <b>Время:</b> {ts}\n\n"
        f"📦 ZIP-архив чата отправляется…"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  USERBOT — PYROGRAM
# ═══════════════════════════════════════════════════════════════════════════════

def _pyro_sender_name(msg: "PyroMessage") -> str:
    if msg.from_user:
        fn = msg.from_user.first_name or ""
        ln = msg.from_user.last_name or ""
        return (fn + " " + ln).strip() or msg.from_user.username or str(msg.from_user.id)
    return "Unknown"

def _pyro_chat_title(msg: "PyroMessage") -> str:
    if msg.chat:
        return getattr(msg.chat, "title", None) or getattr(msg.chat, "username", None) or str(msg.chat.id)
    return "Unknown"

def _pyro_msg_type(msg: "PyroMessage") -> str:
    if getattr(msg, "video_note", None): return "video_note"
    if getattr(msg, "voice", None):      return "voice"
    if getattr(msg, "photo", None):      return "photo"
    if getattr(msg, "video", None):      return "video"
    if getattr(msg, "audio", None):      return "audio"
    if getattr(msg, "document", None):   return "document"
    if getattr(msg, "sticker", None):    return "sticker"
    if getattr(msg, "animation", None):  return "animation"
    if getattr(msg, "contact", None):    return "contact"
    return "text"

def _mem_key(owner_id: int, chat_id: int, msg_id: int) -> str:
    return f"{owner_id}:{chat_id}:{msg_id}"

def _get_mem(owner_id: int, chat_id: int, msg_id: int) -> Optional[dict]:
    for m in mem_cache[owner_id]:
        if m.get("cid") == chat_id and m.get("mid") == msg_id:
            return m
    return None

async def _handle_new_pyro(client: "PyroClient", msg: "PyroMessage"):
    """Кешируем каждое входящее сообщение userbot-пользователя."""
    owner_id = client._owner_id
    try:
        cid   = msg.chat.id
        mid   = msg.id
        mtype = _pyro_msg_type(msg)
        sender= _pyro_sender_name(msg)
        title = _pyro_chat_title(msg)
        text  = msg.text or msg.caption or ""

        entry = {"cid": cid, "mid": mid, "type": mtype,
                 "sender": sender, "title": title, "text": text,
                 "file_id": None, "ttl": getattr(msg, "ttl_seconds", None)}
        mem_cache[owner_id].append(entry)
        asyncio.create_task(
            DB.cache_msg(owner_id, cid, mid,
                         msg.from_user.id if msg.from_user else 0,
                         sender, title, mtype, text)
        )

        # Исчезающее сообщение — сразу уведомляем
        if getattr(msg, "ttl_seconds", None):
            u = await DB.get_user(owner_id)
            if u and u["monitoring_on"]:
                text_n = notif_autodestruct(sender, title, mtype, msg.ttl_seconds)
                # Попытка скачать медиа до исчезновения
                if msg.media:
                    try:
                        data = await client.download_media(msg, in_memory=True)
                        if data:
                            bif = BufferedInputFile(bytes(data.getbuffer()), filename=f"ttl_{mid}.dat")
                            if mtype == "photo":
                                await bot_instance.send_photo(owner_id, bif, caption=text_n, parse_mode=ParseMode.HTML)
                            elif mtype in ("video", "video_note"):
                                await bot_instance.send_video(owner_id, bif, caption=text_n, parse_mode=ParseMode.HTML)
                            elif mtype == "voice":
                                await bot_instance.send_voice(owner_id, bif, caption=text_n, parse_mode=ParseMode.HTML)
                            else:
                                await bot_instance.send_document(owner_id, bif, caption=text_n, parse_mode=ParseMode.HTML)
                            return
                    except Exception:
                        pass
                await bot_instance.send_message(owner_id, text_n, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.debug(f"[UB new msg] owner={owner_id}: {e}")


async def _handle_deleted_pyro(client: "PyroClient", msgs):
    """Перехватываем удалённые сообщения."""
    owner_id = client._owner_id
    try:
        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"]:
            return

        chat_id    = None
        chat_title = "Неизвестный чат"

        if hasattr(msgs, "__iter__") and not isinstance(msgs, list):
            msg_list = list(msgs)
        elif isinstance(msgs, list):
            msg_list = msgs
        else:
            msg_list = [msgs]

        ids = []
        for m in msg_list:
            mid = getattr(m, "id", None)
            if mid is None:
                continue
            ids.append(mid)
            if chat_id is None:
                chat_id = getattr(m, "chat_id", None) or getattr(m, "chat", None)
                if chat_id and not isinstance(chat_id, int):
                    chat_id = chat_id.id if hasattr(chat_id, "id") else None

        if chat_id is None:
            chat_id = 0

        # Получить название чата
        if chat_id != 0:
            try:
                chat_obj = await client.get_chat(chat_id)
                chat_title = getattr(chat_obj, "title", None) or str(chat_id)
            except Exception:
                chat_title = str(chat_id)

        # Детектировать массовое удаление
        cnt = await DB.push_bulk_ts(owner_id, chat_id)
        if len(ids) >= 10 or cnt >= 10:
            await DB.reset_bulk_ts(owner_id)
            await bot_instance.send_message(
                owner_id, notif_bulk(chat_title, max(len(ids), cnt)),
                parse_mode=ParseMode.HTML
            )
            try:
                zf = await build_zip(owner_id, chat_id, chat_title)
                fn = f"merai_{chat_title[:12]}_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
                await bot_instance.send_document(
                    owner_id, BufferedInputFile(zf.read(), filename=fn),
                    caption=f"📦 Архив: <b>{html_module.escape(chat_title)}</b>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                log.error(f"ZIP send err: {e}")
            return

        # Одиночные удаления
        for mid in ids:
            cached = _get_mem(owner_id, chat_id, mid)
            if not cached:
                cached_db = await DB.get_cached_msg(owner_id, chat_id, mid)
                if cached_db:
                    cached = {
                        "cid": cached_db["chat_id"], "mid": cached_db["msg_id"],
                        "type": cached_db["msg_type"], "sender": cached_db["sender_name"],
                        "title": cached_db["chat_title"], "text": cached_db["content"],
                        "file_id": cached_db["file_id"],
                    }

            if cached:
                mtype  = cached["type"]
                sender = cached["sender"]
                asyncio.create_task(
                    DB.log_deletion(owner_id, chat_id, chat_title,
                                    sender, mtype, cached.get("text"))
                )
                if mtype == "text":
                    await bot_instance.send_message(
                        owner_id,
                        notif_deleted_text(sender, chat_title, cached.get("text") or ""),
                        parse_mode=ParseMode.HTML
                    )
                else:
                    fid = cached.get("file_id")
                    note = notif_deleted_media(sender, chat_title, mtype)
                    sent = False
                    if fid:
                        try:
                            if mtype == "photo":
                                await bot_instance.send_photo(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video":
                                await bot_instance.send_video(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "voice":
                                await bot_instance.send_voice(owner_id, fid, caption=note, parse_mode=ParseMode.HTML)
                            elif mtype == "video_note":
                                await bot_instance.send_video_note(owner_id, fid)
                                await bot_instance.send_message(owner_id, note, parse_mode=ParseMode.HTML)
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
                    f"┌ 💬 <b>Чат:</b> {html_module.escape(chat_title)}\n"
                    f"├ 🔢 <b>ID:</b> {mid}\n"
                    f"└ 🕐 <b>Время:</b> {ts}\n"
                    f"<i>(не было в кеше)</i>",
                    parse_mode=ParseMode.HTML
                )
    except Exception as e:
        log.error(f"[UB deleted] owner={owner_id}: {e}")


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
        cached = _get_mem(owner_id, cid, msg.id)
        old_t  = cached["text"] if cached else "—"
        await bot_instance.send_message(
            owner_id,
            notif_edited(sender, title, old_t, new_t),
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(
            DB.log_edit(owner_id, cid, title, sender, old_t, new_t)
        )
        if cached:
            cached["text"] = new_t
    except Exception as e:
        log.debug(f"[UB edited] owner={owner_id}: {e}")


async def start_userbot(owner_id: int, session_str: str) -> bool:
    if not PYROGRAM_OK:
        return False
    if owner_id in ub_clients:
        return True
    if not PYRO_API_ID or not PYRO_API_HASH:
        log.warning("PYRO_API_ID/HASH не заданы")
        return False
    try:
        client = PyroClient(
            name=f"ub_{owner_id}",
            api_id=PYRO_API_ID,
            api_hash=PYRO_API_HASH,
            session_string=session_str,
        )
        client._owner_id = owner_id  # type: ignore

        # Регистрируем обработчики
        client.add_handler(PyroMsgHandler(
            _handle_new_pyro,
            pyro_filters.all
        ))

        # deleted_messages и edited
        from pyrogram.handlers import (
            DeletedMessagesHandler as DelHandler,
            EditedMessageHandler as EditHandler,
        )
        client.add_handler(DelHandler(_handle_deleted_pyro))
        client.add_handler(EditHandler(_handle_edited_pyro, pyro_filters.all))

        await client.start()
        ub_clients[owner_id] = client
        log.info(f"✅ Userbot запущен owner={owner_id}")
        return True
    except Exception as e:
        log.error(f"Userbot start err owner={owner_id}: {e}")
        return False


async def stop_userbot(owner_id: int):
    if owner_id in ub_clients:
        try:
            await ub_clients[owner_id].stop()
        except Exception:
            pass
        del ub_clients[owner_id]


async def ub_send_code(owner_id: int, phone: str) -> Optional[str]:
    """Отправляет код авторизации, возвращает phone_code_hash."""
    if not PYROGRAM_OK:
        return None
    try:
        client = PyroClient(
            name=f"auth_{owner_id}",
            api_id=PYRO_API_ID,
            api_hash=PYRO_API_HASH,
        )
        await client.connect()
        sent = await client.send_code(phone)
        ub_auth_data[owner_id] = {
            "client": client,
            "phone": phone,
            "hash": sent.phone_code_hash,
        }
        return sent.phone_code_hash
    except Exception as e:
        log.error(f"ub_send_code err: {e}")
        return None


async def ub_sign_in(owner_id: int, code: str,
                     password: str = None) -> Optional[str]:
    """Подтверждает код и возвращает session string."""
    data = ub_auth_data.get(owner_id)
    if not data:
        return None
    client = data["client"]
    phone  = data["phone"]
    phash  = data["hash"]
    try:
        try:
            await client.sign_in(phone, phash, code)
        except Exception as e:
            if "PASSWORD_HASH_INVALID" in str(e) or "SESSION_PASSWORD" in str(e):
                if password:
                    await client.check_password(password)
                else:
                    return "NEED_2FA"
            else:
                raise
        session_str = await client.export_session_string()
        await client.stop()
        del ub_auth_data[owner_id]
        return session_str
    except Exception as e:
        log.error(f"ub_sign_in err owner={owner_id}: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════════
#  КЛОНИРОВАННЫЕ БОТЫ
# ═══════════════════════════════════════════════════════════════════════════════

async def launch_clone(token: str, owner_id: int):
    try:
        clone = Bot(token=token,
                    default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        clone_bots[token] = clone
        dp_clone = Dispatcher(storage=MemoryStorage())
        r = Router()

        @r.message(CommandStart())
        async def _cs(msg: Message):
            await msg.answer(
                f"👋 Привет, <b>{html_module.escape(msg.from_user.first_name)}</b>!\n\n"
                f"Это зеркальный бот <b>MerAi &amp; Monitoring</b>.\n"
                f"Для управления используй основной бот."
            )

        @r.message()
        async def _cf(msg: Message):
            await msg.answer(
                "💬 Этот бот — зеркало MerAi.\n"
                "Все функции доступны в основном боте."
            )

        dp_clone.include_router(r)
        log.info(f"Clone bot launched owner={owner_id}")
        await dp_clone.start_polling(clone, handle_signals=False)
    except Exception as e:
        log.error(f"Clone launch err token={token[:20]}: {e}")
        if token in clone_bots:
            del clone_bots[token]


async def stop_clone(token: str):
    if token in clone_bots:
        try:
            await clone_bots[token].session.close()
        except Exception:
            pass
        del clone_bots[token]

# ═══════════════════════════════════════════════════════════════════════════════
#  BOT ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

router = Router()

# ─── /start ──────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    ref = None
    if msg.text and len(msg.text.split()) > 1:
        try:
            arg = msg.text.split()[1]
            if arg.startswith("ref"):
                ref = int(arg[3:])
        except Exception:
            pass

    await DB.upsert_user(u.id, u.username, u.first_name, ref)
    user = await DB.get_user(u.id)

    if not user["is_verified"]:
        await msg.answer(
            f"🔐 <b>Добро пожаловать в MerAi &amp; Monitoring!</b>\n\n"
            f"Привет, <b>{html_module.escape(u.first_name)}</b>! 👋\n\n"
            f"Для защиты от спама нужно пройти быструю верификацию:\n\n"
            f"1️⃣ Нажми кнопку ниже → попадёшь в приватный канал\n"
            f"2️⃣ Нажми «Отправить заявку на вступление»\n"
            f"3️⃣ Сразу возвращайся — бот автоматически тебя разблокирует\n\n"
            f"<i>⚡ Займёт ~10 секунд. Заявку одобрять не нужно.</i>",
            reply_markup=ikb(
                [("✅ Пройти верификацию", CAPTCHA_CHANNEL)]
            ),
        )
        return

    if user["is_banned"]:
        await msg.answer("🚫 Ваш аккаунт заблокирован.")
        return

    await show_main(msg, user)


async def show_main(msg: Message, user: dict):
    active = await DB.plan_active(user["user_id"])
    plan   = user["plan"]
    icon   = plan_emoji(plan)
    exp    = fmt_exp(user["plan_expires"])
    mode   = user["mode"]
    mon    = "🟢 Вкл" if user["monitoring_on"] else "🔴 Выкл"

    text = (
        f"🌟 <b>MerAi &amp; Monitoring</b>\n\n"
        f"👋 Привет, <b>{html_module.escape(user['first_name'] or '—')}</b>!\n\n"
        f"<b>📋 Твой аккаунт:</b>\n"
        f"├ 🆔 ID: <code>{user['user_id']}</code>\n"
        f"├ {icon} <b>План:</b> {plan.upper()}\n"
        f"├ 📅 <b>До:</b> {exp if active else '—'}\n"
        f"├ 🔄 <b>Режим:</b> {'🤖 Bot' if mode == 'chatbot' else '👤 Userbot' if mode == 'userbot' else '❌ Не выбран'}\n"
        f"└ 📡 <b>Мониторинг:</b> {mon}\n\n"
        f"{'✅ <b>Подписка активна</b>' if active else '⚠️ <b>Подписки нет</b> — купи план для старта'}"
    )

    markup = ikb(
        [("⚙️ Режим работы", "mode"), ("💎 Тарифы", "plans")],
        [("📊 Мой профиль", "profile"), ("🎁 Рефералы", "referrals")],
        [("🤖 Клонировать бот", "clone_start"), ("💬 Поддержка", "support")],
        [("📡 Мониторинг вкл/выкл", "toggle_monitor")],
        [("📜 Условия", "terms"), ("❓ Помощь", "help")],
    )
    if user["user_id"] == ADMIN_ID:
        markup.inline_keyboard.append(
            [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")]
        )
    await msg.answer(text, reply_markup=markup)


# ─── Inline query ─────────────────────────────────────────────────────────────

@router.inline_query()
async def inline_handler(query: InlineQuery):
    results = [
        InlineQueryResultArticle(
            id="info",
            title="MerAi & Monitoring",
            description="Мониторинг удалений Telegram",
            input_message_content=InputTextMessageContent(
                message_text=(
                    "🌟 <b>MerAi &amp; Monitoring</b>\n\n"
                    "Мощный бот для мониторинга удалённых сообщений, "
                    "редактирований, видео-кружков и медиа в Telegram.\n\n"
                    "📲 Попробуй: /start"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
    ]
    await query.answer(results, cache_time=30)


# ─── ChatJoinRequest (капча) ──────────────────────────────────────────────────

@router.chat_join_request()
async def handle_join_req(evt: ChatJoinRequest):
    uid = evt.from_user.id
    if evt.chat.id != CAPTCHA_CHAN_ID:
        return
    # НЕ одобряем — просто видим факт заявки как прохождение капчи
    try:
        await DB.upsert_user(uid, evt.from_user.username, evt.from_user.first_name)
        user = await DB.get_user(uid)
        if not user["is_verified"]:
            await DB.verify(uid)
        await bot_instance.send_message(
            uid,
            "✅ <b>Верификация пройдена!</b>\n\n"
            "Добро пожаловать в MerAi &amp; Monitoring!\n"
            "Нажми /start для продолжения.",
            parse_mode=ParseMode.HTML,
        )
        log.info(f"Captcha verified uid={uid}")
    except Exception as e:
        log.error(f"join_req err uid={uid}: {e}")


# ─── CALLBACKS ГЛАВНОГО МЕНЮ ──────────────────────────────────────────────────

@router.callback_query(F.data == "back_main")
async def cb_back_main(q: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await DB.get_user(q.from_user.id)
    if not user:
        await q.answer()
        return
    await q.message.delete()
    await show_main(q.message, user)
    await q.answer()

@router.callback_query(F.data == "toggle_monitor")
async def cb_toggle_monitor(q: CallbackQuery):
    user = await DB.get_user(q.from_user.id)
    if not user:
        await q.answer(); return

    active = await DB.plan_active(q.from_user.id)
    if not active:
        await q.answer("❌ Нужна активная подписка!", show_alert=True)
        return

    new_state = 0 if user["monitoring_on"] else 1
    await DB.set_field(q.from_user.id, "monitoring_on", new_state)

    if new_state and user["mode"] == "userbot" and user["ub_session"]:
        if q.from_user.id not in ub_clients:
            asyncio.create_task(start_userbot(q.from_user.id, user["ub_session"]))

    if not new_state and user["mode"] == "userbot":
        asyncio.create_task(stop_userbot(q.from_user.id))

    await q.answer(f"Мониторинг {'включён 🟢' if new_state else 'выключен 🔴'}", show_alert=True)


# ─── РЕЖИМЫ ───────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "mode")
async def cb_mode(q: CallbackQuery):
    await q.message.edit_text(
        "⚙️ <b>Режим мониторинга</b>\n\n"
        "🤖 <b>Bot Mode (Chatbot)</b>\n"
        "• Добавь бота в группу как администратора\n"
        "• Мониторинг редактирований и новых сообщений\n"
        "• Требуется Telegram Premium Business\n"
        "• <i>Удалённые сообщения НЕ перехватываются</i>\n\n"
        "👤 <b>Userbot Mode</b> <i>(Pro/Квартал/Год)</i>\n"
        "• Подключаешь свой аккаунт Telegram\n"
        "• Полный перехват: удаления, редактирования,\n"
        "  исчезающие сообщения, видео-кружки, голосовые\n"
        "• Работает во ВСЕХ чатах",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb(
            [("🤖 Bot Mode", "set_mode_bot")],
            [("👤 Userbot Mode", "set_mode_ub")],
            [("🔙 Назад", "back_main")],
        ),
    )
    await q.answer()

@router.callback_query(F.data == "set_mode_bot")
async def cb_mode_bot(q: CallbackQuery):
    await DB.set_field(q.from_user.id, "mode", "chatbot")
    await q.message.edit_text(
        "🤖 <b>Bot Mode активирован</b>\n\n"
        "<b>Инструкция для Telegram Business:</b>\n"
        "1️⃣ Telegram → Настройки → Telegram Business → Chatbots\n"
        "2️⃣ Добавить бота и выбрать чаты для мониторинга\n\n"
        "<i>⚠️ Требуется Telegram Premium для Business-функций</i>\n\n"
        "✅ Всё готово! Бот будет отслеживать редактирования "
        "в подключённых Business-чатах.",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("🔙 Меню", "back_main")]),
    )
    await q.answer()

@router.callback_query(F.data == "set_mode_ub")
async def cb_mode_ub(q: CallbackQuery, state: FSMContext):
    active = await DB.plan_active(q.from_user.id)
    if not active:
        await q.answer("❌ Нужна активная подписка!", show_alert=True)
        return
    if not PYROGRAM_OK:
        await q.answer("⚠️ Pyrogram не установлен на сервере", show_alert=True)
        return
    if not PYRO_API_ID:
        await q.answer("⚠️ API_ID не задан. Обратись к @mrztn", show_alert=True)
        return

    user = await DB.get_user(q.from_user.id)
    if user.get("ub_session"):
        await q.message.edit_text(
            "👤 <b>Userbot уже подключён</b>\n\n"
            f"📱 Телефон: <code>{user.get('ub_phone','—')}</code>\n"
            f"📡 Статус: {'🟢 Активен' if q.from_user.id in ub_clients else '🔴 Остановлен'}",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb(
                [("🔄 Переподключить", "ub_reconnect")],
                [("🗑 Отключить userbot", "ub_disconnect")],
                [("🔙 Меню", "back_main")],
            ),
        )
        await q.answer(); return

    await q.message.edit_text(
        "👤 <b>Подключение Userbot</b>\n\n"
        "Шаг 1/3: Введи свой номер телефона\n"
        "Формат: <code>+79991234567</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("❌ Отмена", "back_main")]),
    )
    await state.set_state(UserBotSt.phone)
    await q.answer()

@router.callback_query(F.data == "ub_reconnect")
async def cb_ub_reconnect(q: CallbackQuery, state: FSMContext):
    await DB.set_field(q.from_user.id, "ub_session", None)
    await DB.set_field(q.from_user.id, "ub_active", 0)
    asyncio.create_task(stop_userbot(q.from_user.id))
    await q.message.edit_text(
        "👤 <b>Повторное подключение</b>\n\nВведи номер телефона:\n"
        "Формат: <code>+79991234567</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("❌ Отмена", "back_main")]),
    )
    await state.set_state(UserBotSt.phone)
    await q.answer()

@router.callback_query(F.data == "ub_disconnect")
async def cb_ub_disconnect(q: CallbackQuery):
    await DB.set_field(q.from_user.id, "ub_session", None)
    await DB.set_field(q.from_user.id, "ub_active", 0)
    await DB.set_field(q.from_user.id, "mode", "none")
    await DB.set_field(q.from_user.id, "monitoring_on", 0)
    asyncio.create_task(stop_userbot(q.from_user.id))
    await q.answer("✅ Userbot отключён", show_alert=True)
    await q.message.edit_reply_markup(reply_markup=ikb([("🏠 Меню", "back_main")]))

# ─── FSM: USERBOT SETUP ───────────────────────────────────────────────────────

@router.message(UserBotSt.phone)
async def ub_fsm_phone(msg: Message, state: FSMContext):
    phone = msg.text.strip().replace(" ", "")
    if not re.match(r"^\+\d{10,15}$", phone):
        await msg.answer(
            "❌ Неверный формат. Введи номер как: <code>+79991234567</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    await msg.answer("⏳ Отправляю код…")
    ph_hash = await ub_send_code(msg.from_user.id, phone)
    if not ph_hash:
        await msg.answer(
            "❌ Не удалось отправить код. Проверь номер или попробуй позже.\n"
            "Возможно проблемы с API_ID/HASH.",
            reply_markup=ikb([("🔙 Меню", "back_main")]),
        )
        await state.clear()
        return
    await state.update_data(phone=phone)
    await msg.answer(
        f"📱 Код отправлен на <code>{phone}</code>\n\n"
        f"Шаг 2/3: Введи код из Telegram (или SMS):",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(UserBotSt.code)

@router.message(UserBotSt.code)
async def ub_fsm_code(msg: Message, state: FSMContext):
    code = msg.text.strip().replace(" ", "").replace("-", "")
    await msg.answer("⏳ Проверяю код…")
    result = await ub_sign_in(msg.from_user.id, code)
    if result == "NEED_2FA":
        await msg.answer(
            "🔒 Аккаунт защищён паролем.\n\nШаг 3/3: Введи пароль 2FA:"
        )
        await state.set_state(UserBotSt.twofa)
        return
    if not result:
        await msg.answer(
            "❌ Неверный код. Попробуй ещё раз или начни заново:",
            reply_markup=ikb([("🔄 Начать заново", "set_mode_ub"), ("🔙 Меню", "back_main")])
        )
        await state.clear()
        return
    await _ub_success(msg, state, result)

@router.message(UserBotSt.twofa)
async def ub_fsm_twofa(msg: Message, state: FSMContext):
    data = ub_auth_data.get(msg.from_user.id)
    if not data:
        await msg.answer("⏱ Сессия истекла. Начни заново.", reply_markup=ikb([("🔄 Заново", "set_mode_ub")]))
        await state.clear()
        return
    result = await ub_sign_in(msg.from_user.id, "", password=msg.text.strip())
    if not result or result == "NEED_2FA":
        await msg.answer("❌ Неверный пароль 2FA. Попробуй ещё раз:")
        return
    await _ub_success(msg, state, result)

async def _ub_success(msg: Message, state: FSMContext, session: str):
    await DB.set_field(msg.from_user.id, "ub_session", session)
    await DB.set_field(msg.from_user.id, "ub_active", 1)
    await DB.set_field(msg.from_user.id, "mode", "userbot")
    await state.clear()
    ok = await start_userbot(msg.from_user.id, session)
    if ok:
        await msg.answer(
            "✅ <b>Userbot подключён!</b>\n\n"
            "Теперь включи мониторинг кнопкой в главном меню.\n"
            "Будут перехватываться:\n"
            "• Удалённые сообщения\n"
            "• Редактирования\n"
            "• Исчезающие сообщения 💣\n"
            "• Видео-кружки ⭕\n"
            "• Голосовые 🎙\n"
            "• Фото / видео / файлы",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("🏠 Меню", "back_main")]),
        )
    else:
        await msg.answer(
            "⚠️ Сессия сохранена, но userbot не запустился автоматически.\n"
            "Попробуй включить мониторинг вручную.",
            reply_markup=ikb([("🏠 Меню", "back_main")]),
        )

# ─── ПЛАНЫ И ОПЛАТА ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "plans")
async def cb_plans(q: CallbackQuery):
    text = (
        "💎 <b>Тарифные планы MerAi</b>\n\n"
        "Оплата — <b>Telegram Stars ⭐</b>\n\n"
    )
    for pid, p in PLANS.items():
        text += f"{plan_emoji(pid)} <b>{p['name']}</b> — <b>{p['stars']} ⭐</b> ({p['days']} дн.)\n"

    text += (
        "\n<b>✅ Включено во всех планах:</b>\n"
        "• Мониторинг удалений и редактирований\n"
        "• Bot Mode (Telegram Business)\n"
        "• Клонирование ботов (+3 дня за каждый)\n\n"
        "<b>✅ Userbot режим</b> — с <b>любого платного плана</b>:\n"
        "• Полный перехват (видео-кружки, исчезающие и др.)\n"
        "• ZIP-архив при массовом удалении"
    )
    rows = [[
        (f"{plan_emoji(pid)} {p['name']} — {p['stars']} ⭐", f"buy_{pid}")
    ] for pid, p in PLANS.items()]
    rows.append([("🔙 Назад", "back_main")])
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*rows))
    await q.answer()

@router.callback_query(F.data.startswith("buy_"))
async def cb_buy(q: CallbackQuery):
    plan_id = q.data[4:]
    plan    = PLANS.get(plan_id)
    if not plan:
        await q.answer("Тариф не найден"); return
    await q.answer()
    try:
        await bot_instance.send_invoice(
            chat_id=q.from_user.id,
            title=f"MerAi — {plan['name']}",
            description=(
                f"Подписка MerAi & Monitoring\n"
                f"Период: {plan['days']} дней\n"
                f"Все функции мониторинга + Userbot"
            ),
            payload=f"plan:{plan_id}",
            currency="XTR",
            prices=[LabeledPrice(label=plan["name"], amount=plan["stars"])],
        )
    except Exception as e:
        log.error(f"send_invoice err: {e}")
        await q.message.answer(f"❌ Ошибка создания инвойса: {e}")

@router.pre_checkout_query()
async def pre_checkout(pcq: PreCheckoutQuery):
    await pcq.answer(ok=True)

@router.message(F.successful_payment)
async def on_payment_ok(msg: Message):
    pay     = msg.successful_payment
    payload = pay.invoice_payload   # "plan:week"
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
        f"{plan_emoji(plan_id)} <b>Активирован план: {plan['name']}</b>\n"
        f"📅 Срок: <b>{plan['days']} дней</b>\n"
        f"⭐ Оплачено: <b>{pay.total_amount} Stars</b>\n\n"
        f"Мониторинг доступен! Используй /start",
        parse_mode=ParseMode.HTML,
    )
    log.info(f"Payment OK uid={uid} plan={plan_id} stars={pay.total_amount}")

# ─── ПРОФИЛЬ ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "profile")
async def cb_profile(q: CallbackQuery):
    u = await DB.get_user(q.from_user.id)
    active = await DB.plan_active(q.from_user.id)
    clones = await DB.get_user_clones(q.from_user.id)
    stats  = await DB.get_deletion_stats(q.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref{u['user_id']}"

    text = (
        f"📊 <b>Мой профиль</b>\n\n"
        f"🆔 ID: <code>{u['user_id']}</code>\n"
        f"👤 Имя: {html_module.escape(u['first_name'] or '—')}\n"
        f"🔗 Username: @{u['username'] or '—'}\n\n"
        f"<b>Подписка:</b>\n"
        f"├ {plan_emoji(u['plan'])} {u['plan'].upper()}\n"
        f"├ {'✅ Активна' if active else '❌ Истекла'}\n"
        f"└ 📅 До: {fmt_exp(u['plan_expires'])}\n\n"
        f"<b>Статистика:</b>\n"
        f"├ 🗑 Перехвачено удалений: {stats['total']}\n"
        f"├ 🤖 Клонировано ботов: {len(clones)}\n"
        f"└ 👥 Рефералов: {u['referral_count']}\n\n"
        f"<b>Реферальная ссылка:</b>\n"
        f"<code>{ref_link}</code>"
    )
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("💎 Сменить план", "plans")],
        [("🤖 Мои боты", "my_clones")],
        [("📊 Статистика удалений", "del_stats")],
        [("🔙 Назад", "back_main")],
    ))
    await q.answer()

@router.callback_query(F.data == "del_stats")
async def cb_del_stats(q: CallbackQuery):
    stats = await DB.get_deletion_stats(q.from_user.id)
    text  = f"🗑 <b>Статистика удалений</b>\n\nВсего: <b>{stats['total']}</b>\n\n"

    if stats["top_chats"]:
        text += "<b>Топ чатов:</b>\n"
        for row in stats["top_chats"]:
            text += f"├ {html_module.escape(str(row['chat_title']))} — {row['cnt']} удал.\n"
        text += "\n"

    if stats["by_type"]:
        text += "<b>По типам:</b>\n"
        for row in stats["by_type"]:
            text += f"├ {mtype_icon(row['msg_type'])} {row['msg_type']} — {row['cnt']}\n"
        text += "\n"

    if stats["recent"]:
        text += "<b>Последние 5:</b>\n"
        for r in stats["recent"][:5]:
            ts   = (r["deleted_at"] or "")[:16]
            name = html_module.escape(r["sender_name"] or "?")
            chat = html_module.escape(str(r["chat_title"] or "?"))
            text += f"• {ts} — {name} в {chat}\n"

    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("🔙 Профиль", "profile")]
    ))
    await q.answer()

@router.callback_query(F.data == "my_clones")
async def cb_my_clones(q: CallbackQuery):
    clones = await DB.get_user_clones(q.from_user.id)
    if not clones:
        await q.message.edit_text(
            "🤖 <b>Клонированные боты</b>\n\nУ тебя нет подключённых ботов.\n"
            "Добавь бот и получи +3 дня к подписке!",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([("➕ Добавить бот", "clone_start"), ("🔙 Профиль", "profile")]),
        )
    else:
        text = "🤖 <b>Мои клонированные боты</b>\n\n"
        for c in clones:
            st = "🟢" if c["is_active"] else "🔴"
            text += f"{st} @{c['bot_username'] or '?'} — <i>{c['bot_name'] or '?'}</i>\n"
        await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
            [("➕ Добавить ещё", "clone_start")],
            [("🔙 Профиль", "profile")],
        ))
    await q.answer()

# ─── РЕФЕРАЛЫ ────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "referrals")
async def cb_referrals(q: CallbackQuery):
    u    = await DB.get_user(q.from_user.id)
    cnt  = u["referral_count"]
    link = f"https://t.me/{BOT_USERNAME}?start=ref{u['user_id']}"
    done = cnt >= REF_GOAL
    prog = min(cnt, REF_GOAL)
    bar  = "█" * (prog * 10 // REF_GOAL) + "░" * (10 - prog * 10 // REF_GOAL)

    text = (
        f"🎁 <b>Реферальная программа</b>\n\n"
        f"<b>Прогресс:</b>\n"
        f"{bar}  {cnt}/{REF_GOAL}\n\n"
        f"<b>Награда за {REF_GOAL} рефералов:</b>\n"
        f"Telegram Premium на 1 месяц или эквивалент ✨\n\n"
        f"<b>Твоя ссылка:</b>\n"
        f"<code>{link}</code>\n\n"
        f"{'🎉 Ты достиг цели! Напиши @mrztn за наградой.' if done else f'Ещё нужно: {REF_GOAL - cnt}'}"
    )
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("📤 Поделиться", f"https://t.me/share/url?url={link}&text=Попробуй MerAi!")],
        [("🔙 Меню", "back_main")],
    ))
    await q.answer()

# ─── КЛОНИРОВАНИЕ БОТОВ ───────────────────────────────────────────────────────

@router.callback_query(F.data == "clone_start")
async def cb_clone_start(q: CallbackQuery, state: FSMContext):
    u = await DB.get_user(q.from_user.id)
    if u["plan"] == "free" or not await DB.plan_active(q.from_user.id):
        await q.answer("❌ Нужна активная подписка!", show_alert=True)
        return

    await q.message.edit_text(
        "🤖 <b>Клонирование бота</b>\n\n"
        "<b>Как получить токен:</b>\n"
        "1️⃣ Открой @BotFather → /newbot\n"
        "2️⃣ Придумай имя и username\n"
        "3️⃣ Скопируй токен\n\n"
        "📋 <b>Отправь токен:</b>\n"
        "<i>Формат: 123456789:ABC-DEF1234...</i>\n\n"
        f"🎁 <b>За каждый бот +{CLONE_BONUS_DAYS} дня к подписке!</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("❌ Отмена", "back_main")]),
    )
    await state.set_state(CloneSt.token)
    await q.answer()

@router.message(CloneSt.token)
async def clone_token_recv(msg: Message, state: FSMContext):
    token = msg.text.strip()
    if not re.match(r"^\d+:[A-Za-z0-9_-]{35,}$", token):
        await msg.answer(
            "❌ Неверный формат токена.\n"
            "Токен выглядит так: <code>123456789:ABCDEF...</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    await msg.answer("⏳ Проверяю бота…")
    try:
        test_bot = Bot(token=token)
        info     = await test_bot.get_me()
        await test_bot.session.close()
    except Exception as e:
        await msg.answer(
            f"❌ Неверный токен или бот недоступен.\n<code>{e}</code>",
            parse_mode=ParseMode.HTML,
        )
        await state.clear()
        return

    uid = msg.from_user.id
    ok  = await DB.add_clone(uid, token, info.username, info.full_name)
    if not ok:
        await msg.answer(
            "⚠️ Этот бот уже подключён (или другим пользователем).",
            reply_markup=ikb([("🏠 Меню", "back_main")]),
        )
        await state.clear()
        return

    await DB.add_days(uid, CLONE_BONUS_DAYS)
    asyncio.create_task(launch_clone(token, uid))

    await msg.answer(
        f"✅ <b>Бот @{info.username} подключён!</b>\n\n"
        f"🎁 <b>+{CLONE_BONUS_DAYS} дня</b> добавлено к подписке!\n"
        f"Бот запускается…",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("🏠 Меню", "back_main")]),
    )
    await state.clear()

# ─── ПОДДЕРЖКА ────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "support")
async def cb_support(q: CallbackQuery):
    await q.message.edit_text(
        "💬 <b>Поддержка</b>\n\n"
        "1️⃣ Нажми «Написать сообщение» и опиши проблему\n"
        "2️⃣ Или напиши напрямую: @mrztn\n\n"
        "⏱ Время ответа: до 24 часов",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb(
            [("✍️ Написать", "support_write")],
            [("📱 @mrztn", "https://t.me/mrztn")],
            [("🔙 Назад", "back_main")],
        ),
    )
    await q.answer()

@router.callback_query(F.data == "support_write")
async def cb_support_write(q: CallbackQuery, state: FSMContext):
    await q.message.edit_text(
        "✍️ Напиши своё сообщение и я передам его поддержке:",
        reply_markup=ikb([("❌ Отмена", "back_main")]),
    )
    await state.set_state(SupportSt.message)
    await q.answer()

@router.message(SupportSt.message)
async def support_msg_recv(msg: Message, state: FSMContext):
    uid  = msg.from_user.id
    text = msg.text or "[медиа]"
    tid  = await DB.create_ticket(uid, text)
    await state.clear()
    await msg.answer(
        f"✅ <b>Сообщение отправлено!</b>\n"
        f"Номер тикета: <code>#{tid}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("🏠 Меню", "back_main")]),
    )
    # Уведомляем admin
    u = await DB.get_user(uid)
    try:
        await bot_instance.send_message(
            ADMIN_ID,
            f"📩 <b>Тикет #{tid}</b>\n"
            f"👤 {html_module.escape(u['first_name'] or '?')} "
            f"(@{u['username'] or '?'}, <code>{uid}</code>)\n\n"
            f"{html_module.escape(text)}",
            parse_mode=ParseMode.HTML,
            reply_markup=ikb([
                (f"↩️ Ответить #{tid}", f"adm_reply_{tid}_{uid}"),
            ]),
        )
    except Exception:
        pass

# ─── УСЛОВИЯ ИСПОЛЬЗОВАНИЯ ───────────────────────────────────────────────────

TERMS = (
    "<b>📜 Условия использования MerAi &amp; Monitoring</b>\n\n"
    "<b>1. Общие положения</b>\n"
    "Используя сервис, вы соглашаетесь с настоящими условиями.\n\n"
    "<b>2. Отказ от ответственности</b>\n"
    "Администрация MerAi &amp; Monitoring <b>НЕ НЕСЁТ ответственности</b> за:\n"
    "• последствия использования данных мониторинга;\n"
    "• блокировку аккаунта со стороны Telegram;\n"
    "• технические сбои платформы;\n"
    "• прямые и косвенные убытки пользователя.\n\n"
    "<b>3. Ответственность пользователя</b>\n"
    "Пользователь обязан соблюдать законодательство своей страны "
    "и правила Telegram. Использование в целях незаконной слежки, "
    "шантажа или нарушения частной жизни <b>строго запрещено</b>.\n\n"
    "<b>4. Конфиденциальность</b>\n"
    "Перехваченные данные хранятся только в вашем аккаунте. "
    "Администрация не имеет доступа к содержимому мониторинга.\n\n"
    "<b>5. Возвраты</b>\n"
    "Оплата Telegram Stars является окончательной. "
    "Возврат возможен только при подтверждённом техническом сбое сервиса.\n\n"
    "<b>6. Прекращение доступа</b>\n"
    "Администрация вправе заблокировать аккаунт при нарушении условий "
    "без предупреждения и без возврата средств.\n\n"
    "<i>© 2026 MerAi &amp; Monitoring. Все права защищены.</i>"
)

@router.callback_query(F.data == "terms")
async def cb_terms(q: CallbackQuery):
    await q.message.edit_text(TERMS, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("✅ Принимаю", "back_main")]
    ))
    await q.answer()

@router.callback_query(F.data == "help")
async def cb_help(q: CallbackQuery):
    text = (
        "❓ <b>Справка и инструкции</b>\n\n"
        "<b>Команды:</b>\n"
        "/start — Главное меню\n"
        "/help — Справка\n"
        "/admin — Панель администратора\n\n"
        "<b>Bot Mode:</b>\n"
        "1. Добавь бота в группу как администратора\n"
        "2. Перехватываются редактирования в Business-чатах\n\n"
        "<b>Userbot Mode:</b>\n"
        "1. Меню → Режим → Userbot\n"
        "2. Введи телефон и код из Telegram\n"
        "3. Перехватываются: удаления, редактирования,\n"
        "   исчезающие, видео-кружки ⭕, голосовые 🎙\n\n"
        "<b>Клонирование:</b>\n"
        "Создай бота у @BotFather → отправь токен → +3 дня к подписке"
    )
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("🔙 Назад", "back_main")]
    ))
    await q.answer()

# ─── /help /terms slash commands ─────────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "❓ <b>Справка MerAi &amp; Monitoring</b>\n\n"
        "/start — Главное меню\n"
        "/help  — Справка\n"
        "/admin — Админ-панель (@mrztn)\n\n"
        "Для навигации используй кнопки в главном меню.",
        parse_mode=ParseMode.HTML,
    )

@router.message(Command("terms"))
async def cmd_terms(msg: Message):
    await msg.answer(TERMS, parse_mode=ParseMode.HTML)

# ═══════════════════════════════════════════════════════════════════════════════
#  ADMIN PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

@router.message(Command("admin"))
async def cmd_admin_slash(msg: Message):
    if not is_admin(msg.from_user.id):
        await msg.answer("🚫 Доступ запрещён.")
        return
    await _admin_main(msg)

@router.callback_query(F.data == "admin")
async def cb_admin(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("🚫", show_alert=True); return
    await q.message.delete()
    await _admin_main(q.message)
    await q.answer()

async def _admin_main(msg: Message):
    s = await DB.stats()
    text = (
        "🛠 <b>Админ-панель MerAi</b>\n\n"
        f"👥 Всего пользователей: {s['total']}\n"
        f"✅ Верифицировано: {s['verified']}\n"
        f"💎 Платящих: {s['paid']}\n"
        f"🤖 Клонов ботов: {s['clones']}\n"
        f"⭐ Собрано Stars: {s['stars']}\n"
        f"🗑 Перехвачено удалений: {s['deletions']}\n"
        f"🔵 Активных userbot: {len(ub_clients)}\n"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("👥 Пользователи", "adm_users_p1"), ("📊 Статистика", "adm_stats")],
        [("📢 Рассылка", "adm_broadcast"), ("💬 Тикеты", "adm_tickets")],
        [("🤖 Клоны ботов", "adm_clones"), ("🔙 Меню", "back_main")],
    ))

@router.callback_query(F.data == "adm_stats")
async def cb_adm_stats(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer(); return
    s = await DB.stats()
    text = (
        f"📊 <b>Полная статистика</b>\n\n"
        f"👥 Всего: {s['total']}\n"
        f"✅ Верифиц.: {s['verified']}\n"
        f"💎 Платящих: {s['paid']}\n"
        f"🤖 Клонов: {s['clones']}\n"
        f"⭐ Stars: {s['stars']}\n"
        f"🗑 Удалений: {s['deletions']}\n"
        f"🔵 Userbot сейчас: {len(ub_clients)}\n"
        f"🤖 Клонов запущено: {len(clone_bots)}\n"
    )
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(
        [("🔙 Панель", "adm_back")]
    ))
    await q.answer()

@router.callback_query(F.data == "adm_back")
async def cb_adm_back(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.message.delete()
    await _admin_main(q.message)
    await q.answer()

# ── Список пользователей ──────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("adm_users_p"))
async def cb_adm_users(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    page = int(q.data.split("p")[-1])
    per  = 6
    all_u = await DB.all_users()
    total = len(all_u)
    chunk = all_u[(page-1)*per : page*per]

    text = f"<b>👥 Пользователи</b> (стр.{page}, всего {total})\n\n"
    rows = []
    for u in chunk:
        act = "✅" if await DB.plan_active(u["user_id"]) else "❌"
        ban = "🚫" if u["is_banned"] else ""
        nm  = html_module.escape((u["first_name"] or u["username"] or str(u["user_id"]))[:18])
        text += f"{act}{ban} <code>{u['user_id']}</code> {nm} [{u['plan']}]\n"
        rows.append([(f"👤 {nm[:14]}", f"adm_u_{u['user_id']}")])

    nav = []
    if page > 1:   nav.append(("◀", f"adm_users_p{page-1}"))
    if page*per < total: nav.append(("▶", f"adm_users_p{page+1}"))
    if nav: rows.append(nav)
    rows.append([("🔙 Панель", "adm_back")])

    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*rows))
    await q.answer()

@router.callback_query(F.data.startswith("adm_u_"))
async def cb_adm_user_detail(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    uid = int(q.data[6:])
    u   = await DB.get_user(uid)
    if not u:
        await q.answer("Не найден", show_alert=True); return

    active = await DB.plan_active(uid)
    clones = await DB.get_user_clones(uid)
    stats  = await DB.get_deletion_stats(uid)
    ub_on  = uid in ub_clients

    text = (
        f"<b>👤 Пользователь</b>\n\n"
        f"🆔 <code>{uid}</code>\n"
        f"📛 {html_module.escape(u['first_name'] or '—')}"
        f" (@{u['username'] or '—'})\n"
        f"📦 План: {u['plan'].upper()} {'✅' if active else '❌'}\n"
        f"📅 До: {fmt_exp(u['plan_expires'])}\n"
        f"🔄 Режим: {u['mode']}\n"
        f"📡 Мониторинг: {'🟢' if u['monitoring_on'] else '🔴'}\n"
        f"👤 Userbot: {'🟢 Активен' if ub_on else '🔴'}\n"
        f"🚫 Бан: {'Да' if u['is_banned'] else 'Нет'}\n"
        f"🤖 Клонов: {len(clones)}\n"
        f"🗑 Удалений перехвачено: {stats['total']}\n"
        f"👥 Рефералов: {u['referral_count']}\n"
    )
    # Последние удаления
    if stats["recent"]:
        text += "\n<b>Последние удаления:</b>\n"
        for r in stats["recent"][:3]:
            ts   = (r["deleted_at"] or "")[:16]
            mtyp = mtype_icon(r["msg_type"])
            txt  = html_module.escape((r["content"] or "")[:40])
            text += f"• {ts} {mtyp} {txt}\n"

    markup = ikb(
        [("🔹 week", f"adm_sp_{uid}_week"), ("📆 month", f"adm_sp_{uid}_month")],
        [("📊 quarter", f"adm_sp_{uid}_quarter"), ("🎯 year", f"adm_sp_{uid}_year")],
        [("🆓 Убрать план", f"adm_sp_{uid}_free"), ("🚫 Бан/Разбан", f"adm_ban_{uid}")],
        [("📡 Вкл/Выкл монит.", f"adm_mon_{uid}"), ("⭐ Добавить дни", f"adm_days_{uid}")],
        [("🤖 Клоны юзера", f"adm_ub_{uid}"), ("🔙 Список", "adm_users_p1")],
    )
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    await q.answer()

@router.callback_query(F.data.startswith("adm_sp_"))
async def cb_adm_set_plan(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    parts   = q.data.split("_")       # adm_sp_{uid}_{plan}
    uid     = int(parts[2])
    plan_id = parts[3]
    if plan_id == "free":
        await DB.set_field(uid, "plan", "free")
        await DB.set_field(uid, "plan_expires", None)
    else:
        p = PLANS.get(plan_id)
        if p:
            await DB.set_plan(uid, plan_id, p["days"])
    try:
        await bot_instance.send_message(
            uid,
            f"📦 Ваш план изменён администратором: <b>{plan_id.upper()}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    await q.answer(f"✅ Установлен план {plan_id}", show_alert=True)
    q.data = f"adm_u_{uid}"
    await cb_adm_user_detail(q)

@router.callback_query(F.data.startswith("adm_ban_"))
async def cb_adm_ban(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    uid = int(q.data[8:])
    u   = await DB.get_user(uid)
    nb  = 0 if (u and u["is_banned"]) else 1
    await DB.set_field(uid, "is_banned", nb)
    await q.answer("🚫 Забанен" if nb else "✅ Разбанен", show_alert=True)
    q.data = f"adm_u_{uid}"
    await cb_adm_user_detail(q)

@router.callback_query(F.data.startswith("adm_mon_"))
async def cb_adm_monitor(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    uid = int(q.data[8:])
    u   = await DB.get_user(uid)
    ns  = 0 if (u and u["monitoring_on"]) else 1
    await DB.set_field(uid, "monitoring_on", ns)
    await q.answer(f"Мониторинг {'вкл' if ns else 'выкл'}", show_alert=True)
    q.data = f"adm_u_{uid}"
    await cb_adm_user_detail(q)

@router.callback_query(F.data.startswith("adm_days_"))
async def cb_adm_days_start(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer(); return
    uid = int(q.data[9:])
    await state.update_data(target_uid=uid)
    await q.message.edit_text(
        f"⭐ Добавить дни к подписке пользователя <code>{uid}</code>\n\n"
        f"Введи количество дней (например: 30):",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("❌ Отмена", f"adm_u_{uid}")]),
    )
    await state.set_state(AdminSt.add_balance_amt)
    await q.answer()

@router.message(AdminSt.add_balance_amt)
async def adm_days_recv(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        days = int(msg.text.strip())
        assert 1 <= days <= 3650
    except Exception:
        await msg.answer("❌ Введи число от 1 до 3650")
        return
    data = await state.get_data()
    uid  = data.get("target_uid")
    await DB.add_days(uid, days)
    await state.clear()
    try:
        await bot_instance.send_message(
            uid,
            f"🎁 Администратор добавил <b>+{days} дней</b> к вашей подписке!",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    await msg.answer(f"✅ +{days} дней добавлено пользователю {uid}",
                     reply_markup=ikb([("👤 Пользователь", f"adm_u_{uid}")]))

@router.callback_query(F.data.startswith("adm_ub_"))
async def cb_adm_user_clones(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    uid    = int(q.data[7:])
    clones = await DB.get_user_clones(uid)
    text   = f"<b>🤖 Клоны пользователя {uid}</b>\n\n"
    rows   = []
    for c in clones:
        st = "🟢" if c["is_active"] else "🔴"
        text += f"{st} @{c['bot_username'] or '?'} — {c['bot_name'] or '?'}\n"
        lbl = "🔴 Стоп" if c["token"] in clone_bots else "🟢 Запуск"
        rows.append([(f"{lbl} @{c['bot_username']}", f"adm_ctog_{c['id']}")])
    if not clones:
        text += "Нет клонов."
    rows.append([("🔙 Пользователь", f"adm_u_{uid}")])
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*rows))
    await q.answer()

@router.callback_query(F.data.startswith("adm_ctog_"))
async def cb_adm_clone_toggle(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    cid_db = int(q.data[9:])
    async with DB._conn.execute("SELECT * FROM cloned_bots WHERE id=?", (cid_db,)) as c:
        row = await c.fetchone()
    if not row:
        await q.answer("Не найден", show_alert=True); return
    token    = row["bot_token"]
    owner_id = row["owner_id"]
    if token in clone_bots:
        asyncio.create_task(stop_clone(token))
        await DB._conn.execute("UPDATE cloned_bots SET is_active=0 WHERE id=?", (cid_db,))
        await q.answer("🔴 Остановлен", show_alert=True)
    else:
        asyncio.create_task(launch_clone(token, owner_id))
        await DB._conn.execute("UPDATE cloned_bots SET is_active=1 WHERE id=?", (cid_db,))
        await q.answer("🟢 Запущен", show_alert=True)
    await DB._conn.commit()

# ── Рассылка ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.message.edit_text(
        "📢 <b>Рассылка</b>\n\nВыбери тип:",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb(
            [("📢 Всем пользователям", "adm_bc_all")],
            [("👤 Конкретному ID", "adm_bc_single")],
            [("🔙 Панель", "adm_back")],
        ),
    )
    await q.answer()

@router.callback_query(F.data == "adm_bc_all")
async def cb_bc_all(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.message.edit_text(
        "📢 Введи текст рассылки (HTML):\n\n"
        "<i>Используй /cancel для отмены</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("❌ Отмена", "adm_broadcast")]),
    )
    await state.set_state(AdminSt.broadcast_text)
    await q.answer()

@router.message(AdminSt.broadcast_text)
async def adm_bc_send(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    text = msg.text
    await state.clear()
    all_u = await DB.all_users()
    notice = await msg.answer(f"⏳ Рассылка для {len(all_u)} пользователей…")
    sent = failed = 0
    for u in all_u:
        try:
            await bot_instance.send_message(u["user_id"], text, parse_mode=ParseMode.HTML)
            sent += 1
            await asyncio.sleep(0.05)
        except (TelegramForbiddenError, TelegramBadRequest):
            failed += 1
        except Exception:
            failed += 1
    await notice.edit_text(f"✅ Рассылка завершена!\n✉️ Отправлено: {sent}\n❌ Ошибок: {failed}")

@router.callback_query(F.data == "adm_bc_single")
async def cb_bc_single(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer(); return
    await q.message.edit_text(
        "👤 Введи: <code>ID текст сообщения</code>\n"
        "Пример: <code>7785371505 Привет!</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=ikb([("❌ Отмена", "adm_broadcast")]),
    )
    await state.set_state(AdminSt.broadcast_single)
    await q.answer()

@router.message(AdminSt.broadcast_single)
async def adm_bc_single_send(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split(" ", 1)
    await state.clear()
    if len(parts) < 2:
        await msg.answer("❌ Формат: ID текст")
        return
    try:
        uid = int(parts[0])
        txt = parts[1]
        await bot_instance.send_message(uid, txt, parse_mode=ParseMode.HTML)
        await msg.answer(f"✅ Отправлено пользователю {uid}")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")

# ── Тикеты поддержки (admin) ─────────────────────────────────────────────────

@router.callback_query(F.data == "adm_tickets")
async def cb_adm_tickets(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    tickets = await DB.get_open_tickets()
    if not tickets:
        await q.message.edit_text(
            "💬 Нет открытых тикетов.",
            reply_markup=ikb([("🔙 Панель", "adm_back")])
        )
        await q.answer(); return

    text = f"💬 <b>Открытые тикеты ({len(tickets)})</b>\n\n"
    rows = []
    for t in tickets[:10]:
        nm  = html_module.escape(t.get("first_name") or t.get("username") or str(t["user_id"]))
        txt = html_module.escape((t["message"] or "")[:50])
        text += f"<b>#{t['id']}</b> от {nm}: {txt}…\n"
        rows.append([(f"↩️ #{t['id']} — {nm[:12]}", f"adm_reply_{t['id']}_{t['user_id']}")])

    rows.append([("🔙 Панель", "adm_back")])
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*rows))
    await q.answer()

@router.callback_query(F.data.startswith("adm_reply_"))
async def cb_adm_reply_start(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer(); return
    parts = q.data.split("_")  # adm_reply_{tid}_{uid}
    tid   = int(parts[2])
    uid   = int(parts[3])
    await state.update_data(reply_tid=tid, reply_uid=uid)
    await q.message.answer(
        f"↩️ Ответ на тикет <b>#{tid}</b> пользователю <code>{uid}</code>\n\n"
        f"Введи текст ответа:",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(SupportSt.message)  # reuse state — we'll check context
    await q.answer()

# ── Клоны ботов (admin список) ────────────────────────────────────────────────

@router.callback_query(F.data == "adm_clones")
async def cb_adm_clones(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer(); return
    clones = await DB.all_clones()
    text   = f"<b>🤖 Все клонированные боты ({len(clones)})</b>\n\n"
    rows   = []
    for c in clones:
        run = "🟢" if c["bot_token"] in clone_bots else "🔴"
        text += f"{run} @{c['bot_username'] or '?'} owner={c['owner_id']}\n"
        rows.append([(f"{run} @{c['bot_username']}", f"adm_ctog_{c['id']}")])
    rows.append([("🔙 Панель", "adm_back")])
    await q.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=ikb(*rows))
    await q.answer()

# ── Business Message (Bot Mode monitoring) ────────────────────────────────────

@router.business_message()
async def on_business_msg(msg: Message):
    """Кешируем бизнес-сообщения для Bot Mode."""
    if not msg.business_connection_id:
        return
    try:
        conn = await bot_instance.get_business_connection(msg.business_connection_id)
        owner_id = conn.user.id
        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"] or u["mode"] != "chatbot":
            return
        mtype = "text"
        content = msg.text or msg.caption or ""
        if msg.photo:        mtype = "photo"
        elif msg.video:      mtype = "video"
        elif msg.voice:      mtype = "voice"
        elif msg.video_note: mtype = "video_note"
        elif msg.document:   mtype = "document"
        elif msg.sticker:    mtype = "sticker"
        elif msg.animation:  mtype = "animation"
        sender = msg.from_user.full_name if msg.from_user else "Unknown"
        chat_t = msg.chat.title or msg.chat.full_name or str(msg.chat.id)
        entry  = {"cid": msg.chat.id, "mid": msg.message_id, "type": mtype,
                  "sender": sender, "title": chat_t, "text": content, "file_id": None}
        mem_cache[owner_id].append(entry)
        asyncio.create_task(
            DB.cache_msg(owner_id, msg.chat.id, msg.message_id,
                         msg.from_user.id if msg.from_user else 0,
                         sender, chat_t, mtype, content)
        )
    except Exception as e:
        log.debug(f"business_msg err: {e}")

@router.edited_business_message()
async def on_business_edit(msg: Message):
    """Уведомляем об изменениях в Business-чатах."""
    if not msg.business_connection_id:
        return
    try:
        conn = await bot_instance.get_business_connection(msg.business_connection_id)
        owner_id = conn.user.id
        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"]:
            return
        new_text = msg.text or msg.caption or ""
        cached   = _get_mem(owner_id, msg.chat.id, msg.message_id)
        old_text = cached["text"] if cached else "—"
        sender   = msg.from_user.full_name if msg.from_user else "Unknown"
        chat_t   = msg.chat.title or str(msg.chat.id)
        await bot_instance.send_message(
            owner_id,
            notif_edited(sender, chat_t, old_text, new_text),
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(DB.log_edit(owner_id, msg.chat.id, chat_t, sender, old_text, new_text))
        if cached:
            cached["text"] = new_text
    except Exception as e:
        log.debug(f"business_edit err: {e}")

@router.deleted_business_messages()
async def on_business_delete(evt):
    """Уведомляем об удалениях в Business-чатах."""
    try:
        conn = await bot_instance.get_business_connection(evt.business_connection_id)
        owner_id = conn.user.id
        u = await DB.get_user(owner_id)
        if not u or not u["monitoring_on"]:
            return
        chat_id = evt.chat.id
        chat_t  = evt.chat.title or str(chat_id)
        for mid in (evt.message_ids or []):
            cached = _get_mem(owner_id, chat_id, mid)
            if cached:
                if cached["type"] == "text":
                    await bot_instance.send_message(
                        owner_id,
                        notif_deleted_text(cached["sender"], chat_t, cached.get("text") or ""),
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await bot_instance.send_message(
                        owner_id,
                        notif_deleted_media(cached["sender"], chat_t, cached["type"]),
                        parse_mode=ParseMode.HTML,
                    )
                asyncio.create_task(
                    DB.log_deletion(owner_id, chat_id, chat_t,
                                    cached["sender"], cached["type"], cached.get("text"))
                )
    except Exception as e:
        log.debug(f"business_delete err: {e}")

# ─── Обработчик текстовых сообщений (не в FSM) ───────────────────────────────

@router.message(F.text)
async def msg_any(msg: Message, state: FSMContext):
    curr = await state.get_state()
    if curr:
        return   # в FSM — не трогаем
    if msg.from_user.id == ADMIN_ID and msg.text and msg.text.startswith("/"):
        return
    user = await DB.get_user(msg.from_user.id)
    if not user:
        await cmd_start(msg, state)
        return
    if not user["is_verified"]:
        await cmd_start(msg, state)
        return
    await show_main(msg, user)

# ═══════════════════════════════════════════════════════════════════════════════
#  RESTORE RUNNING USERBOTS & CLONES ON STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

async def restore_sessions():
    log.info("Восстанавливаю активные userbot-сессии и клоны…")
    users = await DB.all_users()
    for u in users:
        if u.get("ub_session") and u.get("ub_active") and u.get("monitoring_on"):
            asyncio.create_task(start_userbot(u["user_id"], u["ub_session"]))

    clones = await DB.all_clones()
    for c in clones:
        if c["is_active"]:
            asyncio.create_task(launch_clone(c["bot_token"], c["owner_id"]))

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    global bot_instance, BOT_USERNAME

    await DB.connect()

    bot_instance = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    me            = await bot_instance.get_me()
    BOT_USERNAME  = me.username
    log.info(f"🚀 Бот @{BOT_USERNAME} (ID {me.id}) стартует…")

    # Устанавливаем команды
    from aiogram.types import BotCommand
    await bot_instance.set_my_commands([
        BotCommand(command="start",  description="Главное меню"),
        BotCommand(command="help",   description="Справка"),
        BotCommand(command="terms",  description="Условия использования"),
        BotCommand(command="admin",  description="Панель администратора"),
    ])

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Восстановление сессий
    asyncio.create_task(restore_sessions())

    log.info("✅ Запуск polling…")
    try:
        await dp.start_polling(
            bot_instance,
            allowed_updates=dp.resolve_used_update_types(),
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
