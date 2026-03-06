import os
import asyncio
import logging
import aiosqlite
import openai
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.markdown import hbold

# --- КОНФИГУРАЦИЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
AI_API_KEY = os.getenv("AI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# Настройка клиента для Z.ai (Zhipu AI)
# Правильный адрес API: https://open.bigmodel.cn/api/paas/v4/
client = openai.AsyncOpenAI(
    api_key=AI_API_KEY,
    base_url="https://open.bigmodel.cn/api/paas/v4/"
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- БАЗА ДАННЫХ ---
DB_NAME = "merai_memory.db"
FAKE_START_COUNT = 78000  # Начальная цифра

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT)')
        await db.execute('CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, role TEXT, content TEXT)')
        await db.commit()

async def add_user(user_id, username):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cur.fetchone():
            await db.execute("INSERT INTO users VALUES (?, ?)", (user_id, username))
            await db.commit()
            return True
    return False

async def get_total_users():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        real = (await cur.fetchone())[0]
        return real + FAKE_START_COUNT

async def add_msg(user_id, role, content):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO history (user_id, role, content) VALUES (?, ?, ?)", (user_id, role, content))
        await db.commit()

async def get_history(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT role, content FROM history WHERE user_id = ? ORDER BY id DESC LIMIT 10", (user_id,))
        rows = await cur.fetchall()
        return [{"role": r[0], "content": r[1]} for r in reversed(rows)]

async def clear_db_history(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM history WHERE user_id = ?", (user_id,))
        await db.commit()

# --- АНИМАЦИЯ "THINKING" ---
async def animate_thinking(message: types.Message):
    """Красивая анимация 'Думает...' с точками"""
    statuses = [
        "🧠 Думаю.", 
        "🧠 Думаю..", 
        "🧠 Думаю...", 
        "🧠 Анализирую...", 
        "🧠 Формулирую..."
    ]
    i = 0
    while True:
        try:
            await message.edit_text(statuses[i % len(statuses)])
            i += 1
            await asyncio.sleep(0.6)
        except Exception:
            break

# --- КЛАВИАТУРЫ ---
main_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🗑 Очистить диалог", callback_data="clear")],
    # Картинки пока убраны, так как API Z.ai для картинок отличается
])

# --- ХЕНДЛЕРЫ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    is_new = await add_user(user.id, user.username)
    count = await get_total_users()
    
    text = (
        f"👋 Привет, {hbold(user.first_name)}!\n"
        f"🧠 Я — {hbold('MerAi GPT')}\n"
        f"👨‍💻 Создатель: {hbold('@mrztn')}\n\n"
        f"📊 Наша семья: {hbold(count)} пользователей!\n\n"
        f"💬 Напиши мне что-нибудь, и я помогу кодом или текстом."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=main_kb)

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ Нет прав.")
    
    count = await get_total_users()
    text = f"👑 Админ-панель\n👥 Юзеров: {count}"
    await message.answer(text)

@dp.callback_query(F.data == "clear")
async def cb_clear(callback: types.CallbackQuery):
    await clear_db_history(callback.from_user.id)
    await callback.answer("✅ Память очищена!", show_alert=True)

@dp.message()
async def handle_msg(message: types.Message):
    user_id = message.from_user.id
    text = message.text
    
    await add_user(user_id, message.from_user.username)
    
    # Запускаем анимацию
    anim_msg = await message.answer("🧠 Думаю...")
    anim_task = asyncio.create_task(animate_thinking(anim_msg))
    
    try:
        # --- ГЕНЕРАЦИЯ ТЕКСТА (Z.ai / GLM) ---
        history = await get_history(user_id)
        system_prompt = (
            "Ты MerAi GPT — умный ассистент. "
            "Твой создатель — @mrztn. "
            "Ты отвечаешь структурированно, красиво. "
            "Если пользователь просит код — ты пишешь его в блоках кода. "
            "Ты не OpenAI, ты MerAi от Z.ai."
        )
        messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": text}]
        
        # Запрос к Z.ai
        # Используем модель glm-4-flash (быстрая и бесплатная/дешевая) или glm-4
        res = await client.chat.completions.create(
            model="glm-4-flash", 
            messages=messages, 
            temperature=0.7
        )
        answer = res.choices[0].message.content
        
        # Сохраняем в память
        await add_msg(user_id, "user", text)
        await add_msg(user_id, "assistant", answer)
        
        # Останавливаем анимацию
        anim_task.cancel()
        
        # Отправка ответа
        if len(answer) > 4090:
            await anim_msg.edit_text("📝 Ответ длинный, отправляю частями...")
            for x in range(0, len(answer), 4000):
                await message.answer(answer[x:x+4000], parse_mode="Markdown")
            await anim_msg.delete()
        else:
            await anim_msg.edit_text(answer, parse_mode="Markdown")
                
    except Exception as e:
        anim_task.cancel()
        await anim_msg.edit_text(f"⚠️ Ошибка API Z.ai:\n`{str(e)}`", parse_mode="Markdown")

# --- ЗАПУСК ---
async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.info("🚀 MerAi Bot Started")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
