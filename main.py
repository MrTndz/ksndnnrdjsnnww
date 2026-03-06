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

# --- ЗАГРУЗКА КОНФИГА ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
AI_API_KEY = os.getenv("AI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0)) # ID создателя для админ-команды

# Настройка AI (Используем OpenAI-совместимый формат)
# Если ваш ключ от другого провайдера, укажите base_url
# Для standard OpenAI base_url можно убрать или оставить стандартным
client = openai.AsyncOpenAI(
    api_key=AI_API_KEY,
    base_url="https://api.openai.com/v1" 
)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

# --- КОНСТАНТЫ И БД ---
DB_NAME = "merai_memory.db"
FAKE_START_COUNT = 78000  # Начальная "накрутка" статистики
MEMORY_LIMIT = 10        # Сколько сообщений помнит бот

# --- БАЗА ДАННЫХ (Инициализация) ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Таблица пользователей
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen INTEGER
            )
        ''')
        # Таблица истории чатов (Память)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                role TEXT,
                content TEXT
            )
        ''')
        await db.commit()

# --- ФУНКЦИИ ПАМЯТИ ---
async def add_message_to_history(user_id, role, content):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO history (user_id, role, content) VALUES (?, ?, ?)",
            (user_id, role, content)
        )
        await db.commit()

async def get_history(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT role, content FROM history WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, MEMORY_LIMIT)
        )
        rows = await cursor.fetchall()
        # Возвращаем в правильном порядке (старые -> новые)
        return [{"role": r[0], "content": r[1]} for r in reversed(rows)]

async def clear_history(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM history WHERE user_id = ?", (user_id,))
        await db.commit()

# --- СТАТИСТИКА ---
async def register_user(user_id, username):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            await db.execute(
                "INSERT INTO users (user_id, username, first_seen) VALUES (?, ?, 1)",
                (user_id, username)
            )
            await db.commit()
            return True
    return False

async def get_total_users():
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        real_count = (await cursor.fetchone())[0]
        return real_count + FAKE_START_COUNT

# --- КЛАВИАТУРЫ ---
def get_main_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🗑 Очистить память", callback_data="clear_memory")],
        [InlineKeyboardButton(text="🖼 Сгенерировать фото", callback_data="gen_image")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- АНИМАЦИЯ "ДУМАЕТ..." ---
async def thinking_animation(message: types.Message):
    statuses = ["Думает.", "Думает..", "Думает..."]
    sent_msg = await message.answer("Думает.")
    return sent_msg

async def update_animation(sent_msg, text, parse_mode="HTML"):
    try:
        await sent_msg.edit_text(text, parse_mode=parse_mode)
    except Exception as e:
        logging.warning(f"Ошибка редактирования: {e}")

# --- ОБРАБОТЧИКИ КОМАНД ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    is_new = await register_user(user.id, user.username)
    total_users = await get_total_users()
    
    text = (
        f"👋 Привет, {hbold(user.first_name)}!\n"
        f"🧠 Я {hbold('MerAi GPT')} — твой умный ассистент.\n"
        f"👨‍💻 Создатель: {hbold('@mrztn')}\n\n"
        f"📊 Наша семья уже выросла до {hbold(total_users)} пользователей!\n\n"
        f"💡 Просто напиши мне что-нибудь.\n"
        f"Если хочешь картинку — нажми кнопку ниже или напиши 'нарисуй...'."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав для этой команды.")
        return

    total_users = await get_total_users()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM history")
        total_messages = (await cursor.fetchone())[0]

    text = (
        f"👑 {hbold('Админ-панель')}\n\n"
        f"👤 Всего пользователей: {total_users}\n"
        f"💬 Сообщений в памяти: {total_messages}\n"
        f"🛠 Версия: 5.2.0"
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("clear"))
async def cmd_clear(message: types.Message):
    await clear_history(message.from_user.id)
    await message.answer("🧠 Память очищена. Начинаем с чистого листа!")

# --- ЛОГИКА AI ---

async def generate_text(user_id, prompt):
    history = await get_history(user_id)
    
    system_prompt = (
        "Ты MerAi GPT, мощный ИИ-ассистент. Твой создатель — @mrztn. "
        "Ты должен отвечать на вопросы, помогать с кодом и общаться дружелюбно. "
        "Если тебя просят написать код, обязательно оформляй его в блоках кода. "
        "Не говори, что ты от OpenAI или Z.ai, ты — MerAi."
    )
    
    messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": prompt}]
    
    response = await client.chat.completions.create(
        model="gpt-3.5-turbo", # Или gpt-4
        messages=messages,
        temperature=0.7
    )
    
    answer = response.choices[0].message.content
    # Сохраняем в память
    await add_message_to_history(user_id, "user", prompt)
    await add_message_to_history(user_id, "assistant", answer)
    
    return answer

async def generate_image(prompt):
    try:
        # Используем DALL-E 3 для генерации
        response = await client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            size="1024x1024",
            quality="standard",
            n=1,
        )
        return response.data[0].url
    except Exception as e:
        return f"Ошибка генерации изображения: {str(e)}"

# --- ОБРАБОТКА СООБЩЕНИЙ ---

@dp.callback_query(F.data == "clear_memory")
async def callback_clear(callback: types.CallbackQuery):
    await clear_history(callback.from_user.id)
    await callback.answer("✅ Память очищена!")
    await callback.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data == "gen_image")
async def callback_image(callback: types.CallbackQuery):
    await callback.message.answer("🎨 Напишите описание картинки (промпт), например: 'Кот в космосе'")
    await callback.answer()

@dp.message()
async def handle_message(message: types.Message):
    user_id = message.from_user.id
    text = message.text
    
    # Регистрация пользователя
    await register_user(user_id, message.from_user.username)
    
    # Запуск анимации
    thinking_msg = await thinking_animation(message)
    
    # Логика выбора: Код/Фото/Текст
    # Список ключевых слов для фото
    image_keywords = ["нарисуй", "сгенерируй фото", "создай изображение", "картинку", "рисунок", "image", "draw"]
    
    is_image_request = any(word in text.lower() for word in image_keywords) or ("сделай мне код" not in text.lower() and message.reply_to_message and message.reply_to_message.from_user.id == bot.id and "рисунок" in message.reply_to_message.text)
    
    # Если просьба явно про картинку
    if any(word in text.lower() for word in image_keywords):
        await update_animation(thinking_msg, "🎨 Рисую изображение...")
        image_url = await generate_image(text)
        
        if image_url.startswith("http"):
            await bot.send_photo(user_id, image_url, caption=f"🖼 Вот ваша картинка по запросу: {text}")
            await thinking_msg.delete()
        else:
            await update_animation(thinking_msg, f"⚠️ {image_url}")
    else:
        # Обычный текст или код
        await update_animation(thinking_msg, "🧠 Обрабатываю запрос...")
        answer = await generate_text(user_id, text)
        
        # Если ответ длинный, лучше отправить новым сообщением, чтобы не мучить edit
        if len(answer) > 4090:
            await thinking_msg.delete()
            await message.answer(answer, parse_mode="Markdown")
        else:
            await update_animation(thinking_msg, answer, parse_mode="Markdown")

# --- ЗАПУСК ---
async def main():
    print("🚀 MerAi Bot запускается...")
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")
