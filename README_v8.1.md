# MerAi & Monitoring v8.1 — CHANGELOG & DOCUMENTATION

**Дата обновления:** 10 марта 2026  
**Версия:** 8.1 (полная переработка v8.0)  
**Автор обновлений:** Claude (Anthropic)  
**Базовая версия:** MerAi & Monitoring v8.0 by @mrztn

---

## 🎯 КРИТИЧЕСКИЕ ОБНОВЛЕНИЯ v8.1

Данная версия представляет собой масштабное обновление с внедрением **5 ключевых систем**, запрошенных владельцем, плюс полная актуализация всех библиотек под стандарты марта 2026 года.

---

## 📋 ПОЛНЫЙ СПИСОК ИЗМЕНЕНИЙ

### 1. 💾 СИСТЕМА СОХРАНЕНИЯ СООБЩЕНИЙ

**Проблема:** Ранее сообщения сохранялись только через автоматический мониторинг удалений.

**Решение v8.1:**

#### A) Автосохранение сообщений в ЛС боту
- **Триггер:** Любое сообщение, отправленное боту в личные сообщения
- **Формат:** Текст, фото, видео, голосовые, видео-кружки, документы
- **Включая:** Таймерные сообщения (TTL), исчезающие медиа
- **Реализация:** Обработчик `@r.message(F.chat.type == "private")`
- **Конфигурация:** `save_dm_messages` в admin-конфиге (по умолчанию: включено)

**Пример использования:**
1. Пользователь отправляет боту любое сообщение (даже с таймером)
2. Бот автоматически сохраняет в кэш + БД
3. Отправляет уведомление о сохранении с иконкой 💾
4. Медиа сохраняется в бинарном виде для последующего экспорта

#### B) Команда "тест" для сохранения
- **Триггер:** Реплай на любое сообщение с текстом "тест" (регистронезависимо)
- **Действие:** Сохранение реплаенутого сообщения
- **Удаление:** Команда "тест" автоматически удаляется после выполнения
- **Реализация:** `@r.message(F.reply_to_message, F.text.lower() == "тест")`
- **Конфигурация:** `save_test_replies` в admin-конфиге

**Пример:**
```
[Сообщение от друга с таймером на фото]
↳ Вы: "тест"
→ Бот: Сохранено! 💾
→ Команда "тест" удалена
```

#### Технические детали:
- **Функция:** `save_message_to_cache(msg, owner_id, reason)`
- **Флаг в БД:** `saved_by_cmd = 1` для отличия от автоперехвата
- **Поддержка медиа:** Полная, включая таймерные (TTL) сообщения
- **Уведомления:** 
  - `notif_saved_dm()` для ЛС
  - `notif_saved_test()` для команды "тест"
- **Экспорт:** В ZIP-архивах помечаются значком 💾

**Структура БД (обновлённая):**
```sql
CREATE TABLE msg_cache (
    ...
    saved_by_cmd INTEGER DEFAULT 0,  -- НОВОЕ ПОЛЕ
    ...
);
```

---

### 2. 🔐 INLINE-КЛАВИАТУРА ДЛЯ USERBOT АВТОРИЗАЦИИ

**Проблема:** 
- Код авторизации вводился через обычные сообщения
- Telegram детектил код в чате → срабатывали защитные механизмы
- Высокий риск блокировки при авторизации userbot

**Решение v8.1:**

#### Защищённый ввод через inline-кнопки

**Компоненты:**
1. **Числовая клавиатура** (`inline_numpad()`)
   - 3x3 сетка цифр (1-9)
   - Нижний ряд: ←, 0, ✅
   - Дисплей текущего ввода (скрыт для пароля)
   - Кнопка отмены

2. **Обработчик ввода** (`handle_numpad()`)
   - Накопление цифр в `ub_auth_data`
   - Backspace для исправлений
   - Submit для отправки

3. **Безопасное хранение**
   - Код/пароль НЕ отправляются в чат
   - Хранятся в памяти (dict `ub_auth_data`)
   - Удаляются после успешной авторизации

**Новый флоу авторизации:**
```
1. api_id (текст) → 2. api_hash (текст) → 3. phone (текст)
4. Код отправлен ✅
5. INLINE-КЛАВИАТУРА для ввода кода ⬅ НОВОЕ
6. Если нужен 2FA → INLINE-КЛАВИАТУРА для пароля ⬅ НОВОЕ
7. Успешная авторизация
```

**Технические детали:**
- **Callback pattern:** `numpad_{purpose}_{action}`
  - `purpose`: `code` или `password`
  - `action`: `0-9`, `back`, `submit`, `display`
- **Функции:**
  - `inline_numpad(current_code, purpose)` — генерация клавиатуры
  - `handle_numpad(q)` — обработка нажатий
  - `_ub_auth_success(msg, session)` — финализация

**Преимущества:**
- ✅ Telegram не видит код в сообщениях
- ✅ Невозможно случайно отправить код в другой чат
- ✅ Визуальная обратная связь (дисплей ввода)
- ✅ Защита от детектирования

**Старая система (удалена):**
```python
# FSM states (УДАЛЕНЫ из v8.1):
# UserBotSt.code = State()    ❌
# UserBotSt.twofa = State()   ❌
```

---

### 3. 🎁 ТЕСТОВЫЙ ПЕРИОД ВМЕСТО FREE-ПЛАНА

**Проблема:** 
- Free-план не давал доступа к функциям
- Пользователи не могли оценить возможности
- Низкая конверсия в платные подписки

**Решение v8.1:**

#### Система тестового периода

**Ключевые изменения:**

1. **Новая структура БД:**
```sql
ALTER TABLE users ADD COLUMN trial_expires TEXT;
ALTER TABLE users ADD COLUMN trial_used INTEGER DEFAULT 0;
-- plan теперь может быть 'trial' вместо 'free'
```

2. **Автоактивация при верификации:**
```python
async def verify(uid):
    test_days = await DB.get_config("test_period_days", "3")
    trial_exp = (datetime.now(timezone.utc) + timedelta(days=test_days)).isoformat()
    await DB.execute("""
        UPDATE users SET 
            is_verified=1, 
            trial_expires=?, 
            trial_used=0,
            plan='trial'
        WHERE user_id=?
    """, (trial_exp, uid))
```

3. **Проверка активности:**
```python
async def plan_active(uid) -> bool:
    # Проверка trial
    if plan == "trial" and not trial_used and trial_expires:
        if datetime.fromisoformat(trial_expires) > datetime.now(timezone.utc):
            return True  # ✅ Trial активен
    
    # Проверка платного плана
    if plan not in ("trial", "free") and plan_expires:
        return datetime.fromisoformat(plan_expires) > datetime.now(timezone.utc)
    
    return False
```

**Логика работы:**
1. Новый пользователь → верификация → **автоматически 3 дня trial**
2. Trial даёт 100% функционала (как платная подписка)
3. После trial → требуется покупка плана
4. При покупке → `trial_used = 1`, plan меняется на купленный

**Отображение в интерфейсе:**
```
🎁 Тестовый период
├ 📅 До: 13.03.2026 15:30
```

**Настройка периода:**
- **Конфиг:** `test_period_days` (по умолчанию: 3)
- **Admin-панель:** Изменяется в разделе "Конфиг"

**Статистика:**
- Новое поле в `/admin`: "🎁 На тестовом периоде: X"
- Отслеживание конверсии trial → paid

---

### 4. ✅ АВТОМАТИЧЕСКАЯ ВЕРИФИКАЦИЯ КАНАЛА

**Проблема:**
- Администратору неудобно вручную управлять каналом
- Нет автопроверки выхода пользователей
- Нет блокировки функционала при покидании канала

**Решение v8.1:**

#### Многоуровневая система проверки канала

**Компоненты:**

1. **Базовая проверка** (`check_channel_membership()`)
```python
async def check_channel_membership(bot_obj, user_id) -> bool:
    try:
        member = await bot_obj.get_chat_member(CAPTCHA_CHAN_ID, user_id)
        is_member = member.status.value in (
            "member", "administrator", "creator", "restricted"
        )
        
        # Обновляем статус в БД
        await DB.set_field(user_id, "channel_member", 1 if is_member else 0)
        await DB.set_field(user_id, "channel_left", 0 if is_member else 1)
        await DB.set_field(user_id, "last_channel_check", datetime.now().isoformat())
        
        return is_member
    except Exception:
        return False
```

2. **Комплексная верификация** (`verify_and_check_channel()`)
```python
async def verify_and_check_channel(user_id) -> tuple[bool, str]:
    """
    Returns: (can_proceed, error_message)
    """
    user = await DB.get_user(user_id)
    
    # Проверка бана
    if user["is_banned"]:
        return False, "🚫 Ваш аккаунт заблокирован"
    
    # Проверка верификации
    if not user["is_verified"]:
        return False, "❌ Необходимо пройти верификацию"
    
    # Автоматическая проверка канала
    auto_verify = await DB.get_config("auto_verify_enabled", "1")
    if auto_verify == "1":
        is_member = await check_channel_membership(bot_instance, user_id)
        if not is_member or user["channel_left"]:
            return False, (
                "❌ Вы покинули канал верификации!\n\n"
                f"Вступите обратно: {CAPTCHA_CHANNEL}"
            )
    
    # Проверка активности плана
    if not await DB.plan_active(user_id):
        return False, "⚠️ Подписка истекла!"
    
    return True, ""
```

3. **Точки проверки:**
- ✅ Перед включением мониторинга
- ✅ Перед покупкой плана
- ✅ Перед настройкой режима работы
- ✅ Перед клонированием ботов
- ✅ При каждом /start

**Пример использования:**
```python
@r.callback_query(F.data == "toggle_monitor")
async def _toggle_monitor(q: CallbackQuery):
    can_proceed, error_msg = await verify_and_check_channel(q.from_user.id)
    if not can_proceed:
        await q.answer(error_msg, show_alert=True)
        return
    # ... продолжение логики
```

4. **Фоновая проверка** (`channel_check_task()`)
```python
async def channel_check_task():
    while True:
        check_interval = int(await DB.get_config("channel_check_hours", "1"))
        await asyncio.sleep(check_interval * 3600)
        
        users = await DB.all_users()
        for u in users:
            if not u["is_verified"] or u["is_banned"]:
                continue
            
            is_member = await check_channel_membership(bot_instance, u["user_id"])
            
            # Если вышел - уведомляем и останавливаем
            if not is_member and not u["channel_left"]:
                await bot_instance.send_message(
                    u["user_id"],
                    f"⚠️ Обнаружен выход из канала!\n\n"
                    f"Вернись: {CAPTCHA_CHANNEL}"
                )
                await DB.set_field(u["user_id"], "monitoring_on", 0)
                if u["user_id"] in ub_clients:
                    asyncio.create_task(stop_userbot(u["user_id"]))
```

**Новые поля БД:**
```sql
ALTER TABLE users ADD COLUMN channel_left INTEGER DEFAULT 0;
ALTER TABLE users ADD COLUMN last_channel_check TEXT DEFAULT (datetime('now'));
```

**Настройки admin:**
- `auto_verify_enabled` — включить/выключить автопроверку
- `channel_check_hours` — интервал фоновой проверки (часы)

**Блокировка функционала:**
При выходе из канала:
- ❌ Мониторинг автоматически отключается
- ❌ Userbot останавливается
- ❌ Все действия блокируются до возвращения
- ✅ Уведомление пользователю

---

### 5. 🔧 РАСШИРЕННАЯ АДМИН-ПАНЕЛЬ КОНФИГА

**Проблема:** Старая админка имела ограниченные настройки.

**Решение v8.1:**

#### Новые настраиваемые параметры

**Добавлены в `system_config`:**

1. **API ключи Userbot:**
   - `pyro_api_id` — числовой ID приложения
   - `pyro_api_hash` — хеш приложения

2. **Система подписок:**
   - `ref_goal` — цель рефералов (по умолчанию: 50)
   - `clone_bonus_days` — бонус за клонирование (3 дня)
   - `test_period_days` — длительность trial (3 дня) ⬅ НОВОЕ

3. **Безопасность:**
   - `auto_verify_enabled` — автопроверка канала (1/0) ⬅ НОВОЕ
   - `channel_check_hours` — интервал проверки (часы) ⬅ НОВОЕ

4. **Функции сохранения:**
   - `save_dm_messages` — сохранять ЛС боту (1/0) ⬅ НОВОЕ
   - `save_test_replies` — команда "тест" (1/0) ⬅ НОВОЕ

**Интерфейс admin-конфига:**
```
🔧 Конфиг системы v8.1

API ключи (Userbot):
pyro_api_id: 12345678
pyro_api_hash: abcdef12...

Система подписок:
ref_goal: 50
clone_bonus_days: 3
test_period_days: 3

Безопасность:
auto_verify_enabled: 1
channel_check_hours: 1

Функции сохранения:
save_dm_messages: 1
save_test_replies: 1

[Кнопки редактирования для каждого параметра]
```

**Редактирование:**
1. Админ нажимает кнопку параметра
2. Вводит новое значение
3. Система обновляет БД + глобальные переменные
4. Изменения применяются немедленно

---

## 📦 ОБНОВЛЕНИЯ БИБЛИОТЕК ПОД 2026

**requirements.txt v8.1:**
```txt
# MerAi & Monitoring v8.1
# Python 3.11+ | Март 2026
# Обновлено: 10.03.2026

aiogram==3.26.0          # Stable release март 2026
Pyrogram==2.0.106        # Latest MTProto
TgCrypto==1.2.5          # Crypto for Pyrogram
aiosqlite==0.20.0        # Async SQLite
aiohttp==3.10.5          # HTTP клиент
aiofiles==24.1.0         # Async файлы
python-dateutil==2.9.0   # Дата/время утилиты
```

**Критические обновления:**
- ✅ aiogram 3.26.0 — поддержка Telegram Bot API 9.5 (март 2026)
- ✅ Совместимость с Python 3.11+
- ✅ Все зависимости актуализированы

---

## 🗄️ СТРУКТУРА БД v8.1

**Новые поля в `users`:**
```sql
trial_expires    TEXT           -- Дата окончания trial
trial_used       INTEGER DEFAULT 0    -- Флаг использования trial
channel_left     INTEGER DEFAULT 0    -- Флаг выхода из канала
last_channel_check TEXT DEFAULT (datetime('now'))  -- Последняя проверка
```

**Новые поля в `msg_cache`:**
```sql
saved_by_cmd     INTEGER DEFAULT 0    -- Сохранено вручную (ЛС/тест)
```

**Новые записи в `system_config`:**
```sql
('test_period_days',    '3')
('auto_verify_enabled', '1')
('channel_check_hours', '1')
('save_dm_messages',    '1')
('save_test_replies',   '1')
```

---

## 🚀 НОВЫЕ ФОНОВЫЕ ЗАДАЧИ

**v8.1 запускает 3 фоновые задачи:**

1. **`restore_sessions()`** — восстановление userbot после рестарта
2. **`autorenew_task()`** — автопродление подписок (каждый час)
3. **`channel_check_task()`** — проверка канала (каждые N часов) ⬅ НОВОЕ

---

## 📊 СТАТИСТИКА v8.1

**Новые метрики в `/admin`:**
```
🎁 На тестовом периоде: X  ⬅ НОВОЕ
👤 Userbot сессий: X
💾 Сохранено вручную: X  ⬅ (через кэш)
```

---

## 🔄 МИГРАЦИЯ С v8.0 НА v8.1

**Автоматические действия при первом запуске:**

1. **Создание новых полей БД** — `trial_expires`, `trial_used`, и т.д.
2. **Вставка новых конфигов** — `test_period_days`, `save_dm_messages`
3. **Миграция существующих пользователей:**
   - Free-план → Trial (если не истёк)
   - Платные планы остаются без изменений

**Ручные действия не требуются.**

---

## 💡 РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ

### Для пользователей:

1. **Тестовый период:**
   - Используй все функции бесплатно 3 дня
   - Попробуй оба режима (Bot + Userbot)
   - Оцени перехват удалений, TTL, view_once

2. **Сохранение сообщений:**
   - Важные сообщения → пересылай боту в ЛС
   - Таймерные медиа → отправляй до истечения
   - Команда "тест" → быстрое сохранение прямо в чате

3. **Безопасность:**
   - Не выходи из канала верификации
   - Используй inline-клавиатуру для userbot
   - Код 2FA вводи через кнопки

### Для администратора:

1. **Настройка конфига:**
   - Установи оптимальный `test_period_days` (3-7 дней)
   - Включи `auto_verify_enabled` для безопасности
   - Настрой `channel_check_hours` (1-6 часов)

2. **Мониторинг:**
   - Проверяй статистику trial → paid конверсии
   - Отслеживай выходы из канала
   - Анализируй использование сохранений

3. **Оптимизация:**
   - Корректируй `clone_bonus_days` для стимула
   - Изменяй `ref_goal` в зависимости от роста

---

## 🐛 ИЗВЕСТНЫЕ ОГРАНИЧЕНИЯ

1. **Сохранение в ЛС:**
   - Работает только для верифицированных пользователей
   - Требуется активный план (trial или paid)

2. **Команда "тест":**
   - Регистронезависима ("тест", "Тест", "ТЕСТ")
   - Работает только на реплаи
   - Требует прав на удаление сообщения

3. **Inline-клавиатура:**
   - Максимум 10 символов для кода/пароля
   - Только цифры (для буквенных паролей нужна доработка)

4. **Фоновая проверка канала:**
   - Зависит от Bot API (может быть задержка)
   - При большом количестве пользователей — нагрузка на API

---

## 📞 ПОДДЕРЖКА И ВОПРОСЫ

**Технические вопросы:**
- Telegram: @mrztn
- Email: support@merai.bot (если настроен)

**Документация:**
- `/help` в боте — полная справка
- `/terms` — условия использования
- `/admin` → Конфиг — настройки системы

---

## 📝 ЛИЦЕНЗИЯ

© 2026 MerAi & Monitoring  
Все права защищены.  
Базовая версия by @mrztn  
Обновление v8.1 by Claude (Anthropic)

---

## 🎯 ЗАКЛЮЧЕНИЕ

Версия 8.1 представляет собой **полностью переработанную систему** с акцентом на:

✅ **Удобство** — сохранение сообщений в 1 клик  
✅ **Безопасность** — inline-ввод кодов, автопроверка канала  
✅ **Гибкость** — тестовый период, расширенная админка  
✅ **Актуальность** — все библиотеки обновлены под март 2026  

**Рекомендуемые следующие шаги:**
1. Протестировать все новые функции на тестовом боте
2. Настроить admin-конфиг под свои нужды
3. Запустить production и мониторить метрики
4. Собрать обратную связь от первых пользователей

Удачного запуска! 🚀
