#!/bin/bash
# MerAi & Monitoring - Скрипт установки для BotHost
# Версия: 5.2.0

echo "🚀 MerAi & Monitoring - Установка зависимостей"
echo "================================================"

# Установка зависимостей
echo "📦 Установка Python пакетов..."
pip install -r requirements.txt --break-system-packages

if [ $? -eq 0 ]; then
    echo "✅ Зависимости установлены успешно!"
else
    echo "❌ Ошибка установки зависимостей"
    exit 1
fi

# Проверка переменных окружения
echo ""
echo "🔍 Проверка конфигурации..."

if [ -z "$BOT_TOKEN" ]; then
    echo "⚠️  BOT_TOKEN не установлен!"
    echo "Создайте переменную окружения: BOT_TOKEN=ваш_токен"
    exit 1
else
    echo "✅ BOT_TOKEN найден"
fi

# Проверка режима
MODE=${MODE:-bot}
echo "📱 Режим работы: $MODE"

if [ "$MODE" = "userbot" ]; then
    if [ -z "$API_ID" ] || [ -z "$API_HASH" ]; then
        echo "⚠️  Для userbot режима требуются API_ID и API_HASH"
        exit 1
    else
        echo "✅ Userbot конфигурация проверена"
    fi
fi

echo ""
echo "================================================"
echo "✅ Установка завершена!"
echo "================================================"
echo ""
echo "🚀 Запуск бота..."
python3 merai_bot.py
