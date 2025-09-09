#!/bin/bash
# Quick Commands for First Wave Campaign
# Быстрые команды для работы с кампанией

echo "🚀 FIRST WAVE CAMPAIGN - БЫСТРЫЕ КОМАНДЫ"
echo "========================================"

# Переходим в правильную директорию
cd "$(dirname "$0")"

# Функция для вывода разделителя
separator() {
    echo ""
    echo "----------------------------------------"
    echo ""
}

# Функция для запроса подтверждения
confirm() {
    read -p "❓ $1 (y/N): " -n 1 -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]]
}

# Меню быстрых команд
echo "Выберите действие:"
echo ""
echo "1. 🔍 Полная диагностика системы"
echo "2. 📊 Отчет по базе данных"
echo "3. 🎯 Анализ целевой аудитории"
echo "4. 🧪 Тестовая отправка (3 контакта)"
echo "5. 🚀 Полная кампания (тест + отправка)"
echo "6. 📁 Экспорт целевой аудитории"
echo "7. 🧹 Очистка дубликатов"
echo "8. 📋 Просмотр логов"
echo "9. 🎛️  Интерактивное меню"
echo "0. 🚪 Выход"
echo ""

read -p "➡️ Выберите номер (0-9): " choice

case $choice in
    1)
        echo "🔍 Запуск полной диагностики..."
        separator
        
        echo "📋 Проверка готовности системы:"
        python campaign_manager.py check
        
        separator
        echo "📊 Общий отчет:"
        python campaign_manager.py report
        
        separator
        echo "🎯 Анализ целевой аудитории:"
        python campaign_manager.py analyze --target
        ;;
        
    2)
        echo "📊 Генерация отчета по базе данных..."
        separator
        
        if confirm "Показать детальный отчет?"; then
            python campaign_manager.py report --detailed
        else
            python campaign_manager.py report
        fi
        ;;
        
    3)
        echo "🎯 Анализ целевой аудитории..."
        separator
        python campaign_manager.py analyze --target
        
        if confirm "Экспортировать целевую аудиторию в CSV?"; then
            python campaign_manager.py export
        fi
        ;;
        
    4)
        echo "🧪 Тестовая отправка сообщений..."
        separator
        
        read -p "Количество тестовых контактов (по умолчанию 3): " test_limit
        test_limit=${test_limit:-3}
        
        if confirm "Запустить тест на $test_limit контактов?"; then
            python first_wave_campaign.py --test-only --test-limit $test_limit
        fi
        ;;
        
    5)
        echo "🚀 Полная кампания..."
        separator
        
        echo "⚠️  ВНИМАНИЕ: Будет запущена полная кампания сообщений!"
        echo "Сначала будет выполнен тест, затем основная отправка."
        echo ""
        echo "Выберите режим аккаунтов:"
        echo "1. 👥 Мульти-аккаунт (messenger1 + messenger2, рекомендуется)"
        echo "2. 👤 Один аккаунт (только messenger1)"
        echo ""
        
        read -p "Выберите режим (1-2): " account_mode
        
        campaign_cmd="python first_wave_campaign.py"
        if [ "$account_mode" = "2" ]; then
            campaign_cmd="$campaign_cmd --single-account"
            echo "✅ Будет использоваться один аккаунт"
        else
            echo "✅ Будет использоваться мульти-аккаунт"
        fi
        
        if confirm "Продолжить с полной кампанией?"; then
            $campaign_cmd
        fi
        ;;
        
    6)
        echo "📁 Экспорт целевой аудитории..."
        separator
        
        timestamp=$(date +"%Y%m%d_%H%M%S")
        output_file="data/target_audience_$timestamp.csv"
        
        echo "Экспорт в файл: $output_file"
        
        if confirm "Применить фильтры при экспорте?"; then
            python campaign_manager.py export -o "$output_file"
        else
            python campaign_manager.py export -o "$output_file" --no-filters
        fi
        ;;
        
    7)
        echo "🧹 Очистка дубликатов..."
        separator
        
        echo "Проверка дубликатов (режим просмотра):"
        python campaign_manager.py cleanup
        
        separator
        if confirm "Выполнить удаление дубликатов?"; then
            python campaign_manager.py cleanup --no-dry-run
        fi
        ;;
        
    8)
        echo "📋 Просмотр логов..."
        separator
        
        log_file="data/first_wave_campaign.log"
        
        if [ -f "$log_file" ]; then
            echo "Выберите действие с логом:"
            echo "1. Показать последние 50 строк"
            echo "2. Показать все ошибки"
            echo "3. Показать статистику отправки"
            echo "4. Следить за логом в реальном времени"
            echo ""
            
            read -p "Выберите (1-4): " log_choice
            
            case $log_choice in
                1)
                    echo "📜 Последние 50 строк лога:"
                    tail -50 "$log_file"
                    ;;
                2)
                    echo "❌ Ошибки в логе:"
                    grep -i "error\|failed\|exception" "$log_file" | tail -20
                    ;;
                3)
                    echo "📊 Статистика отправки:"
                    echo "Отправлено успешно:"
                    grep -c "✅.*отправлено" "$log_file" 2>/dev/null || echo "0"
                    echo "Ошибок отправки:"
                    grep -c "❌.*ошибка" "$log_file" 2>/dev/null || echo "0"
                    ;;
                4)
                    echo "👁️  Мониторинг лога (Ctrl+C для выхода):"
                    tail -f "$log_file"
                    ;;
            esac
        else
            echo "❌ Лог файл не найден: $log_file"
            echo "Запустите кампанию для создания лога."
        fi
        ;;
        
    9)
        echo "🎛️ Запуск интерактивного меню..."
        separator
        python launch_campaign.py
        ;;
        
    0)
        echo "👋 До свидания!"
        exit 0
        ;;
        
    *)
        echo "❌ Некорректный выбор: $choice"
        echo "Выберите число от 0 до 9"
        exit 1
        ;;
esac

separator
echo "✅ Команда выполнена!"

# Предложение дополнительных действий
echo ""
echo "💡 Дополнительные команды:"
echo "• python campaign_manager.py --help  - справка по менеджеру"
echo "• python first_wave_campaign.py --help  - справка по кампании"
echo "• python launch_campaign.py  - интерактивное меню"
echo ""
echo "📁 Важные файлы:"
echo "• data/SBC - Attendees.csv  - основная база участников"
echo "• data/first_wave_campaign.log  - детальный лог"
echo "• data/first_wave_results.csv  - результаты отправки"
echo ""

read -p "Нажмите Enter для завершения..."
