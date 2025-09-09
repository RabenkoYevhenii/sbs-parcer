# First Wave Campaign - Примеры использования

## 🚀 Быстрый старт

### 1. Интерактивный запуск (рекомендуется)
```bash
cd restricted/
python launch_campaign.py
```

### 2. Быстрые команды через скрипт
```bash
cd restricted/
./quick_commands.sh
```

## 📋 Основные команды

### Диагностика и анализ
```bash
# Проверка готовности системы
python campaign_manager.py check

# Общий отчет по базе данных
python campaign_manager.py report

# Детальный отчет
python campaign_manager.py report --detailed

# Анализ целевой аудитории
python campaign_manager.py analyze --target
```

### Управление данными
```bash
# Экспорт целевой аудитории
python campaign_manager.py export

# Экспорт в конкретный файл
python campaign_manager.py export -o my_targets.csv

# Экспорт без фильтров
python campaign_manager.py export --no-filters

# Проверка дубликатов
python campaign_manager.py cleanup

# Удаление дубликатов
python campaign_manager.py cleanup --no-dry-run
```

### Запуск кампаний
```bash
# Только тестовая отправка (3 контакта)
python first_wave_campaign.py --test-only

# Тестовая отправка с 10 контактами
python first_wave_campaign.py --test-only --test-limit 10

# Полная кампания (тест + основная отправка) с мульти-аккаунтом
python first_wave_campaign.py

# Полная кампания с одним аккаунтом
python first_wave_campaign.py --single-account

# Только основная отправка (без теста) с мульти-аккаунтом
python first_wave_campaign.py --full-only

# Увеличенный дневной лимит (200 сообщений)
python first_wave_campaign.py --daily-limit 200

# Комбинированные параметры
python first_wave_campaign.py --test-limit 5 --daily-limit 150
```

## 📊 Мониторинг

### Просмотр логов
```bash
# Последние записи
tail -50 data/first_wave_campaign.log

# Мониторинг в реальном времени
tail -f data/first_wave_campaign.log

# Поиск ошибок
grep -i "error\|failed" data/first_wave_campaign.log

# Статистика отправки
grep -c "✅.*отправлено" data/first_wave_campaign.log
```

### Анализ результатов
```bash
# Отчет после кампании
python campaign_manager.py report

# Проверка оставшейся аудитории
python campaign_manager.py analyze --target
```

## 🔧 Настройка

### Основные параметры в campaign_config.py
```python
# Тестовый режим
TEST_MODE = True
TEST_CONTACTS_LIMIT = 3

# Лимиты отправки
DAILY_LIMIT = 100
DELAY_BETWEEN_MESSAGES = (3, 7)

# Рабочее время
WORKING_HOURS_START = time(9, 0)
WORKING_HOURS_END = time(18, 0)
```

## 📁 Важные файлы

```
restricted/
├── data/
│   ├── SBC - Attendees.csv              # Основная база
│   ├── first_wave_campaign.log          # Детальный лог
│   ├── first_wave_campaign_log.json     # Лог кампаний
│   ├── first_wave_results.csv           # Результаты
│   └── target_audience_*.csv            # Экспорты аудитории
├── first_wave_campaign.py               # Основной скрипт
├── campaign_manager.py                  # Утилита управления
├── launch_campaign.py                   # Интерактивный запуск
└── quick_commands.sh                    # Быстрые команды
```

## ⚡ Типичные сценарии использования

### Первый запуск
```bash
# 1. Проверка готовности
python campaign_manager.py check

# 2. Анализ данных
python campaign_manager.py report --detailed

# 3. Интерактивный запуск
python launch_campaign.py
```

### Ежедневная работа
```bash
# Быстрая проверка
./quick_commands.sh

# Или интерактивно
python launch_campaign.py
```

### Экстренная диагностика
```bash
# Проверка системы
python campaign_manager.py check

# Просмотр ошибок
grep -i error data/first_wave_campaign.log | tail -10

# Анализ аудитории
python campaign_manager.py analyze --target
```

## 🚨 Решение проблем

### CSV файл не найден
```bash
ls data/SBC\ -\ Attendees.csv
# Убедитесь что файл существует в папке data/
```

### Ошибки аккаунтов
```bash
# Проверьте .env файл
cat .env | grep -E "(USERNAME|PASSWORD|USER_ID)"
```

### Нет целевой аудитории
```bash
# Проверьте фильтры
python campaign_manager.py analyze --target

# Экспортируйте без фильтров
python campaign_manager.py export --no-filters
```

### Проблемы с отправкой
```bash
# Проверьте логи
tail -50 data/first_wave_campaign.log

# Запустите тест
python first_wave_campaign.py --test-only --test-limit 1
```

---

💡 **Совет**: Всегда начинайте с команды `python launch_campaign.py` для интерактивного режима
