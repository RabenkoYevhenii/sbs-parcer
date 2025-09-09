# Мульти-аккаунтовая система - Руководство по использованию

## 🎯 Обзор

Система поддерживает использование **двух messenger аккаунтов** для неограниченной пропускной способности и равномерного распределения нагрузки.

## 📊 Распределение нагрузки

### Автоматическое распределение:
- **messenger1**: 50% контактов
- **messenger2**: 50% контактов  

### Пример распределения:
```
Всего контактов: 100
├── messenger1: 50 контактов (1-50)
└── messenger2: 50 контактов (51-100)
```

## ⚙️ Настройка аккаунтов

### 1. Файл .env
```env
# Основной scraper аккаунт (для получения данных)
SCRAPER_USERNAME=scraper_account@example.com
SCRAPER_PASSWORD=scraper_password
SCRAPER_USER_ID=scraper_user_id

# Messenger аккаунт 1 (50% отправки)
MESSENGER1_USERNAME=messenger1@example.com
MESSENGER1_PASSWORD=messenger1_password
MESSENGER1_USER_ID=messenger1_user_id

# Messenger аккаунт 2 (50% отправки)  
MESSENGER2_USERNAME=messenger2@example.com
MESSENGER2_PASSWORD=messenger2_password
MESSENGER2_USER_ID=messenger2_user_id
```

### 2. Проверка конфигурации
```bash
# Проверка готовности всех аккаунтов
python campaign_manager.py check

# Тест подключения
python first_wave_campaign.py --test-only --test-limit 2
```

## 🚀 Команды запуска

### Мульти-аккаунт (рекомендуется)
```bash
# Полная кампания с автоматическим распределением
python first_wave_campaign.py

# Тест с мульти-аккаунтом
python first_wave_campaign.py --test-only --test-limit 10

# Без лимитов для мульти-аккаунта
python first_wave_campaign.py --daily-limit 0
```

### Один аккаунт
```bash
# Полная кампания только с messenger1
python first_wave_campaign.py --single-account

# Тест с одним аккаунтом
python first_wave_campaign.py --test-only --single-account
```

## 📈 Мониторинг мульти-аккаунта

### Лог файлы показывают:
```
[messenger1] [1] Отправка: John Doe
✅ [messenger1] Сообщение отправлено: John Doe
[messenger2] [61] Отправка: Jane Smith  
✅ [messenger2] Сообщение отправлено: Jane Smith
```

### Итоговая статистика:
```
📊 Статистика по аккаунтам:
   messenger1: 50/50 (100.0%)
   messenger2: 50/50 (100.0%) 
   👥 Мульти-аккаунт: Да
```

## 🔧 Продвинутые настройки

### campaign_config.py
```python
# Распределение между аккаунтами
ACCOUNT_DISTRIBUTION = {
    "messenger1": 50,  # 50% контактов
    "messenger2": 50   # 50% контактов
}

# Резервные аккаунты
BACKUP_ACCOUNTS = ["scraper"]

# Автоматическое переключение при блокировке
AUTO_SWITCH_ACCOUNT = True
```

### Настройка лимитов:
```python
# Без лимитов отправки
DAILY_LIMIT = 0  # 0 = неограниченно

# Задержки одинаковые для всех аккаунтов
DELAY_BETWEEN_MESSAGES = (3, 7)
```

## 🛡️ Безопасность и лимиты

### Настройки отправки (без ограничений):
- **Без дневных лимитов** - отправка неограниченного количества сообщений
- **Равномерное распределение** - 50% на каждый аккаунт
- **Задержки**: 3-7 секунд между сообщениями для каждого аккаунта
- **Переключение**: автоматическое при обнаружении блокировки
- **Круглосуточная работа** - без ограничений по времени

### Стратегии безопасности:
1. **Разные IP адреса** для разных аккаунтов (рекомендуется)
2. **Разное время работы** - можно настроить сдвиг
3. **Разные шаблоны** сообщений (опционально)
4. **Мониторинг ошибок** по каждому аккаунту

## 🔄 Процесс работы мульти-аккаунта

### 1. Инициализация
```
🚀 Запускаем браузер...
🔑 Логинимся с messenger1...
✅ Скрапер инициализирован с messenger1
```

### 2. Распределение контактов
```
📊 Распределение контактов:
  Messenger1: 50 контактов  
  Messenger2: 50 контактов
```

### 3. Отправка с messenger1
```
🔄 Начинаем отправку с Messenger1 аккаунта...
📥 Завантажуємо існуючі чати...
[messenger1] [1] Отправка: Contact 1
[messenger1] [2] Отправка: Contact 2
...
✅ Messenger1: отправлено 50 сообщений
```

### 4. Переключение на messenger2
```
🔄 Начинаем отправку с Messenger2 аккаунта...
🚪 Выходим из поточного акаунта...
🔑 Логинимся с messenger2...
📥 Завантажуємо існуючі чати...
[messenger2] [61] Отправка: Contact 61
...
✅ Messenger2: отправлено 50 сообщений
```

## 📊 Результаты и отчеты

### CSV результатов включает колонку account_used:
```csv
timestamp,user_id,full_name,message_sent,account_used
2025-09-08T10:00:00,user1,John Doe,true,messenger1
2025-09-08T10:05:00,user2,Jane Smith,true,messenger2
```

### Анализ эффективности:
```bash
# Отчет по результатам
python campaign_manager.py report --detailed

# Анализ по аккаунтам
grep "account_used" data/first_wave_results.csv | sort | uniq -c
```

## 🚨 Устранение проблем

### Один аккаунт не работает:
```bash
# Проверка конкретного аккаунта
python -c "
from api_test import SBCAttendeesScraper
scraper = SBCAttendeesScraper()
scraper.start()  # Попробует messenger1
scraper.switch_account('messenger2')  # Переключится на messenger2
"
```

### Ошибки переключения:
1. Проверьте credentials в .env
2. Убедитесь что оба аккаунта валидны
3. Проверьте что нет блокировок

### Неравномерное распределение:
- Система автоматически адаптируется при ошибках
- Если один аккаунт заблокирован, вся нагрузка идет на второй
- Можно настроить ACCOUNT_DISTRIBUTION в config

## 💡 Рекомендации

### Для максимальной эффективности:
1. **Используйте разные браузерные профили** для аккаунтов
2. **Настройте разные рабочие часы** (опционально)
3. **Мониторьте логи** на предмет ошибок
4. **Делайте бэкапы** настроек аккаунтов
5. **Тестируйте** перед большими кампаниями

### Типичный рабочий процесс:
```bash
# 1. Проверка готовности
python campaign_manager.py check

# 2. Тест мульти-аккаунта
python first_wave_campaign.py --test-only --test-limit 4

# 3. Анализ тестовых результатов
tail -20 data/first_wave_campaign.log

# 4. Полная кампания
python first_wave_campaign.py

# 5. Мониторинг
tail -f data/first_wave_campaign.log
```

---

**Примечание**: Мульти-аккаунтовая система автоматически активируется для кампаний с более чем 10 контактами. Для меньших кампаний используется один аккаунт.
