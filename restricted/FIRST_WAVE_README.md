# First Wave Campaign - SBC Summit 2025

Система для проведения первой волны сообщений участникам SBC Summit 2025.

## 📋 Описание

Цель: Запустить первую волну сообщений по всем зарегистрированным на SBC, кто еще не получил outreach, включая невалидированных (по position), но относящихся к онлайн типу gaming_vertical или кто не указал но мы ему не рассылали сообщение в SBC.

## 🚀 Быстрый старт

### 1. Подготовка

```bash
# Убедитесь что у вас есть необходимые файлы:
# - restricted/data/SBC - Attendees.csv
# - .env файл с настройками аккаунтов

# Проверьте готовность системы
python restricted/campaign_manager.py check
```

### 2. Предварительный анализ

```bash
# Общий отчет по базе данных
python restricted/campaign_manager.py report --detailed

# Анализ целевой аудитории
python restricted/campaign_manager.py analyze --target

# Экспорт целевой аудитории для проверки
python restricted/campaign_manager.py export -o target_audience_preview.csv
```

### 3. Тестовый запуск

```bash
# Запуск только тестовых сообщений (по умолчанию 3 контакта)
python restricted/first_wave_campaign.py --test-only

# Тестовый запуск с большим количеством контактов
python restricted/first_wave_campaign.py --test-only --test-limit 10
```

### 4. Полная кампания

```bash
# Запуск полной кампании с мульти-аккаунтом (рекомендуется)
python restricted/first_wave_campaign.py

# Запуск с одним аккаунтом
python restricted/first_wave_campaign.py --single-account

# Запуск только полной кампании без теста
python restricted/first_wave_campaign.py --full-only

# Настройка дневного лимита
python restricted/first_wave_campaign.py --daily-limit 200
```

## 📁 Структура файлов

```
restricted/
├── first_wave_campaign.py      # Основной скрипт кампании
├── campaign_config.py          # Конфигурация кампании
├── campaign_manager.py         # Утилита управления
├── api_test.py                 # Базовый скрапер (существующий)
├── config.py                   # Базовая конфигурация (существующий)
└── data/
    ├── SBC - Attendees.csv     # Основная база участников
    ├── first_wave_campaign_log.json    # Лог кампаний
    ├── first_wave_results.csv          # Результаты отправки
    └── target_audience_*.csv           # Экспорты целевой аудитории
```

## 👥 Мульти-аккаунтовая система

### Принцип работы:
- **Автоматическое распределение** контактов между двумя messenger аккаунтами
- **60% контактов** отправляется с messenger1
- **40% контактов** отправляется с messenger2  
- **Автоматическое переключение** между аккаунтами
- **Независимые лимиты** для каждого аккаунта

### Преимущества:
- ✅ **Увеличение пропускной способности** - до 200 сообщений в день
- ✅ **Снижение риска блокировок** - нагрузка распределена
- ✅ **Резервирование** - если один аккаунт заблокирован, работает второй
- ✅ **Параллельная работа** - более быстрая отправка

### Настройка аккаунтов:

В файле `.env` должны быть настроены оба messenger аккаунта:
```env
# Messenger Account 1
MESSENGER1_USERNAME=your_username1
MESSENGER1_PASSWORD=your_password1  
MESSENGER1_USER_ID=your_user_id1

# Messenger Account 2
MESSENGER2_USERNAME=your_username2
MESSENGER2_PASSWORD=your_password2
MESSENGER2_USER_ID=your_user_id2
```

### Управление мульти-аккаунтом:

```bash
# Использовать мульти-аккаунт (по умолчанию)
python first_wave_campaign.py

# Отключить мульти-аккаунт (использовать только messenger1)
python first_wave_campaign.py --single-account
```

### Основные настройки (campaign_config.py)

```python
# Режим работы
TEST_MODE = True                 # Тестовый режим по умолчанию
TEST_CONTACTS_LIMIT = 3         # Количество тестовых контактов

# Ограничения
DAILY_LIMIT = 100               # Максимум сообщений в день
DELAY_BETWEEN_MESSAGES = (3, 7) # Задержка между сообщениями (сек)

# Рабочее время
WORKING_HOURS_START = time(9, 0)  # 09:00
WORKING_HOURS_END = time(18, 0)   # 18:00
WORKING_DAYS = [0, 1, 2, 3, 4]   # Пн-Пт
```

### Фильтры целевой аудитории

Система автоматически применяет следующие фильтры:

1. **Исключение уже отправленных** - контакты со статусом "Sent"
2. **Фильтрация по компаниям** - исключение компаний где уже получен ответ
3. **Gaming Vertical фильтр** - включение онлайн gaming или пустых полей
4. **Исключение land-based** - исключение контактов с "land" в gaming_vertical
5. **Удаление дубликатов** - по user_id
6. **Валидация данных** - обязательные поля: user_id, full_name

## 🏢 Фильтрация по компаниям

### Принцип работы:
- **Компании с ответами**: если кто-то из компании уже ответил, все остальные сотрудники исключаются
- **Автоматическая пометка**: исключенные контакты получают статус "contacted with other worker"
- **Компании без ответа**: если отправляли сообщение, но ответа нет - другие сотрудники остаются доступными

### Статусы обработки компаний:
- **Ответили** - компания исключена из будущих кампаний
- **Отправлено без ответа** - компания доступна для других сотрудников
- **Помечены** - сотрудники отмечены как "contacted with other worker"
- **Непроцессированные** - компании доступные для первичного контакта

## 🎯 Целевая аудитория

### Включаемые критерии:
- **Gaming Vertical**: содержит "online", "casino", "sports betting", "poker", "slots", "bingo", "lottery", "fantasy sports", "esports", "igaming", "betting", "gambling"
- **Пустые Gaming Vertical**: участники без указанного gaming_vertical
- **Не связались ранее**: нет записи в логе кампаний и статус ≠ "Sent"
- **Компании без ответов**: исключены компании где кто-то уже ответил

### Исключаемые критерии:
- **Land-based**: содержит "land", "land-based", "retail", "offline"
- **Уже отправленные**: статус "Sent" в CSV
- **Компании с ответами**: любая компания где получен ответ от сотрудника
- **Дубликаты**: повторяющиеся user_id
- **Неполные данные**: отсутствие user_id или full_name

### Специальные статусы:
- **"contacted with other worker"**: сотрудники из компаний где уже получен ответ

### Приоритетные позиции:
- CEO, COO, CFO, CPO
- Head of Payments, Payments Director
- Business Development
- Country/Regional Manager

## 📧 Шаблоны сообщений

Система использует 4 основных шаблона сообщений с персонализацией по имени. Также есть специализированные шаблоны для разных типов позиций:

- **CEO шаблон** - для руководителей
- **Payments шаблон** - для специалистов по платежам
- **Business Development шаблон** - для BD менеджеров

## 📊 Мониторинг и отчетность

### Команды campaign_manager.py

```bash
# Общий отчет
python restricted/campaign_manager.py report

# Детальный отчет
python restricted/campaign_manager.py report --detailed

# Проверка готовности
python restricted/campaign_manager.py check

# Анализ целевой аудитории
python restricted/campaign_manager.py analyze --target

# Экспорт целевой аудитории
python restricted/campaign_manager.py export

# Очистка дубликатов (просмотр)
python restricted/campaign_manager.py cleanup

# Очистка дубликатов (выполнение)
python restricted/campaign_manager.py cleanup --no-dry-run
```

### Лог файлы

1. **first_wave_campaign.log** - детальный лог выполнения
2. **first_wave_campaign_log.json** - структурированный лог кампаний
3. **first_wave_results.csv** - результаты отправки сообщений

## 🔒 Безопасность и ограничения

### Автоматические ограничения:
- **Дневной лимит**: 100 сообщений в день
- **Задержка**: 3-7 секунд между сообщениями
- **Рабочее время**: только в рабочие часы (настраивается)
- **Повторные попытки**: до 3 попыток при ошибках
- **Адаптивная задержка**: увеличение при обнаружении лимитов

### Безопасность аккаунтов:
- **Распределение нагрузки** между messenger аккаунтами
- **Автоматическое переключение** при блокировке
- **Детекция rate limiting**
- **Максимальное время сессии**: 120 минут

## 📈 Процесс кампании

### Этапы выполнения:

1. **Загрузка данных** - чтение CSV файла участников
2. **Проверка логов** - анализ предыдущих кампаний
3. **Фильтрация** - применение критериев целевой аудитории
4. **Подготовка сообщений** - генерация персонализированных текстов
5. **Инициализация** - подключение к системе отправки
6. **Тестирование** (опционально) - отправка тестовых сообщений
7. **Основная отправка** - массовая рассылка
8. **Логирование** - сохранение результатов
9. **Отчетность** - генерация итогового отчета

### Статусы обработки:

- **Pending** - ожидает отправки
- **Sent** - сообщение отправлено
- **Failed** - ошибка отправки
- **Sent Answer** - получен ответ от участника
- **contacted with other worker** - исключен, так как коллега из компании уже ответил

## 🚨 Устранение неполадок

### Частые проблемы:

1. **"CSV файл не найден"**
   ```bash
   # Проверьте наличие файла
   ls restricted/data/SBC\ -\ Attendees.csv
   ```

2. **"Ошибка логина"**
   ```bash
   # Проверьте .env файл
   cat .env | grep -E "(USERNAME|PASSWORD|USER_ID)"
   ```

3. **"Нет доступных контактов"**
   ```bash
   # Проверьте фильтры
   python restricted/campaign_manager.py analyze --target
   ```

4. **"Rate limit detected"**
   - Система автоматически увеличит задержки
   - Можно переключиться на другой аккаунт
   - Дождаться сброса лимитов

### Логи для диагностики:

```bash
# Последние записи лога
tail -f restricted/data/first_wave_campaign.log

# Поиск ошибок
grep -i error restricted/data/first_wave_campaign.log

# Статистика отправки
grep -i "sent\|failed" restricted/data/first_wave_campaign.log | tail -20
```

## 📋 Чек-лист перед запуском

- [ ] Файл `SBC - Attendees.csv` существует и содержит данные
- [ ] Файл `.env` настроен с валидными аккаунтами
- [ ] Проведена проверка готовности: `python restricted/campaign_manager.py check`
- [ ] Проанализирована целевая аудитория: `python restricted/campaign_manager.py analyze --target`
- [ ] Выполнен тестовый запуск: `python restricted/first_wave_campaign.py --test-only`
- [ ] Проверены результаты теста в логах
- [ ] Настроены параметры в `campaign_config.py` (при необходимости)

## 🔄 Пример полного рабочего процесса

```bash
# 1. Проверка готовности
python restricted/campaign_manager.py check

# 2. Анализ данных
python restricted/campaign_manager.py report --detailed
python restricted/campaign_manager.py analyze --target

# 3. Экспорт для проверки (опционально)
python restricted/campaign_manager.py export -o preview.csv

# 4. Очистка дубликатов (если нужно)
python restricted/campaign_manager.py cleanup
python restricted/campaign_manager.py cleanup --no-dry-run  # после проверки

# 5. Тестовый запуск
python restricted/first_wave_campaign.py --test-only --test-limit 5

# 6. Анализ результатов теста
tail -50 restricted/data/first_wave_campaign.log

# 7. Полная кампания (если тест успешен)
python restricted/first_wave_campaign.py

# 8. Мониторинг выполнения
tail -f restricted/data/first_wave_campaign.log

# 9. Итоговый отчет
python restricted/campaign_manager.py report
```

## ⚡ Важные замечания

1. **Всегда начинайте с тестового режима** для проверки работоспособности
2. **Мониторьте логи** во время выполнения кампании
3. **Соблюдайте лимиты** для предотвращения блокировок
4. **Делайте бэкапы** CSV файлов перед изменениями
5. **Используйте рабочее время** для отправки сообщений
6. **Проверяйте качество данных** перед запуском

## 📞 Поддержка

В случае проблем:

1. Проверьте логи: `restricted/data/first_wave_campaign.log`
2. Запустите диагностику: `python restricted/campaign_manager.py check`
3. Проверьте конфигурацию аккаунтов в `.env`
4. Убедитесь в наличии всех необходимых файлов

---

**Версия**: 1.0  
**Дата**: Сентябрь 2025  
**Совместимость**: Python 3.9+
