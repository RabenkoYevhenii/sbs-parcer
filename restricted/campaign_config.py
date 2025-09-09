"""
Configuration file for First Wave Campaign
Файл конфигурации для первой волны сообщений
"""

from datetime import time
from typing import Dict, List

# =================== ОСНОВНЫЕ НАСТРОЙКИ ===================

# Режим работы
TEST_MODE = True  # Включить тестовый режим по умолчанию
TEST_CONTACTS_LIMIT = 3  # Количество тестовых контактов

# Ограничения по отправке
DAILY_LIMIT = 0  # Без лимитов (0 = неограниченно)
HOURLY_LIMIT = 0  # Без лимитов (0 = неограниченно)
DELAY_BETWEEN_MESSAGES = (
    3,
    7,
)  # Задержка между сообщениями (мин, макс секунды)

# Время работы (в часах, 24-часовой формат) - отключено
WORKING_HOURS_START = time(0, 0)  # 00:00 (работаем круглосуточно)
WORKING_HOURS_END = time(23, 59)  # 23:59

# Дни недели для отправки (0=Понедельник, 6=Воскресенье)
WORKING_DAYS = [0, 1, 2, 3, 4, 5, 6]  # Все дни недели

# =================== ФИЛЬТРЫ ЦЕЛЕВОЙ АУДИТОРИИ ===================

# Ключевые слова для онлайн gaming vertical
ONLINE_GAMING_KEYWORDS = [
    "online",
    "casino",
    "sports betting",
    "poker",
    "slots",
    "bingo",
    "lottery",
    "fantasy sports",
    "esports",
    "igaming",
    "betting",
    "gambling",
]

# Исключенные gaming vertical
EXCLUDED_GAMING_KEYWORDS = ["land", "land-based", "retail", "offline"]

# Приоритетные позиции (для первоочередной отправки)
PRIORITY_POSITIONS = [
    "chief executive officer",
    "ceo",
    "chief operating officer",
    "coo",
    "chief financial officer",
    "cfo",
    "chief payments officer",
    "cpo",
    "head of payments",
    "payments director",
    "business development",
    "bd",
    "partnerships",
    "partner",
    "country manager",
    "regional manager",
]

# Исключенные позиции
EXCLUDED_POSITIONS = [
    "intern",
    "student",
    "assistant",
    "secretary",
    "coordinator",  # но не исключаем если есть другие ключевые слова
]

# =================== ШАБЛОНЫ СООБЩЕНИЙ ===================

# Основные шаблоны первого сообщения
MESSAGE_TEMPLATES = [
    {
        "id": "template_1",
        "weight": 25,  # Вес для случайного выбора (%)
        "text": "Hello {name} !\nI'm thrilled to see you at the SBC Summit in Lisbon the following month! Before things get hectic, it's always a pleasure to connect with other iGaming experts.\nI speak on behalf of Flexify Finance, a company that specializes in smooth payments for high-risk industries. Visit us at Stand E613 if you're looking into new payment options or simply want to discuss innovation.\nWhat is your main objective or priority for the expo this year? I'd love to know what you're thinking about!",
    },
    {
        "id": "template_2",
        "weight": 25,
        "text": "Hi {name} !\nExcited to connect with fellow SBC Summit attendees! I'm representing Flexify Finance - we provide payment solutions specifically designed for iGaming and high-risk industries.\nWe'll be at Stand E613 during the summit in Lisbon. Would love to learn about your current payment challenges or discuss the latest trends in our industry.\nWhat brings you to SBC Summit this year? Any specific goals or connections you're hoping to make?",
    },
    {
        "id": "template_3",
        "weight": 25,
        "text": "Hello {name} !\nLooking forward to the SBC Summit in Lisbon! As someone in the iGaming space, I always enjoy connecting with industry professionals before the event buzz begins.\nI'm with Flexify Finance - we specialize in seamless payment processing for high-risk sectors. Feel free to stop by Stand E613 if you'd like to explore new payment innovations.\nWhat are you most excited about at this year's summit? Any particular sessions or networking goals?",
    },
    {
        "id": "template_4",
        "weight": 25,
        "text": "Hi {name}, looks like we'll both be at SBC Lisbon this month!\nAlways great to meet fellow iGaming pros before the chaos begins.\nI'm with Flexify Finance, a payments provider for high-risk verticals - you'll find us at Stand E613.\nOut of curiosity, what's your main focus at the expo this year ?",
    },
]

# Специальные шаблоны для разных типов контактов
TEMPLATE_VARIANTS = {
    "ceo": [
        "Hello {name}!\nAs a fellow industry leader attending SBC Summit Lisbon, I'd love to connect before the event kicks off.\nI'm representing Flexify Finance at Stand E613 - we're pioneering payment solutions for high-risk verticals like iGaming.\nWould be great to discuss how payment innovation is shaping our industry. What's your strategic focus for the summit this year?"
    ],
    "payments": [
        "Hi {name}!\nSaw you're attending SBC Summit Lisbon - always great to connect with payments professionals in our space!\nI'm with Flexify Finance (Stand E613), where we're solving complex payment challenges for iGaming and high-risk industries.\nWould love to exchange insights on the latest payment trends. What payment innovations are you most excited about this year?"
    ],
    "business_development": [
        "Hello {name}!\nLooking forward to SBC Summit Lisbon! As someone in business development, you probably appreciate the value of early connections.\nI'm representing Flexify Finance at Stand E613 - we're expanding our payment solutions for iGaming operators globally.\nWould be interesting to discuss market trends and partnership opportunities. What regions or verticals are you focusing on this year?"
    ],
}

# =================== ЛОГИРОВАНИЕ И ОТЧЕТНОСТЬ ===================

# Уровни логирования
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR

# Форматы файлов результатов
RESULTS_CSV_COLUMNS = [
    "timestamp",
    "user_id",
    "full_name",
    "position",
    "company",
    "gaming_vertical",
    "message_template_id",
    "message_sent",
    "error",
    "response_time_ms",
]

# Метрики для отслеживания
TRACKED_METRICS = [
    "total_contacts_found",
    "valid_contacts",
    "already_contacted",
    "excluded_by_filters",
    "priority_contacts",
    "messages_sent",
    "messages_failed",
    "success_rate",
    "average_response_time",
]

# =================== НАСТРОЙКИ АККАУНТОВ ===================

# Распределение нагрузки между messenger аккаунтами
ACCOUNT_DISTRIBUTION = {
    "messenger1": 50,  # 50% контактов
    "messenger2": 50,  # 50% контактов
}

# Резервные аккаунты в случае блокировки
BACKUP_ACCOUNTS = ["scraper"]  # Список резервных аккаунтов

# =================== ПРОДВИНУТЫЕ НАСТРОЙКИ ===================

# Интеллектуальная задержка (увеличивается при обнаружении лимитов)
ADAPTIVE_DELAY = True
DELAY_MULTIPLIER_ON_ERROR = 2.0  # Увеличивать задержку в X раз при ошибках

# Повторные попытки
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = (30, 60)  # Задержка между повторными попытками

# Детекция блокировки/лимитов
RATE_LIMIT_DETECTION = True
RATE_LIMIT_KEYWORDS = [
    "rate limit",
    "too many requests",
    "blocked",
    "temporarily unavailable",
    "try again later",
]

# Автоматическое переключение аккаунтов при блокировке
AUTO_SWITCH_ACCOUNT = True

# =================== КАЧЕСТВО ДАННЫХ ===================

# Минимальные требования к качеству контакта
MIN_NAME_LENGTH = 2
REQUIRED_FIELDS = ["user_id", "full_name"]

# Черный список доменов/компаний (если есть email)
BLACKLISTED_DOMAINS = ["test.com", "example.com", "fake.com"]

# Исключенные компании
EXCLUDED_COMPANIES = ["test company", "example corp"]

# =================== БЕЗОПАСНОСТЬ ===================

# Максимальное время работы сессии (в минутах)
MAX_SESSION_DURATION = 120

# Автоматическая остановка при высоком проценте ошибок
AUTO_STOP_ERROR_THRESHOLD = 50  # Остановить если > 50% ошибок

# Мониторинг подозрительной активности
SUSPICIOUS_ACTIVITY_CHECKS = True

# =================== ФУНКЦИИ КОНФИГУРАЦИИ ===================


def get_message_template_by_position(position: str) -> str:
    """Возвращает специализированный шаблон на основе позиции"""
    position_lower = position.lower()

    if any(
        keyword in position_lower for keyword in ["ceo", "chief executive"]
    ):
        return TEMPLATE_VARIANTS.get("ceo", [MESSAGE_TEMPLATES[0]["text"]])[0]
    elif any(
        keyword in position_lower
        for keyword in ["payment", "finance", "treasury"]
    ):
        return TEMPLATE_VARIANTS.get(
            "payments", [MESSAGE_TEMPLATES[1]["text"]]
        )[0]
    elif any(
        keyword in position_lower
        for keyword in ["business development", "bd", "partnership"]
    ):
        return TEMPLATE_VARIANTS.get(
            "business_development", [MESSAGE_TEMPLATES[2]["text"]]
        )[0]
    else:
        # Выбираем случайный основной шаблон
        import random

        weights = [t["weight"] for t in MESSAGE_TEMPLATES]
        template = random.choices(MESSAGE_TEMPLATES, weights=weights)[0]
        return template["text"]


def is_working_time() -> bool:
    """Проверяет, является ли текущее время рабочим (всегда True - работаем без ограничений)"""
    return True  # Работаем круглосуточно без ограничений


def get_current_delay() -> tuple:
    """Возвращает текущую задержку с учетом адаптивности"""
    if ADAPTIVE_DELAY:
        # Здесь можно добавить логику адаптивной задержки
        # на основе текущей нагрузки или ошибок
        return DELAY_BETWEEN_MESSAGES
    return DELAY_BETWEEN_MESSAGES


def should_prioritize_contact(position: str, company: str) -> bool:
    """Определяет, является ли контакт приоритетным"""
    position_lower = position.lower() if position else ""

    return any(priority in position_lower for priority in PRIORITY_POSITIONS)


# =================== ВАЛИДАЦИЯ КОНФИГУРАЦИИ ===================


def validate_config():
    """Валидирует конфигурацию на корректность"""
    errors = []

    # Проверяем временные ограничения
    if WORKING_HOURS_START >= WORKING_HOURS_END:
        errors.append(
            "WORKING_HOURS_START должен быть меньше WORKING_HOURS_END"
        )

    # Проверяем лимиты (0 = безлимитный режим)
    if DAILY_LIMIT < 0 or HOURLY_LIMIT < 0:
        errors.append(
            "Лимиты не могут быть отрицательными. 0 = безлимитный режим"
        )

    if (
        DAILY_LIMIT > 0
        and HOURLY_LIMIT > 0
        and HOURLY_LIMIT * 24 > DAILY_LIMIT
    ):
        errors.append("Часовой лимит * 24 не должен превышать дневной лимит")

    # Проверяем веса шаблонов
    total_weight = sum(t["weight"] for t in MESSAGE_TEMPLATES)
    if total_weight != 100:
        errors.append(
            f"Сумма весов шаблонов должна быть 100, сейчас: {total_weight}"
        )

    # Проверяем распределение аккаунтов
    total_distribution = sum(ACCOUNT_DISTRIBUTION.values())
    if total_distribution != 100:
        errors.append(
            f"Сумма распределения аккаунтов должна быть 100, сейчас: {total_distribution}"
        )

    if errors:
        raise ValueError(
            "Ошибки конфигурации:\n"
            + "\n".join(f"- {error}" for error in errors)
        )


# Автоматическая валидация при импорте
if __name__ != "__main__":
    try:
        validate_config()
    except ValueError as e:
        print(f"⚠️ Предупреждение: {e}")
