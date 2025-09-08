from playwright.sync_api import sync_playwright
import json
import csv
import time
from datetime import datetime, timedelta
import os
import uuid
import re
import random
import shutil
import traceback
from zoneinfo import ZoneInfo
from typing import List, Dict, Set, Tuple, Optional
from config import settings

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Import ContactExtractor from the parent directory
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from extract_contacts import ContactExtractor


class SBCAttendeesScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.is_logged_in = False
        self.current_account = None
        self.existing_chats = {}  # Кеш існуючих чатів {user_id: chat_id}

        # Initialize contact extractor for immediate contact extraction during scraping
        self.contact_extractor = ContactExtractor()

        # Валідація обов'язкових environment variables
        self._validate_env_variables()

        # Конфігурація акаунтів з environment variables
        self.accounts = {
            "scraper": {
                "username": settings.scraper_username,
                "password": settings.scraper_password,
                "user_id": settings.scraper_user_id,
                "name": f"Scraper Account ({settings.scraper_username})",
                "role": "scraping",
            },
            "messenger1": {
                "username": settings.messenger1_username,
                "password": settings.messenger1_password,
                "user_id": settings.messenger1_user_id,
                "name": f"Messenger Account 1 ({settings.messenger1_username})",
                "role": "messaging",
            },
            "messenger2": {
                "username": settings.messenger2_username,
                "password": settings.messenger2_password,
                "user_id": settings.messenger2_user_id,
                "name": f"Messenger Account 2 ({settings.messenger2_username})",
                "role": "messaging",
            },
        }

        # Шаблони повідомлень для follow-up
        self.follow_up_messages = [
            "Hello {name} !\nI'm thrilled to see you at the SBC Summit in Lisbon the following month! Before things get hectic, it's always a pleasure to connect with other iGaming experts.\nI speak on behalf of Flexify Finance, a company that specializes in smooth payments for high-risk industries. Visit us at Stand E613 if you're looking into new payment options or simply want to discuss innovation.\nWhat is your main objective or priority for the expo this year? I'd love to know what you're thinking about!",
            "Hi {name} !\nExcited to connect with fellow SBC Summit attendees! I'm representing Flexify Finance - we provide payment solutions specifically designed for iGaming and high-risk industries.\nWe'll be at Stand E613 during the summit in Lisbon. Would love to learn about your current payment challenges or discuss the latest trends in our industry.\nWhat brings you to SBC Summit this year? Any specific goals or connections you're hoping to make?",
            "Hello {name} !\nLooking forward to the SBC Summit in Lisbon! As someone in the iGaming space, I always enjoy connecting with industry professionals before the event buzz begins.\nI'm with Flexify Finance - we specialize in seamless payment processing for high-risk sectors. Feel free to stop by Stand E613 if you'd like to explore new payment innovations.\nWhat are you most excited about at this year's summit? Any particular sessions or networking goals?",
            "Hi {name}, looks like we'll both be at SBC Lisbon this month!\nAlways great to meet fellow iGaming pros before the chaos begins.\nI'm with Flexify Finance, a payments provider for high-risk verticals - you'll find us at Stand E613.\nOut of curiosity, what's your main focus at the expo this year ?",
        ]

        # Second follow-up message that always gets sent after the first one
        self.second_follow_up_message = (
            "Is payments something on your radar to explore ?"
        )

        # Follow-up messages for response tracking
        self.follow_up_templates = {
            "day_3": "Hello, {name}\nJust to follow up, Flexify Finance will be present at SBC Summit Lisbon at Stand E613. With more than 80 local payment options, we're helping iGaming brands expand in high-risk markets while also holding a prize draw.\nWould you have a few minutes during the expo? It would be great to connect.",
            "day_7": "Hello {name}!\nJust wanted to gently follow up and let you know that Flexify Finance will be at SBC Summit Lisbon (Stand E613). We're supporting iGaming brands in high-risk markets with 80+ local payment options - and we'll also have a fun prize draw at our stand.\nIf you have a few minutes during the expo, I'd really enjoy connecting and having a quick chat.",
            "final": "Hi {name}!\nSBC Summit Lisbon starts tomorrow! 🎉\nFlexify Finance will be at Stand E613 with 80+ local payment solutions for high-risk markets. We'd love to meet you in person and discuss how we can help your iGaming business grow.\nDon't miss our prize draw at the stand! Looking forward to seeing you there.",
        }

        # SBC Summit start date (September 16, 2025) in Kyiv timezone
        kyiv_tz = ZoneInfo("Europe/Kiev")
        self.sbc_start_date = datetime(2025, 9, 16, tzinfo=kyiv_tz)

    def _validate_env_variables(self):
        """Валідує наявність обов'язкових environment variables"""
        required_vars = [
            ("SCRAPER_USERNAME", settings.scraper_username),
            ("SCRAPER_PASSWORD", settings.scraper_password),
            ("SCRAPER_USER_ID", settings.scraper_user_id),
            ("MESSENGER1_USERNAME", settings.messenger1_username),
            ("MESSENGER1_PASSWORD", settings.messenger1_password),
            ("MESSENGER1_USER_ID", settings.messenger1_user_id),
            ("MESSENGER2_USERNAME", settings.messenger2_username),
            ("MESSENGER2_PASSWORD", settings.messenger2_password),
            ("MESSENGER2_USER_ID", settings.messenger2_user_id),
        ]

        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value or var_value.startswith(("MESSENGER", "your_")):
                missing_vars.append(var_name)

        if missing_vars:
            print(
                "❌ ПОМИЛКА КОНФІГУРАЦІЇ: Відсутні обов'язкові environment variables:"
            )
            for var in missing_vars:
                print(f"   - {var}")
            print(
                "\n💡 Створіть файл .env на основі .env.template та заповніть реальні дані:"
            )
            print("   cp .env.template .env")
            print("   # потім відредагуйте .env файл з вашими credentials")
            raise ValueError(
                f"Відсутні environment variables: {', '.join(missing_vars)}"
            )

    def start(self):
        """Запускає браузер і логіниться"""
        print("🚀 Запускаємо браузер...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        )
        self.page = self.context.new_page()

        # Логінимося зі scraper акаунтом за замовчуванням
        return self.login("scraper")

    def login(self, account_key="scraper"):
        """Виконує логін з вказаним акаунтом"""
        if account_key not in self.accounts:
            print(f"❌ Невідомий акаунт: {account_key}")
            return None

        account = self.accounts[account_key]
        self.current_account = account_key

        print("📄 Відкриваємо sbcconnect.com...")
        self.page.goto("https://sbcconnect.com", wait_until="domcontentloaded")
        self.page.wait_for_timeout(5000)

        print(f"🔑 Логінимося з {account['name']}...")
        result = self.page.evaluate(
            f"""
            async () => {{
                const response = await fetch('https://sbcconnect.com/api/account/login', {{
                    method: 'POST',
                    headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{
                        username: '{account['username']}',
                        password: '{account['password']}',
                        rememberMe: true
                    }})
                }});
                const data = await response.json();
                return {{status: response.status, data: data}};
            }}
        """
        )

        if result["status"] == 200:
            print("✅ Успішно залогінилися!")
            self.is_logged_in = True
            return result["data"]
        else:
            print(f"❌ Помилка логіну: {result}")
            return None

    def logout(self):
        """Виконує вихід з поточного акаунта"""
        if not self.is_logged_in:
            print("ℹ️ Вже не залогінені")
            return True

        print("🚪 Виходимо з поточного акаунта...")
        result = self.page.evaluate(
            """
            async () => {
                try {
                    const response = await fetch('https://sbcconnect.com/api/account/logout', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'}
                    });
                    return {status: response.status, data: await response.text()};
                } catch (error) {
                    return {status: 500, error: error.message};
                }
            }
        """
        )

        if (
            result["status"] == 200 or result["status"] == 404
        ):  # 404 може означати що вже вийшли
            print("✅ Успішно вийшли з акаунта")
            self.is_logged_in = False
            self.current_account = None

            # Очищаємо кеш чатів при зміні акаунта
            self.existing_chats.clear()

            return True
        else:
            print(f"⚠️ Помилка при виході: {result}")
            # Навіть якщо logout API не спрацював, очищаємо локальний стан
            self.is_logged_in = False
            self.current_account = None
            self.existing_chats.clear()
            return True

    def api_request(self, method, endpoint, data=None):
        """Виконує API запит через браузер"""
        if not self.is_logged_in:
            print("❌ Спочатку потрібно залогінитися")
            return None

        url = (
            f"https://sbcconnect.com/api/{endpoint}"
            if not endpoint.startswith("http")
            else endpoint
        )

        js_code = """
            async (params) => {
                const {url, method, data} = params;
                const options = {
                    method: method,
                    headers: {
                        'Accept': 'application/json, text/plain, */*',
                        'Content-Type': 'application/json'
                    }
                };
                
                if (data && method !== 'GET') {
                    options.body = JSON.stringify(data);
                }
                
                try {
                    const response = await fetch(url, options);
                    let responseData;
                    const contentType = response.headers.get('content-type');
                    
                    if (contentType && contentType.includes('application/json')) {
                        responseData = await response.json();
                    } else {
                        responseData = await response.text();
                    }
                    
                    return {
                        status: response.status,
                        data: responseData
                    };
                } catch (error) {
                    return {
                        status: 'error',
                        message: error.toString()
                    };
                }
            }
        """

        params = {"url": url, "method": method, "data": data}
        result = self.page.evaluate(js_code, params)

        status = result.get("status", 0)
        if 200 <= status < 300:
            # Для 204 No Content повертаємо True замість даних
            if status == 204:
                return True
            return result.get("data")
        else:
            print(f"   ❌ Помилка {endpoint}: {status}")
            if result.get("data"):
                print(f"      Деталі: {result.get('data')}")
            return None

    def advanced_search(self, from_index=0, size=2000):
        """Виконує advanced search з фільтрами"""
        search_params = {
            "name": None,
            "title": None,
            "companyName": None,
            "countries": [],
            "areaOfResp": [],
            "gamingVerticals": [
                "Online: Sports Betting",
                "Online: Casino",
                "Online: Poker",
                "Online: Slots",
                "Online: Bingo",
                "Online: Lottery",
                "Online: Fantasy Sports",
                "Online: Esports",
            ],
            "organizationTypes": [
                "Operator - Casino/Bookmaker/Sportsbook",
                "Supplier/Service Provider",
                "Affiliate",
                "Sports Organisation",
            ],
        }

        endpoint = f"attendee/advancedSearch?eventPath=sbc-summit-2025&from={from_index}&size={size}"
        return self.api_request("POST", endpoint, search_params)

    def get_all_advanced_search_results(self):
        """Отримує всі результати advanced search"""
        all_results = []
        from_index = 0
        size = 2000  # Максимальний розмір

        while True:
            print(f"\n📥 Завантажуємо з індексу {from_index}...")
            batch = self.advanced_search(from_index, size)

            if not batch:
                print("   ❌ Помилка запиту")
                break

            if isinstance(batch, list):
                if len(batch) == 0:
                    print("   📊 Досягнуто кінця (пустий список)")
                    break

                all_results.extend(batch)
                print(
                    f"   ✅ Отримано {len(batch)} записів (всього: {len(all_results)})"
                )

                if len(batch) < size:
                    print(
                        f"   📊 Досягнуто кінця (отримано {len(batch)} < {size})"
                    )
                    break

                from_index += size
                time.sleep(1)  # Затримка між запитами
            else:
                print(f"   ❌ Неочікувана структура: {type(batch)}")
                break

        return all_results

    def get_user_details(self, user_id):
        """Отримує детальну інформацію про користувача"""
        endpoint = f"user/getById?userId={user_id}&eventPath=sbc-summit-2025"
        return self.api_request("GET", endpoint)

    # =================== MESSAGING METHODS ===================

    def load_chats_list(self):
        """Завантажує список існуючих чатів"""
        endpoint = "chat/LoadChatsList?eventPath=sbc-summit-2025"
        chats_data = self.api_request("GET", endpoint)

        if chats_data and isinstance(chats_data, list):
            # Отримуємо user_id поточного акаунта
            current_user_id = self.accounts[self.current_account]["user_id"]

            # Кешуємо чати для швидкого доступу
            for chat in chats_data:
                chat_id = chat.get("chatId")
                if not chat_id:
                    continue

                # Для single чатів (персональні чати)
                if chat.get("isSingleChat") and chat.get("singleChatDetails"):
                    user_info = chat["singleChatDetails"].get("user", {})
                    other_participant_id = user_info.get("id")

                    if (
                        other_participant_id
                        and other_participant_id != current_user_id
                    ):
                        self.existing_chats[other_participant_id] = chat_id

            print(f"📋 Закешовано {len(self.existing_chats)} існуючих чатів")
            return chats_data
        else:
            return []

    def find_chat_with_user(self, target_user_id: str) -> Optional[str]:
        """Знаходить існуючий чат з користувачем"""
        return self.existing_chats.get(target_user_id)

    def check_chat_has_messages(self, chat_id: str) -> bool:
        """Перевіряє чи є повідомлення в чаті"""
        endpoint = f"chat/LoadChat?chatId={chat_id}"
        chat_data = self.api_request("GET", endpoint)

        if chat_data and isinstance(chat_data, dict):
            messages = chat_data.get("messages", [])

            if messages:
                print(f"       📝 Знайдено {len(messages)} повідомлень у чаті")
                # Показуємо останнє повідомлення для контексту
                last_message = messages[-1]
                last_msg_preview = last_message.get("message", "")[:50] + "..."
                print(f"       📄 Останнє повідомлення: '{last_msg_preview}'")
                return True
            else:
                print(f"       📭 Чат порожній (без повідомлень)")
                return False
        else:
            print(f"       ⚠️ Не вдалося завантажити дані чату")
            return False

    def create_chat(self, target_user_id: str) -> Optional[str]:
        """Створює новий чат з користувачем"""
        current_user_id = self.accounts[self.current_account]["user_id"]

        if not current_user_id:
            return None

        chat_id = str(uuid.uuid4())
        endpoint = "chat/createChat"
        data = {
            "eventPath": "sbc-summit-2025",
            "participants": [current_user_id, target_user_id],
            "chatId": chat_id,
        }

        result = self.api_request("POST", endpoint, data)

        if result is True or result is not None:
            self.existing_chats[target_user_id] = chat_id
            return chat_id
        else:
            return None

    def send_message(self, chat_id: str, message: str) -> bool:
        """Відправляє повідомлення в чат"""
        message_id = str(uuid.uuid4())
        # Створюємо timestamp у форматі UTC з мілісекундами
        from datetime import datetime, timezone

        current_time = (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

        endpoint = "chat/sendMessage"
        data = {
            "chatId": chat_id,
            "messageId": message_id,
            "message": message,
            "createdDate": current_time,
        }

        result = self.api_request("POST", endpoint, data)
        return result is not None

    def send_message_to_user(
        self, target_user_id: str, message: str, full_name: str = None
    ) -> bool:
        """Повний пайплайн відправки повідомлення користувачу з автоматичним follow-up"""
        # 1. Перевіряємо чи є існуючий чат
        chat_id = self.find_chat_with_user(target_user_id)

        if chat_id:
            # 1.1. Якщо чат існує, перевіряємо чи є в ньому повідомлення
            print(
                f"       🔍 Перевіряємо чи є повідомлення в існуючому чаті..."
            )
            if self.check_chat_has_messages(chat_id):
                print(f"       ⏭️ Чат вже містить повідомлення, пропускаємо")
                return False  # Не відправляємо, якщо вже є повідомлення
            else:
                print(
                    f"       ✅ Чат порожній, можна відправляти повідомлення"
                )
        else:
            # 2. Створюємо новий чат
            print(f"       🆕 Створюємо новий чат...")
            chat_id = self.create_chat(target_user_id)
            if not chat_id:
                return False

        # 3. Відправляємо перше повідомлення
        if not self.send_message(chat_id, message):
            return False

        # 4. Чекаємо 5 секунд і відправляємо друге повідомлення
        print(f"       ✅ Перше повідомлення відправлено")
        print(f"       ⏱️ Чекаємо 5 секунд перед другим повідомленням...")
        time.sleep(5)

        # 5. Відправляємо друге повідомлення
        if not self.send_message(chat_id, self.second_follow_up_message):
            print(f"       ⚠️ Не вдалося відправити друге повідомлення")
            return False

        print(
            f"       ✅ Друге повідомлення відправлено: '{self.second_follow_up_message}'"
        )

        # 6. Оновлюємо CSV файл про відправлені повідомлення
        print(f"       📝 Оновлюємо CSV файл...")
        data_dir = "restricted/data"
        csv_file = os.path.join(data_dir, "SBC - Attendees.csv")
        if full_name:
            self.update_csv_with_messaging_status(
                csv_file, target_user_id, full_name, chat_id
            )
        else:
            print(
                f"       ⚠️ Не вдалося оновити CSV - відсутнє ім'я користувача"
            )

        return True

    def load_chat_details(self, chat_id: str) -> Optional[Dict]:
        """Завантажує детальну інформацію про чат"""
        endpoint = f"chat/LoadChat?chatId={chat_id}"
        return self.api_request("GET", endpoint)

    def parse_message_timestamp(
        self, timestamp_str: str
    ) -> Optional[datetime]:
        """Парсить timestamp повідомлення в datetime об'єкт"""
        try:
            # Timestamp format: "2025-09-05T10:30:00.123Z"
            if timestamp_str.endswith("Z"):
                # Remove Z and add UTC timezone
                timestamp_str = timestamp_str[:-1] + "+00:00"

            # Parse the timestamp
            dt = datetime.fromisoformat(timestamp_str)

            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))

            return dt
        except Exception as e:
            print(f"⚠️ Помилка парсингу timestamp {timestamp_str}: {e}")
            return None

    def analyze_chat_for_followup(self, chat_data: Dict) -> Dict:
        """Аналізує чат для визначення необхідності follow-up"""
        result = {
            "needs_followup": False,
            "followup_type": None,
            "first_message_date": None,
            "participant_name": None,
            "participant_id": None,
            "days_since_first": 0,
            "has_response": False,
        }

        if not chat_data or not isinstance(chat_data, dict):
            return result

        messages = chat_data.get("messages", [])
        if not messages:
            return result

        # Отримуємо ID поточного користувача
        current_user_id = self.accounts[self.current_account]["user_id"]

        # Отримуємо інформацію про учасника чату
        if chat_data.get("isSingleChat") and chat_data.get("participants"):
            # Знаходимо учасника, який не є поточним користувачем
            participants = chat_data.get("participants", [])
            for participant in participants:
                if participant.get("userId") != current_user_id:
                    result["participant_id"] = participant.get("userId")
                    result["participant_name"] = (
                        f"{participant.get('firstName', '')} {participant.get('lastName', '')}".strip()
                    )
                    break

        # Сортуємо повідомлення за часом
        sorted_messages = sorted(
            messages, key=lambda x: x.get("createdDate", "")
        )

        # Знаходимо перше повідомлення від нас
        first_our_message = None
        for msg in sorted_messages:
            if msg.get("userId") == current_user_id:
                first_our_message = msg
                break

        if not first_our_message:
            return result

        # Парсимо дату першого повідомлення
        first_message_timestamp = self.parse_message_timestamp(
            first_our_message.get("createdDate", "")
        )
        if not first_message_timestamp:
            return result

        result["first_message_date"] = first_message_timestamp

        # Розраховуємо кількість днів з першого повідомлення
        kyiv_tz = ZoneInfo("Europe/Kiev")

        # Конвертуємо в київський час для консистентності
        if first_message_timestamp.tzinfo is None:
            # Якщо немає timezone info, припускаємо UTC
            first_message_timestamp = first_message_timestamp.replace(
                tzinfo=ZoneInfo("UTC")
            )

        current_time = datetime.now(kyiv_tz)
        first_message_kyiv = first_message_timestamp.astimezone(kyiv_tz)

        days_diff = (current_time.date() - first_message_kyiv.date()).days
        result["days_since_first"] = days_diff

        # Перевіряємо чи є відповіді від учасника після нашого першого повідомлення
        for msg in sorted_messages:
            msg_timestamp = self.parse_message_timestamp(
                msg.get("createdDate", "")
            )
            if msg.get("userId") != current_user_id and msg_timestamp:
                # Конвертуємо msg_timestamp в timezone-aware якщо потрібно
                if msg_timestamp.tzinfo is None:
                    msg_timestamp = msg_timestamp.replace(
                        tzinfo=ZoneInfo("UTC")
                    )

                # Тепер порівнюємо timezone-aware datetimes
                if msg_timestamp > first_message_timestamp:
                    result["has_response"] = True
                    break

        # Якщо немає відповіді, визначаємо тип follow-up
        if not result["has_response"]:
            # За 1 день до SBC (15 вересня)
            sbc_date_kyiv = self.sbc_start_date.astimezone(kyiv_tz)
            days_until_sbc = (sbc_date_kyiv.date() - current_time.date()).days

            if days_until_sbc == 1:  # За 1 день до конференції
                result["needs_followup"] = True
                result["followup_type"] = "final"
            elif days_diff >= 7:  # 7+ днів після першого повідомлення
                result["needs_followup"] = True
                result["followup_type"] = "day_7"
            elif days_diff >= 3:  # 3+ днів після першого повідомлення
                result["needs_followup"] = True
                result["followup_type"] = "day_3"

        return result

    def send_followup_message(
        self, chat_id: str, followup_type: str, participant_name: str
    ) -> bool:
        """Відправляє follow-up повідомлення"""
        if followup_type not in self.follow_up_templates:
            print(f"❌ Невідомий тип follow-up: {followup_type}")
            return False

        # Отримуємо перше ім'я
        first_name = (
            participant_name.split()[0]
            if participant_name.split()
            else "there"
        )

        # Форматуємо повідомлення
        message_template = self.follow_up_templates[followup_type]
        message = message_template.format(name=first_name)

        # Відправляємо повідомлення
        return self.send_message(chat_id, message)

    def process_followup_campaigns(
        self, account_key: str = None
    ) -> Dict[str, int]:
        """Обробляє всі чати для follow-up кампаній"""
        if account_key and account_key != self.current_account:
            print(f"🔄 Переключаємося на акаунт {account_key}...")
            if not self.switch_account(account_key):
                return {"error": 1}

        print(f"\n📬 ОБРОБКА FOLLOW-UP КАМПАНІЙ")
        print(f"👤 Акаунт: {self.accounts[self.current_account]['name']}")
        print("=" * 50)

        # Завантажуємо список чатів
        print("📥 Завантажуємо список чатів...")
        chats_data = self.load_chats_list()

        if not chats_data:
            print("❌ Не вдалося завантажити чати")
            return {"error": 1}

        stats = {
            "total_chats": len(chats_data),
            "analyzed": 0,
            "day_3_sent": 0,
            "day_7_sent": 0,
            "final_sent": 0,
            "has_responses": 0,
            "chat_ids_stored": 0,
            "errors": 0,
        }

        print(f"📊 Знайдено {stats['total_chats']} чатів для аналізу")

        # Підготуємо шлях до CSV файлу для оновлення chat_id
        data_dir = "restricted/data"
        csv_file = os.path.join(data_dir, "SBC - Attendees.csv")

        for i, chat in enumerate(chats_data, 1):
            chat_id = chat.get("chatId")
            if not chat_id:
                continue

            # Пропускаємо групові чати
            if not chat.get("isSingleChat"):
                continue

            print(
                f"\n[{i}/{stats['total_chats']}] Аналізуємо чат {chat_id[:8]}..."
            )

            # Додаємо випадкову затримку між запитами (1-3 секунди)
            delay = random.uniform(1.0, 3.0)
            print(f"   ⏱️ Затримка {delay:.1f}с перед запитом...")
            time.sleep(delay)

            try:
                # Завантажуємо детальну інформацію про чат
                chat_details = self.load_chat_details(chat_id)
                if not chat_details:
                    print("   ⚠️ Не вдалося завантажити деталі чату")
                    stats["errors"] += 1
                    continue

                # Аналізуємо чат
                analysis = self.analyze_chat_for_followup(chat_details)
                stats["analyzed"] += 1

                # Отримуємо базову інформацію про чат для дебагу
                print(
                    f"   🔍 Debug: participant_id={analysis.get('participant_id', 'None')}"
                )
                print(
                    f"   🔍 Debug: participant_name={analysis.get('participant_name', 'None')}"
                )

                if analysis["participant_name"]:
                    print(f"   👤 Учасник: {analysis['participant_name']}")

                if analysis["first_message_date"]:
                    print(
                        f"   📅 Перше повідомлення: {analysis['first_message_date'].strftime('%d.%m.%Y')}"
                    )
                    print(
                        f"   ⏰ Днів з першого повідомлення: {analysis['days_since_first']}"
                    )

                # Зберігаємо chat_id в CSV якщо є participant_id
                if analysis["participant_id"]:
                    success = self.update_csv_with_chat_id(
                        csv_file, analysis["participant_id"], chat_id
                    )
                    if success:
                        print(f"   💾 chat_id збережено в CSV")
                        stats["chat_ids_stored"] += 1
                    else:
                        print(f"   ⚠️ Не вдалося зберегти chat_id в CSV")
                else:
                    print(
                        f"   ⚠️ Відсутній participant_id, chat_id не збережено"
                    )

                if analysis["has_response"]:
                    print("   ✅ Учасник відповів")
                    # Оновлюємо статус в CSV з "Sent" на "Answered"
                    print(
                        f"   🔍 Debug: user_id для оновлення статусу = {analysis['participant_id']}"
                    )
                    if self.update_csv_response_status(
                        csv_file, analysis["participant_id"], True
                    ):
                        print("   📝 Статус оновлено на 'Answered' в CSV")
                    else:
                        print("   ⚠️ Не вдалося оновити статус в CSV")
                    stats["has_responses"] += 1
                    continue

                if analysis["needs_followup"]:
                    followup_type = analysis["followup_type"]
                    print(f"   📨 Потрібен follow-up: {followup_type}")

                    # Перевіряємо чи вже був відправлений цей тип follow-up
                    already_sent = self.check_followup_already_sent(
                        csv_file, chat_id, followup_type
                    )

                    if already_sent:
                        print(
                            f"   ⏭️ Follow-up {followup_type} вже був відправлений"
                        )
                        continue

                    # Відправляємо follow-up
                    success = self.send_followup_message(
                        chat_id,
                        followup_type,
                        analysis["participant_name"] or "there",
                    )

                    if success:
                        print(f"   ✅ Follow-up ({followup_type}) відправлено")
                        stats[f"{followup_type}_sent"] += 1

                        # Оновлюємо Follow-up статус в CSV
                        self.update_csv_followup_status(
                            csv_file, chat_id, followup_type
                        )

                        # Випадкова затримка після відправки повідомлення (2-5 секунд)
                        message_delay = random.uniform(2.0, 5.0)
                        print(
                            f"   ⏱️ Затримка {message_delay:.1f}с після відправки..."
                        )
                        time.sleep(message_delay)
                    else:
                        print(f"   ❌ Помилка відправки follow-up")
                        stats["errors"] += 1
                else:
                    print("   ⏭️ Follow-up не потрібен")

            except Exception as e:
                print(f"   ❌ Помилка обробки чату: {e}")
                stats["errors"] += 1

        # Виводимо підсумки
        print(f"\n📊 ПІДСУМКИ FOLLOW-UP КАМПАНІЇ:")
        print(f"   📋 Проаналізовано чатів: {stats['analyzed']}")
        print(f"   💾 chat_id збережено: {stats['chat_ids_stored']}")
        print(f"   ✅ З відповідями: {stats['has_responses']}")
        print(f"   📨 Follow-up 3 дні: {stats['day_3_sent']}")
        print(f"   📨 Follow-up 7 днів: {stats['day_7_sent']}")
        print(f"   📨 Фінальний follow-up: {stats['final_sent']}")
        print(f"   ❌ Помилок: {stats['errors']}")

        total_sent = (
            stats["day_3_sent"] + stats["day_7_sent"] + stats["final_sent"]
        )
        print(f"   📈 Всього відправлено: {total_sent}")

        return stats

    def get_followup_candidates_from_csv(
        self, csv_file: str = None
    ) -> List[Dict]:
        """Отримує кандидатів для follow-up з CSV файлу"""
        if not csv_file:
            data_dir = "restricted/data"
            csv_file = os.path.join(data_dir, "SBC - Attendees.csv")

        candidates = []

        try:
            import pandas as pd

            if not os.path.exists(csv_file):
                print(f"❌ Файл {csv_file} не знайдено")
                return candidates

            df = pd.read_csv(csv_file)

            # Фільтруємо записи з статусом "Sent" та наявним chat_id
            mask = (
                (df["connected"] == "Sent")
                & (df["chat_id"].notna())
                & (df["chat_id"] != "")
            )

            filtered_df = df[mask]

            print(
                f"📊 Знайдено {len(filtered_df)} кандидатів для follow-up з статусом 'Sent'"
            )

            # Поточна дата в Києві
            kyiv_tz = ZoneInfo("Europe/Kiev")
            current_date = datetime.now(kyiv_tz)

            for _, row in filtered_df.iterrows():
                # Парсимо дату відправки
                date_str = row.get("Date", "")
                if not date_str:
                    continue

                try:
                    # Формат дати: "5.09" (день.місяць)
                    day, month = date_str.split(".")
                    # Припускаємо поточний рік
                    sent_date = datetime(
                        current_date.year, int(month), int(day), tzinfo=kyiv_tz
                    )

                    days_since_sent = (
                        current_date.date() - sent_date.date()
                    ).days

                    # Перевіряємо чи потрібен follow-up
                    needs_followup = False
                    followup_type = None

                    # За 1 день до SBC
                    sbc_date_kyiv = self.sbc_start_date.astimezone(kyiv_tz)
                    days_until_sbc = (
                        sbc_date_kyiv.date() - current_date.date()
                    ).days

                    if days_until_sbc == 1:
                        needs_followup = True
                        followup_type = "final"
                    elif days_since_sent >= 7:
                        needs_followup = True
                        followup_type = "day_7"
                    elif days_since_sent >= 3:
                        needs_followup = True
                        followup_type = "day_3"

                    if needs_followup:
                        candidates.append(
                            {
                                "chat_id": row["chat_id"],
                                "full_name": row["full_name"],
                                "user_id": self.extract_user_id_from_url(
                                    row["source_url"]
                                ),
                                "days_since_sent": days_since_sent,
                                "followup_type": followup_type,
                                "sent_date": sent_date,
                            }
                        )

                except Exception as e:
                    print(f"       ⚠️ Помилка парсингу дати '{date_str}': {e}")
                    continue

            print(f"🎯 З них {len(candidates)} потребують follow-up")
            return candidates

        except ImportError:
            print("❌ pandas не встановлено, використовуємо стару логіку")
            return []
        except Exception as e:
            print(f"❌ Помилка читання CSV: {e}")
            return []

    def extract_user_id_from_url(self, source_url: str) -> str:
        """Витягує user_id з source_url"""
        if not source_url:
            return ""
        try:
            # URL format: https://sbcconnect.com/event/sbc-summit-2025/attendees/{user_id}
            return source_url.split("/")[-1]
        except:
            return ""

    def process_followup_campaigns_optimized(
        self, account_key: str = None
    ) -> Dict[str, int]:
        """Оптимізована обробка follow-up кампаній на основі CSV"""
        if account_key and account_key != self.current_account:
            print(f"🔄 Переключаємося на акаунт {account_key}...")
            if not self.switch_account(account_key):
                return {"error": 1}

        print(f"\n📬 ОПТИМІЗОВАНА ОБРОБКА FOLLOW-UP КАМПАНІЙ")
        print(f"👤 Акаунт: {self.accounts[self.current_account]['name']}")
        print("=" * 50)

        # Отримуємо кандидатів з CSV
        candidates = self.get_followup_candidates_from_csv()

        if not candidates:
            print("✅ Немає кандидатів для follow-up")
            return {
                "total_candidates": 0,
                "analyzed": 0,
                "day_3_sent": 0,
                "day_7_sent": 0,
                "final_sent": 0,
                "status_updated": 0,
                "already_sent": 0,
                "errors": 0,
            }

        stats = {
            "total_candidates": len(candidates),
            "analyzed": 0,
            "day_3_sent": 0,
            "day_7_sent": 0,
            "final_sent": 0,
            "status_updated": 0,
            "already_sent": 0,
            "errors": 0,
        }

        data_dir = "restricted/data"
        csv_file = os.path.join(data_dir, "SBC - Attendees.csv")

        for i, candidate in enumerate(candidates, 1):
            chat_id = candidate["chat_id"]
            full_name = candidate["full_name"]
            user_id = candidate["user_id"]
            followup_type = candidate["followup_type"]
            days_since = candidate["days_since_sent"]

            print(
                f"\n[{i}/{len(candidates)}] {full_name} (chat: {chat_id[:8]}...)"
            )
            print(f"   📅 Днів з відправки: {days_since}")
            print(f"   📨 Тип follow-up: {followup_type}")

            # Додаємо випадкову затримку між запитами (1-3 секунди)
            delay = random.uniform(1.0, 3.0)
            print(f"   ⏱️ Затримка {delay:.1f}с перед запитом...")
            time.sleep(delay)

            try:
                # Завантажуємо детальну інформацію про чат
                chat_details = self.load_chat_details(chat_id)
                if not chat_details:
                    print(f"   ❌ Не вдалося завантажити чат")
                    stats["errors"] += 1
                    continue

                # Аналізуємо чат на предмет відповідей
                analysis = self.analyze_chat_for_followup(chat_details)
                stats["analyzed"] += 1

                # Перевіряємо чи є відповідь
                if analysis["has_response"]:
                    print(f"   ✅ Є відповідь від користувача")
                    # Оновлюємо статус в CSV з "Sent" на "Answered"
                    if self.update_csv_response_status(
                        csv_file, user_id, True
                    ):
                        stats["status_updated"] += 1
                    continue

                # Перевіряємо чи вже був відправлений цей тип follow-up
                already_sent = self.check_followup_already_sent(
                    csv_file, chat_id, followup_type
                )

                if already_sent:
                    print(
                        f"   ⏭️ Follow-up {followup_type} вже був відправлений"
                    )
                    stats["already_sent"] += 1
                    continue

                # Відправляємо follow-up повідомлення
                first_name = (
                    full_name.split()[0] if full_name.split() else "there"
                )

                if self.send_followup_message(
                    chat_id, followup_type, first_name
                ):
                    print(f"   ✅ Follow-up відправлено")
                    stats[f"{followup_type}_sent"] += 1

                    # Оновлюємо Follow-up статус в CSV
                    self.update_csv_followup_status(
                        csv_file, chat_id, followup_type
                    )

                    # Випадкова затримка після відправки повідомлення (2-5 секунд)
                    message_delay = random.uniform(2.0, 5.0)
                    print(
                        f"   ⏱️ Затримка {message_delay:.1f}с після відправки..."
                    )
                    time.sleep(message_delay)
                else:
                    print(f"   ❌ Помилка відправки follow-up")
                    stats["errors"] += 1

            except Exception as e:
                print(f"   ❌ Помилка обробки: {e}")
                stats["errors"] += 1

        # Виводимо підсумки
        print(f"\n📊 ПІДСУМКИ ОПТИМІЗОВАНОЇ FOLLOW-UP КАМПАНІЇ:")
        print(f"   📋 Кандидатів з CSV: {stats['total_candidates']}")
        print(f"   🔍 Проаналізовано: {stats['analyzed']}")
        print(f"   📨 Follow-up 3 дні: {stats['day_3_sent']}")
        print(f"   📨 Follow-up 7 днів: {stats['day_7_sent']}")
        print(f"   📨 Фінальний follow-up: {stats['final_sent']}")
        print(f"   🔄 Статуси оновлено: {stats['status_updated']}")
        print(f"   ⏭️ Вже відправлені: {stats['already_sent']}")
        print(f"   ❌ Помилок: {stats['errors']}")

        total_sent = (
            stats["day_3_sent"] + stats["day_7_sent"] + stats["final_sent"]
        )
        print(f"   📈 Всього відправлено: {total_sent}")

        return stats

    def extract_user_data_from_csv(
        self, csv_file: str, apply_filters: bool = True
    ) -> List[Dict[str, str]]:
        """Витягує user ID та імена з CSV файлу з опціональною фільтрацією"""
        user_data = []

        if not os.path.exists(csv_file):
            print(f"❌ Файл {csv_file} не знайдено")
            return user_data

        try:
            import pandas as pd

            # Читаємо CSV файл з більш толерантними налаштуваннями
            try:
                df = pd.read_csv(csv_file, encoding="utf-8")
                print(f"📊 Загальна кількість записів: {len(df)}")
            except pd.errors.ParserError as e:
                print(f"⚠️ Помилка парсингу CSV (спробуємо виправити): {e}")
                # Спробуємо з іншими параметрами
                try:
                    df = pd.read_csv(
                        csv_file,
                        encoding="utf-8",
                        quoting=1,
                        skipinitialspace=True,
                    )
                    print(
                        f"📊 Загальна кількість записів (після виправлення): {len(df)}"
                    )
                except Exception as e2:
                    print(f"❌ Критична помилка парсингу CSV: {e2}")
                    print("💡 Спробуємо базову обробку без pandas...")
                    raise ImportError("Fallback to basic CSV processing")
            except UnicodeDecodeError:
                print("⚠️ Помилка кодування, спробуємо з іншим кодуванням...")
                try:
                    df = pd.read_csv(csv_file, encoding="latin-1")
                    print(
                        f"📊 Загальна кількість записів (latin-1): {len(df)}"
                    )
                except Exception as e3:
                    print(f"❌ Помилка з усіма кодуваннями: {e3}")
                    raise ImportError("Fallback to basic CSV processing")

            if apply_filters:
                print("🔍 Застосовуємо фільтри...")
                original_count = len(df)

                # 1. Фільтр по порожньому полю 'connected' (якщо колонка існує)
                if "connected" in df.columns:
                    df = df[df["connected"].isna() | (df["connected"] == "")]
                    print(
                        f"   Після фільтру 'connected' (порожнє): {len(df)} записів"
                    )
                else:
                    print(
                        f"   Колонка 'connected' не знайдена, пропускаємо фільтр"
                    )

                # 2. Фільтр по порожньому полю 'Follow-up' (якщо колонка існує)
                if "Follow-up" in df.columns:
                    df = df[df["Follow-up"].isna() | (df["Follow-up"] == "")]
                    print(
                        f"   Після фільтру 'Follow-up' (порожнє): {len(df)} записів"
                    )
                else:
                    print(
                        f"   Колонка 'Follow-up' не знайдена, пропускаємо фільтр"
                    )

                # 3. Фільтр по порожньому полю 'valid' (якщо колонка існує)
                if "valid" in df.columns:
                    df = df[df["valid"].isna() | (df["valid"] == "")]
                    print(
                        f"   Після фільтру 'valid' (порожнє): {len(df)} записів"
                    )
                else:
                    print(
                        f"   Колонка 'valid' не знайдена, пропускаємо фільтр"
                    )

                # 4. Фільтр по gaming_vertical (без "land")
                if "gaming_vertical" in df.columns:
                    df = df[
                        ~df["gaming_vertical"].str.contains(
                            "land", case=False, na=False
                        )
                    ]
                    print(
                        f"   Після фільтру gaming_vertical (без 'land'): {len(df)} записів"
                    )

                # 5. Фільтр по позиції (містить ключові слова)
                position_keywords = [
                    "chief executive officer",
                    "ceo",
                    "chief operating officer",
                    "coo",
                    "chief financial officer",
                    "cfo",
                    "chief payments officer",
                    "cpo",
                    "payments",
                    "psp",
                    "operations",
                    "business development",
                    "partnerships",
                    "relationship",
                    "country manager",
                ]
                if "position" in df.columns:
                    # Конвертуємо позиції в нижній регістр для порівняння
                    df["position_lower"] = (
                        df["position"].str.lower().fillna("")
                    )

                    # Створюємо маску для позицій що містять ключові слова
                    position_mask = df["position_lower"].str.contains(
                        "|".join(position_keywords), case=False, na=False
                    )

                    # Виключаємо "coordinator" для COO
                    coordinator_mask = df["position_lower"].str.contains(
                        "coordinator", case=False, na=False
                    )
                    coo_mask = df["position_lower"].str.contains(
                        "coo", case=False, na=False
                    )

                    # Застосовуємо фільтр: включаємо позиції з ключовими словами, але виключаємо coordinator при COO
                    df = df[position_mask & ~(coo_mask & coordinator_mask)]

                    # Видаляємо тимчасову колонку
                    df = df.drop("position_lower", axis=1)

                    print(
                        f"   Після фільтру позиції (ключові слова, виключаючи COO+coordinator): {len(df)} записів"
                    )

                print(
                    f"📊 Відфільтровано: {original_count} → {len(df)} записів"
                )

            # Перетворюємо в список користувачів
            for _, row in df.iterrows():
                source_url = row.get("source_url", "")
                full_name = row.get("full_name", "")

                if source_url and full_name:
                    # Витягуємо user ID з URL
                    match = re.search(r"/attendees/([^/?]+)", source_url)
                    if match:
                        user_id = match.group(1)

                        # Витягуємо перше ім'я
                        first_name = (
                            full_name.split()[0]
                            if full_name.split()
                            else "there"
                        )

                        user_data.append(
                            {
                                "user_id": user_id,
                                "first_name": first_name,
                                "full_name": full_name,
                            }
                        )

        except ImportError:
            print(
                "⚠️ pandas не встановлено або помилка парсингу, використовуємо базову обробку..."
            )
            try:
                with open(csv_file, "r", encoding="utf-8") as f:
                    # Спочатку читаємо заголовки
                    first_line = f.readline().strip()
                    headers = first_line.split(",")

                    # Перевіряємо чи є потрібні колонки
                    if (
                        "source_url" not in headers
                        or "full_name" not in headers
                    ):
                        print(
                            "❌ Не знайдено обов'язкові колонки 'source_url' або 'full_name'"
                        )
                        return user_data

                    source_url_idx = headers.index("source_url")
                    full_name_idx = headers.index("full_name")

                    line_num = 1
                    for line in f:
                        line_num += 1
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            # Простий спліт по комі (може не працювати ідеально, але краще ніж нічого)
                            fields = line.split(",")

                            # Перевіряємо чи достатньо полів
                            if len(fields) > max(
                                source_url_idx, full_name_idx
                            ):
                                source_url = (
                                    fields[source_url_idx].strip().strip('"')
                                )
                                full_name = (
                                    fields[full_name_idx].strip().strip('"')
                                )

                                if source_url and full_name:
                                    # Витягуємо user ID з URL
                                    match = re.search(
                                        r"/attendees/([^/?]+)", source_url
                                    )
                                    if match:
                                        user_id = match.group(1)

                                        # Витягуємо перше ім'я
                                        first_name = (
                                            full_name.split()[0]
                                            if full_name.split()
                                            else "there"
                                        )

                                        user_data.append(
                                            {
                                                "user_id": user_id,
                                                "first_name": first_name,
                                                "full_name": full_name,
                                            }
                                        )
                        except Exception as line_error:
                            print(
                                f"⚠️ Пропускаємо пошкоджений рядок {line_num}: {str(line_error)[:50]}..."
                            )
                            continue

            except Exception as file_error:
                print(f"❌ Помилка читання файлу: {file_error}")
                try:
                    # Спробуємо з іншим кодуванням
                    with open(csv_file, "r", encoding="latin-1") as f:
                        reader = csv.DictReader(f)
                        for row_num, row in enumerate(reader, 2):
                            try:
                                source_url = row.get("source_url", "")
                                full_name = row.get("full_name", "")

                                if source_url and full_name:
                                    # Витягуємо user ID з URL
                                    match = re.search(
                                        r"/attendees/([^/?]+)", source_url
                                    )
                                    if match:
                                        user_id = match.group(1)

                                        # Витягуємо перше ім'я
                                        first_name = (
                                            full_name.split()[0]
                                            if full_name.split()
                                            else "there"
                                        )

                                        user_data.append(
                                            {
                                                "user_id": user_id,
                                                "first_name": first_name,
                                                "full_name": full_name,
                                            }
                                        )
                            except Exception as row_error:
                                print(
                                    f"⚠️ Пропускаємо пошкоджений рядок {row_num}: {str(row_error)[:50]}..."
                                )
                                continue
                except Exception as final_error:
                    print(f"❌ Критична помилка обробки файлу: {final_error}")
                    return user_data

        print(f"📋 Знайдено {len(user_data)} користувачів для обробки")
        return user_data

    def fix_malformed_csv(self, csv_file: str, backup: bool = True) -> bool:
        """Виправляє пошкоджений CSV файл"""
        import shutil

        if backup:
            backup_file = f"{csv_file}.backup"
            try:
                shutil.copy2(csv_file, backup_file)
                print(f"📁 Створено backup: {backup_file}")
            except Exception as e:
                print(f"⚠️ Не вдалося створити backup: {e}")

        try:
            fixed_rows = []

            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

            if not lines:
                print("❌ Файл порожній")
                return False

            # Перший рядок - заголовки
            header_line = lines[0].strip()
            headers = [h.strip().strip('"') for h in header_line.split(",")]
            expected_fields = len(headers)

            print(f"📊 Очікується {expected_fields} полів на рядок")
            print(f"📋 Заголовки: {', '.join(headers[:5])}...")

            fixed_rows.append(header_line)

            for line_num, line in enumerate(lines[1:], 2):
                line = line.strip()
                if not line:
                    continue

                # Підраховуємо поля
                fields = line.split(",")

                if len(fields) == expected_fields:
                    # Рядок правильний
                    fixed_rows.append(line)
                elif len(fields) > expected_fields:
                    # Занадто багато полів - можливо незахищені коми в даних
                    print(
                        f"⚠️ Рядок {line_num}: {len(fields)} полів замість {expected_fields}"
                    )

                    # Спробуємо зберегти тільки перші потрібні поля
                    fixed_line = ",".join(fields[:expected_fields])
                    fixed_rows.append(fixed_line)
                    print(f"✅ Виправлено рядок {line_num}")
                else:
                    # Замало полів - пропускаємо
                    print(
                        f"❌ Пропускаємо рядок {line_num}: тільки {len(fields)} полів"
                    )
                    continue

            # Записуємо виправлений файл
            with open(csv_file, "w", encoding="utf-8", newline="") as f:
                for row in fixed_rows:
                    f.write(row + "\n")

            print(
                f"✅ Файл виправлено. Збережено {len(fixed_rows)-1} рядків даних"
            )
            return True

        except Exception as e:
            print(f"❌ Помилка виправлення файлу: {e}")
            return False

    def update_csv_with_messaging_status(
        self, csv_file: str, user_id: str, full_name: str, chat_id: str = None
    ):
        """Оновлює CSV файл з інформацією про відправлене повідомлення"""
        try:
            # Читаємо весь CSV файл
            import pandas as pd

            df = pd.read_csv(csv_file)

            # Знаходимо запис за user_id (витягуємо з source_url)
            mask = df["source_url"].str.contains(user_id, na=False)

            if mask.any():
                # Визначаємо автора на основі поточного акаунта
                if self.current_account == "messenger1":
                    author = "Daniil"
                elif self.current_account == "messenger2":
                    author = "Yaroslav"
                else:
                    author = "System"

                # Отримуємо поточну дату у форматі d.mm за київським часом
                kyiv_tz = ZoneInfo("Europe/Kiev")
                current_date = datetime.now(kyiv_tz).strftime("%-d.%m")

                # Оновлюємо поля
                df.loc[mask, "connected"] = "Sent"
                df.loc[mask, "Comment"] = (
                    author  # Використовуємо Comment як author field
                )
                df.loc[mask, "Date"] = current_date

                # Зберігаємо chat_id якщо надано
                if chat_id:
                    df.loc[mask, "chat_id"] = chat_id

                # Зберігаємо оновлений файл
                df.to_csv(csv_file, index=False, encoding="utf-8")

                chat_info = f", chat_id={chat_id}" if chat_id else ""
                print(
                    f"       📝 CSV оновлено: connected=Sent, author={author}, date={current_date}{chat_info}"
                )
            else:
                print(
                    f"       ⚠️ Не знайдено запис для user_id {user_id} у CSV"
                )

        except ImportError:
            print(f"       ⚠️ pandas не встановлено, CSV не оновлено")
        except Exception as e:
            print(f"       ❌ Помилка оновлення CSV: {e}")

    def update_csv_response_status(
        self, csv_file: str, user_id: str, has_response: bool
    ):
        """Оновлює статус відповіді в CSV файлі за user_id"""
        try:
            import pandas as pd

            df = pd.read_csv(csv_file)
            print(f"       🔍 Debug: шукаємо user_id '{user_id}' в CSV")

            # Знаходимо запис за user_id (витягуємо з source_url)
            mask = df["source_url"].str.contains(user_id, na=False)
            matching_records = mask.sum()
            print(
                f"       🔍 Debug: знайдено {matching_records} записів з таким user_id"
            )

            if mask.any():
                current_status = df.loc[mask, "connected"].iloc[0]
                print(f"       🔍 Debug: поточний статус = '{current_status}'")

                # Оновлюємо статус з "Sent" на "Answered" якщо є відповідь
                if has_response and current_status == "Sent":
                    df.loc[mask, "connected"] = "Answered"

                    # Зберігаємо оновлений файл
                    df.to_csv(csv_file, index=False, encoding="utf-8")

                    print(
                        f"       📝 Оновлено статус: Sent → Answered для user_id {user_id}"
                    )
                    return True
                elif not has_response:
                    print(f"       ℹ️ has_response=False, статус не змінюється")
                    return True
                elif current_status != "Sent":
                    print(
                        f"       ℹ️ Статус '{current_status}' != 'Sent', оновлення не потрібно"
                    )
                    return True
                else:
                    print(
                        f"       ⚠️ Невідома причина, чому статус не оновився"
                    )
                    return False
            else:
                print(
                    f"       ⚠️ Запис з user_id '{user_id}' не знайдено в CSV"
                )
                return False

        except ImportError:
            print(f"       ⚠️ pandas не встановлено, статус не оновлено")
            return False
        except Exception as e:
            print(f"       ❌ Помилка оновлення статусу: {e}")
            return False

    def update_csv_with_chat_id(
        self, csv_file: str, user_id: str, chat_id: str
    ):
        """Оновлює CSV файл з chat_id для конкретного користувача"""
        try:
            import pandas as pd

            df = pd.read_csv(csv_file)

            # Знаходимо запис за user_id (витягуємо з source_url)
            mask = df["source_url"].str.contains(user_id, na=False)

            if mask.any():
                # Перевіряємо чи вже є chat_id
                current_chat_id = df.loc[mask, "chat_id"].iloc[0]

                if (
                    pd.isna(current_chat_id)
                    or current_chat_id == ""
                    or current_chat_id != chat_id
                ):
                    # Оновлюємо chat_id
                    df.loc[mask, "chat_id"] = chat_id

                    # Зберігаємо оновлений файл
                    df.to_csv(csv_file, index=False, encoding="utf-8")

                    print(
                        f"       📝 Оновлено chat_id: {chat_id[:8]}... для user_id {user_id}"
                    )
                    return True
                else:
                    print(
                        f"       ℹ️ chat_id вже встановлено для user_id {user_id}"
                    )
                    return True
            else:
                print(
                    f"       ⚠️ Не знайдено запис для user_id {user_id} у CSV"
                )
                return False

        except ImportError:
            print(f"       ⚠️ pandas не встановлено, chat_id не оновлено")
            return False
        except Exception as e:
            print(f"       ❌ Помилка оновлення chat_id: {e}")
            return False

    def update_csv_followup_status(
        self, csv_file: str, chat_id: str, followup_type: str
    ):
        """Оновлює статус Follow-up в CSV файлі після відправки"""
        try:
            import pandas as pd
            from zoneinfo import ZoneInfo

            df = pd.read_csv(csv_file)

            # Знаходимо запис за chat_id
            mask = df["chat_id"] == chat_id

            if mask.any():
                # Оновлюємо Follow-up колонку
                df.loc[mask, "Follow-up"] = "true"

                # Додаємо інформацію про тип follow-up до коментаря
                current_comment = df.loc[mask, "Comment"].iloc[0]
                if pd.isna(current_comment) or current_comment == "":
                    new_comment = f"follow-up_{followup_type}"
                else:
                    new_comment = (
                        f"{current_comment}, follow-up_{followup_type}"
                    )

                df.loc[mask, "Comment"] = new_comment

                # ВАЖЛИВО: Записуємо дату відправки follow-up
                kyiv_tz = ZoneInfo("Europe/Kiev")
                current_date = datetime.now(kyiv_tz).strftime("%-d.%m")

                # Додаємо колонку follow_up_date якщо її немає
                if "follow_up_date" not in df.columns:
                    df["follow_up_date"] = ""

                df.loc[mask, "follow_up_date"] = current_date

                # Зберігаємо оновлений файл
                df.to_csv(csv_file, index=False, encoding="utf-8")

                print(
                    f"       📝 Follow-up статус оновлено: {followup_type}, дата: {current_date}"
                )
                return True
            else:
                print(f"       ⚠️ Не знайдено запис з chat_id {chat_id}")
                return False

        except ImportError:
            print(
                f"       ⚠️ pandas не встановлено, Follow-up статус не оновлено"
            )
            return False
        except Exception as e:
            print(f"       ❌ Помилка оновлення Follow-up статусу: {e}")
            return False

    def check_followup_already_sent(
        self, csv_file: str, chat_id: str, followup_type: str
    ) -> bool:
        """Перевіряє чи вже був відправлений follow-up цього типу"""
        try:
            import pandas as pd

            df = pd.read_csv(csv_file)

            # Знаходимо запис за chat_id
            mask = df["chat_id"] == chat_id

            if mask.any():
                comment = df.loc[mask, "Comment"].iloc[0]
                if pd.isna(comment):
                    return False

                # Перевіряємо чи містить коментар інформацію про цей тип follow-up
                return f"follow-up_{followup_type}" in str(comment)

            return False

        except ImportError:
            return False
        except Exception as e:
            print(f"       ⚠️ Помилка перевірки follow-up: {e}")
            return False

    def bulk_message_users_from_csv(
        self, csv_file: str, delay_seconds: int = 3, user_limit: int = None
    ):
        """Відправляє повідомлення всім користувачам з CSV файлу"""
        print(f"\n📬 РОЗСИЛКА ПОВІДОМЛЕНЬ З ФАЙЛУ: {csv_file}")

        # Завантажуємо існуючі чати
        print("📥 Завантажуємо існуючі чати...")
        self.load_chats_list()

        # Витягуємо дані користувачів з CSV
        user_data = self.extract_user_data_from_csv(csv_file)

        if not user_data:
            print("❌ Не знайдено користувачів для обробки")
            return 0, 0

        # Застосовуємо ліміт користувачів якщо вказано
        if user_limit and user_limit > 0:
            if len(user_data) > user_limit:
                original_count = len(user_data)
                user_data = user_data[:user_limit]
                print(
                    f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {original_count} доступних користувачів"
                )

        success_count = 0
        failed_count = 0
        skipped_count = 0

        for i, user_info in enumerate(user_data, 1):
            user_id = user_info["user_id"]
            first_name = user_info["first_name"]
            full_name = user_info["full_name"]

            print(
                f"\n[{i}/{len(user_data)}] Обробляємо {full_name} (ID: {user_id})..."
            )

            try:
                # Вибираємо випадкове повідомлення з шаблонів та підставляємо ім'я
                message_template = random.choice(self.follow_up_messages)
                message = message_template.format(name=first_name)

                print(
                    f"   💬 Відправляємо: '{message_template[:50]}...' з ім'ям '{first_name}'"
                )
                print(
                    f"   💬 + автоматичний follow-up: '{self.second_follow_up_message}'"
                )

                # Відправляємо повідомлення (з автоматичним follow-up та перевіркою чату)
                success = self.send_message_to_user(
                    user_id, message, full_name
                )

                if success:
                    print(f"   ✅ Повідомлення відправлено")
                    success_count += 1
                elif success is False:
                    # Якщо повертається False, це означає що чат має повідомлення і ми пропускаємо
                    print(f"   ⏭️ Пропущено (чат вже має повідомлення)")
                    skipped_count += 1
                else:
                    print(f"   ❌ Помилка відправки")
                    failed_count += 1

                # Затримка між повідомленнями
                if i < len(user_data):
                    print(f"   ⏱️ Чекаємо {delay_seconds} секунд...")
                    time.sleep(delay_seconds)

            except Exception as e:
                print(f"   ❌ Помилка: {e}")
                failed_count += 1

        print(f"\n📊 ПІДСУМОК РОЗСИЛКИ:")
        print(f"   ✅ Успішно: {success_count}")
        print(f"   ⏭️ Пропущено: {skipped_count}")
        print(f"   ❌ Помилок: {failed_count}")
        print(
            f"   📈 Успішність: {(success_count/(success_count+failed_count)*100):.1f}%"
            if (success_count + failed_count) > 0
            else "N/A"
        )

        return success_count, failed_count

    def bulk_message_multi_account(
        self, csv_file: str, delay_seconds: int = 3, user_limit: int = None
    ):
        """Відправляє повідомлення з CSV файлу розподіляючи дані між двома messenger акаунтами"""
        print(
            f"\n📬 РОЗСИЛКА ПОВІДОМЛЕНЬ З ДВОХ MESSENGER АКАУНТІВ: {csv_file}"
        )

        # Витягуємо дані користувачів з CSV
        user_data = self.extract_user_data_from_csv(csv_file)

        if not user_data:
            print("❌ Не знайдено користувачів для обробки")
            return 0, 0

        # Застосовуємо ліміт користувачів якщо вказано
        if user_limit and user_limit > 0:
            if len(user_data) > user_limit:
                user_data = user_data[:user_limit]
                print(
                    f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {len(self.extract_user_data_from_csv(csv_file))} доступних користувачів"
                )

        # Розділяємо дані навпіл між messenger акаунтами
        mid_point = len(user_data) // 2
        messenger1_data = user_data[:mid_point]
        messenger2_data = user_data[mid_point:]

        print(f"📊 Розподіл контактів:")
        print(
            f"   👤 Messenger1 ({self.accounts['messenger1']['name']}): {len(messenger1_data)} контактів"
        )
        print(
            f"   👤 Messenger2 ({self.accounts['messenger2']['name']}): {len(messenger2_data)} контактів"
        )

        total_success = 0
        total_failed = 0

        # Обробляємо першим messenger акаунтом
        if messenger1_data:
            print(f"\n🔄 Переключаємося на Messenger1...")
            self.switch_account("messenger1")
            success, failed = self._process_user_batch(
                messenger1_data, delay_seconds, "Messenger1"
            )
            total_success += success
            total_failed += failed

        # Обробляємо другим messenger акаунтом
        if messenger2_data:
            print(f"\n🔄 Переключаємося на Messenger2...")
            self.switch_account("messenger2")

            # Додаткова затримка після переключення акаунтів
            print(f"   ⏱️ Чекаємо 5 секунд після переключення акаунта...")
            time.sleep(5)

            success, failed = self._process_user_batch(
                messenger2_data, delay_seconds, "Messenger2"
            )
            total_success += success
            total_failed += failed

        print(f"\n📊 ЗАГАЛЬНИЙ ПІДСУМОК:")
        print(f"   ✅ Успішно: {total_success}")
        print(f"   ❌ Помилок: {total_failed}")
        print(
            f"   📈 Успішність: {(total_success/(total_success+total_failed)*100):.1f}%"
            if (total_success + total_failed) > 0
            else "N/A"
        )

        return total_success, total_failed

    def switch_account(self, account_key):
        """Переключає на інший акаунт"""
        if account_key not in self.accounts:
            print(f"❌ Невідомий акаунт: {account_key}")
            return False

        if self.current_account == account_key:
            print(f"ℹ️ Вже використовується {account_key}")
            return True

        print(
            f"🔄 Переключаємося з {self.current_account} на {account_key}..."
        )

        # Спочатку виходимо з поточного акаунта
        if self.current_account is not None:
            self.logout()

        # Тепер логінимося з новим акаунтом
        success = self.login(account_key)
        if success:
            print(
                f"✅ Успішно переключилися на {self.accounts[account_key]['name']}"
            )
            return True
        else:
            print(f"❌ Помилка переключення на {account_key}")
            return False

    def _process_user_batch(self, user_data, delay_seconds, account_name):
        """Обробляє групу користувачів для одного акаунта"""
        print(f"\n📬 Обробка {len(user_data)} контактів для {account_name}")

        # Завантажуємо існуючі чати для поточного акаунта
        print("📥 Завантажуємо існуючі чати...")
        self.load_chats_list()

        success_count = 0
        failed_count = 0
        skipped_count = 0

        for i, user_info in enumerate(user_data, 1):
            user_id = user_info["user_id"]
            first_name = user_info["first_name"]
            full_name = user_info["full_name"]

            print(
                f"\n[{i}/{len(user_data)}] Обробляємо {full_name} (ID: {user_id})..."
            )

            try:
                # Використовуємо звичайні повідомлення (з автоматичним follow-up)
                message_template = random.choice(self.follow_up_messages)
                message = message_template.format(name=first_name)

                print(
                    f"   💬 Відправляємо: '{message_template[:50]}...' з ім'ям '{first_name}'"
                )
                print(
                    f"   💬 + автоматичний follow-up: '{self.second_follow_up_message}'"
                )

                # Відправляємо повідомлення (з автоматичним follow-up та перевіркою чату)
                success = self.send_message_to_user(
                    user_id, message, full_name
                )

                if success:
                    print(f"   ✅ Повідомлення відправлено")
                    success_count += 1
                elif success is False:
                    # Якщо повертається False, це означає що чат має повідомлення і ми пропускаємо
                    print(f"   ⏭️ Пропущено (чат вже має повідомлення)")
                    skipped_count += 1
                else:
                    print(f"   ❌ Помилка відправки")
                    failed_count += 1

                # Затримка між повідомленнями
                if i < len(user_data):
                    print(f"   ⏱️ Чекаємо {delay_seconds} секунд...")
                    time.sleep(delay_seconds)

            except Exception as e:
                print(f"   ❌ Помилка: {e}")
                failed_count += 1

        print(f"\n📊 ПІДСУМОК для {account_name}:")
        print(f"   ✅ Успішно: {success_count}")
        print(f"   ⏭️ Пропущено: {skipped_count}")
        print(f"   ❌ Помилок: {failed_count}")

        return success_count, failed_count

    def load_existing_attendees(self, csv_file="SBC - Attendees.csv"):
        """Завантажує існуючі записи з CSV"""
        existing = set()

        if not os.path.exists(csv_file):
            print(f"⚠️ Файл {csv_file} не знайдено")
            return existing

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Створюємо унікальний ключ з full_name та company_name
                full_name = row.get("full_name", "").strip().lower()
                company = row.get("company_name", "").strip().lower()

                if full_name:  # Мінімум потрібне ім'я
                    key = f"{full_name}|{company}"
                    existing.add(key)

        print(f"📋 Завантажено {len(existing)} існуючих записів з {csv_file}")
        return existing

    def find_new_attendees(self, search_results, existing_keys):
        """Знаходить нових учасників"""
        new_attendees = []

        for attendee in search_results:
            # Формуємо full_name
            first_name = (attendee.get("firstName") or "").strip()
            last_name = (attendee.get("lastName") or "").strip()
            full_name = f"{first_name} {last_name}".strip().lower()

            # Отримуємо company
            company = (attendee.get("companyName") or "").strip().lower()

            # Створюємо ключ
            if full_name:
                key = f"{full_name}|{company}"

                if key not in existing_keys:
                    new_attendees.append(attendee)

        return new_attendees

    def format_attendee_for_csv(self, attendee_details):
        """Форматує дані учасника для CSV"""
        # Перевіряємо чи є userProfile (детальні дані) чи це базові дані
        if "userProfile" in attendee_details:
            # Детальні дані з API get_user_details
            profile = attendee_details["userProfile"]
            first_name = profile.get("firstName", "")
            last_name = profile.get("lastName", "")
            full_name = f"{first_name} {last_name}".strip()

            # Extract contacts from introduction text immediately
            introduction_text = profile.get("introduction", "")
            other_contacts = ""
            if introduction_text:
                contacts = self.contact_extractor.extract_contacts_from_text(
                    introduction_text
                )
                other_contacts = (
                    ", ".join(sorted(contacts)) if contacts else ""
                )

            return {
                "full_name": full_name,
                "company_name": profile.get("companyName", ""),
                "position": profile.get("jobTitle", ""),
                "linkedin_url": profile.get("linkedInUrl", ""),
                "facebook_url": profile.get("facebookUrl", ""),
                "x_twitter_url": profile.get("twitterUrl", ""),
                "other_socials": profile.get(
                    "website", ""
                ),  # Використовуємо website як додаткову соціальну мережу
                "other_contacts": other_contacts,  # Add extracted contacts immediately
                "country": profile.get("country", ""),
                "responsibility": profile.get("areaOfResponsibility", ""),
                "gaming_vertical": profile.get("mainGamingVertical", ""),
                "organization_type": profile.get("organizationType", ""),
                "introduction": introduction_text,
                "source_url": f"https://sbcconnect.com/event/sbc-summit-2025/attendees/{attendee_details.get('userId', '')}",
                "profile_image_url": profile.get("photoUrl", ""),
            }
        else:
            # Базові дані з advanced search
            first_name = attendee_details.get("firstName", "")
            last_name = attendee_details.get("lastName", "")
            full_name = f"{first_name} {last_name}".strip()

            return {
                "full_name": full_name,
                "company_name": attendee_details.get("companyName", ""),
                "position": attendee_details.get("jobTitle", ""),
                "linkedin_url": "",  # Ці поля доступні тільки в детальних даних
                "facebook_url": "",
                "x_twitter_url": "",
                "other_socials": "",
                "other_contacts": "",  # Empty for basic data without introduction
                "country": "",
                "responsibility": "",
                "gaming_vertical": "",
                "organization_type": "",
                "introduction": "",
                "source_url": f"https://sbcconnect.com/event/sbc-summit-2025/attendees/{attendee_details.get('userId', '')}",
                "profile_image_url": attendee_details.get("photoUrl", ""),
            }

    def save_new_attendees(
        self, new_attendees_data, csv_file="SBC - Attendees.csv"
    ):
        """Додає нових учасників до CSV файлу"""
        file_exists = os.path.exists(csv_file)

        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            fieldnames = [
                "full_name",
                "company_name",
                "position",
                "linkedin_url",
                "facebook_url",
                "x_twitter_url",
                "other_socials",
                "other_contacts",  # Add other_contacts field to CSV structure
                "country",
                "responsibility",
                "gaming_vertical",
                "organization_type",
                "introduction",
                "source_url",
                "profile_image_url",
                "connected",  # Додаємо колонки для messaging
                "Follow-up",
                "valid",
                "Comment",
                "Date",
                "chat_id",  # Новий стовпець для зберігання chat_id
            ]

            writer = csv.DictWriter(f, fieldnames=fieldnames)

            # Пишемо заголовки якщо файл новий
            if not file_exists:
                writer.writeheader()

            # Записуємо дані
            for attendee in new_attendees_data:
                # Додаємо порожні значення для нових колонок
                attendee_with_messaging_fields = attendee.copy()
                attendee_with_messaging_fields.update(
                    {
                        "connected": "",
                        "Follow-up": "",
                        "valid": "",
                        "Comment": "",
                        "Date": "",
                    }
                )
                writer.writerow(attendee_with_messaging_fields)

        print(
            f"💾 Додано {len(new_attendees_data)} нових записів до {csv_file}"
        )

    def process_new_attendees(self, new_attendees):
        """Обробляє нових учасників - отримує детальні дані"""
        detailed_data = []
        total = len(new_attendees)

        print(f"\n🔍 Отримуємо детальні дані для {total} нових учасників...")

        for i, attendee in enumerate(new_attendees, 1):
            user_id = attendee.get("userId")
            if not user_id:
                print(f"   [{i}/{total}] ⚠️ Пропускаємо запис без userId")
                continue

            full_name = f"{attendee.get('firstName', '')} {attendee.get('lastName', '')}".strip()
            print(
                f"   [{i}/{total}] Обробляємо {full_name} (ID: {user_id})..."
            )

            # Отримуємо детальні дані
            details = self.get_user_details(user_id)

            if details and isinstance(details, dict):
                # Перевіряємо чи є userProfile в відповіді
                if "userProfile" in details:
                    print(f"       ✅ Отримано детальні дані")
                    formatted = self.format_attendee_for_csv(details)
                    # Show contact extraction feedback
                    if formatted.get("other_contacts"):
                        print(
                            f"       📞 Знайдено контакти: {formatted['other_contacts']}"
                        )
                    else:
                        print(f"       📞 Контакти не знайдено")
                else:
                    print(
                        f"       ⚠️ Немає userProfile, використовуємо базові дані"
                    )
                    formatted = self.format_attendee_for_csv(attendee)
                detailed_data.append(formatted)
            else:
                # Якщо не вдалося отримати деталі, використовуємо базові дані
                print(
                    f"       ⚠️ Не вдалося отримати деталі, використовуємо базові дані"
                )
                formatted = self.format_attendee_for_csv(attendee)
                detailed_data.append(formatted)

            # Затримка між запитами
            if i % 10 == 0:
                time.sleep(1)

        # Show contact extraction summary
        contacts_found = sum(
            1 for attendee in detailed_data if attendee.get("other_contacts")
        )
        print(f"\n📊 ПІДСУМОК ЕКСТРАКЦІЇ КОНТАКТІВ:")
        print(
            f"   📞 Знайдено контакти у {contacts_found} з {len(detailed_data)} профілів"
        )
        print(
            f"   📈 Відсоток знайдених контактів: {(contacts_found/len(detailed_data)*100):.1f}%"
            if detailed_data
            else "0%"
        )

        return detailed_data

    def run_update(self, csv_file="SBC - Attendees.csv"):
        """Основний метод для оновлення бази"""
        print("\n" + "=" * 60)
        print("🔄 ОНОВЛЕННЯ БАЗИ УЧАСНИКІВ SBC SUMMIT 2025")
        print("=" * 60)

        # Create data directory if it doesn't exist
        data_dir = "restricted/data"
        os.makedirs(data_dir, exist_ok=True)

        # Update csv_file path to include data directory
        csv_file_path = os.path.join(data_dir, csv_file)

        # 1. Отримуємо всі результати advanced search
        print("\n📡 Етап 1: Завантаження даних з advanced search...")
        all_results = self.get_all_advanced_search_results()
        print(f"✅ Всього знайдено: {len(all_results)} учасників")

        # 2. Завантажуємо існуючу базу
        print("\n📋 Етап 2: Порівняння з існуючою базою...")
        existing_keys = self.load_existing_attendees(csv_file_path)

        # 3. Знаходимо нових
        new_attendees = self.find_new_attendees(all_results, existing_keys)
        print(f"🆕 Знайдено нових: {len(new_attendees)} учасників")

        if not new_attendees:
            print("\n✅ База актуальна, нових учасників немає")
            return

        # 4. Отримуємо детальні дані для нових
        print("\n🔍 Етап 3: Отримання детальних даних...")
        detailed_data = self.process_new_attendees(new_attendees)

        # 5. Зберігаємо в CSV
        print("\n💾 Етап 4: Збереження даних...")
        self.save_new_attendees(detailed_data, csv_file_path)

        # 6. Також зберігаємо окремий файл з новими
        kyiv_tz = ZoneInfo("Europe/Kiev")
        yesterday = datetime.now(kyiv_tz) - timedelta(days=1)
        today = yesterday.strftime("%m_%d")
        new_file = os.path.join(data_dir, f"attendees_{today}.csv")
        with open(new_file, "w", newline="", encoding="utf-8") as f:
            fieldnames = [
                "full_name",
                "company_name",
                "position",
                "linkedin_url",
                "facebook_url",
                "x_twitter_url",
                "other_socials",
                "other_contacts",  # Add other_contacts field to daily CSV structure
                "country",
                "responsibility",
                "gaming_vertical",
                "organization_type",
                "introduction",
                "source_url",
                "profile_image_url",
                "connected",  # Додаємо колонки для messaging
                "Follow-up",
                "valid",
                "Comment",
                "Date",
                "chat_id",  # Новий стовпець для зберігання chat_id
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            # Додаємо порожні значення для messaging полів
            detailed_data_with_messaging = []
            for attendee in detailed_data:
                attendee_copy = attendee.copy()
                attendee_copy.update(
                    {
                        "connected": "",
                        "Follow-up": "",
                        "valid": "",
                        "Comment": "",
                        "Date": "",
                    }
                )
                detailed_data_with_messaging.append(attendee_copy)

            writer.writerows(detailed_data_with_messaging)

        print(f"💾 Також збережено в {new_file}")

        print("\n" + "=" * 60)
        print("✅ ОНОВЛЕННЯ ЗАВЕРШЕНО")
        print(
            f"   Всього в базі: {len(existing_keys) + len(new_attendees)} учасників"
        )
        print(f"   Додано нових: {len(new_attendees)}")
        print("=" * 60)

    def show_main_menu(self):
        """Показує головне меню та обробляє вибір користувача"""
        while True:
            print("\n" + "=" * 60)
            print("🎯 SBC ATTENDEES MANAGER")
            print("=" * 60)
            print(
                f"📍 Current account: {self.accounts[self.current_account]['name']}"
            )
            print("-" * 60)
            print("1. 📥 Scrape new contacts (uses scraper account)")
            print("2. 👥 Send messages (dual messenger accounts)")
            print(
                "3. 📞 Follow-up campaigns (track responses & send follow-ups)"
            )
            print("4. 🔄 Update existing CSV with contacts")
            print("5. 🚪 Exit")
            print("=" * 60)

            choice = input("➡️ Choose an action (1-5): ").strip()

            if choice == "1":
                self.handle_scrape_contacts()
            elif choice == "2":
                self.handle_multi_account_messages()
            elif choice == "3":
                self.handle_followup_campaigns()
            elif choice == "4":
                self.handle_update_csv_contacts()
            elif choice == "5":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please select 1, 2, 3, 4, or 5.")

    def handle_scrape_contacts(self):
        """Обробляє скрейпінг нових контактів"""
        print("\n🔄 SCRAPING NEW CONTACTS")
        print("=" * 40)

        # Перевіряємо чи використовується scraper акаунт
        if self.current_account != "scraper":
            print("🔄 Переключаємося на scraper акаунт...")
            self.switch_account("scraper")

        confirm = input("Start scraping new attendees? (y/n): ").lower()
        if confirm == "y":
            self.run_update("SBC - Attendees.csv")
        else:
            print("❌ Scraping cancelled")

    def handle_multi_account_messages(self):
        """Обробляє відправку повідомлень з використанням двох messenger акаунтів"""
        print("\n👥 SEND MESSAGES WITH MULTI-ACCOUNT")
        print("=" * 40)

        # Показуємо конфігурацію messenger акаунтів
        print("🔧 Messenger accounts configuration:")
        print(
            f"   messenger1: {self.accounts['messenger1']['name']} ({self.accounts['messenger1']['username']})"
        )
        print(
            f"   messenger2: {self.accounts['messenger2']['name']} ({self.accounts['messenger2']['username']})"
        )

        # Показуємо доступні CSV файли
        data_dir = "restricted/data"
        csv_files = []

        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith(".csv"):
                    csv_files.append(file)

        if not csv_files:
            print("❌ No CSV files found in restricted/data/")
            return

        print("\n📋 Available CSV files:")
        for i, file in enumerate(csv_files, 1):
            file_path = os.path.join(data_dir, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    count = sum(1 for row in reader)
                print(f"   {i}. {file} ({count} contacts)")
            except:
                print(f"   {i}. {file} (unable to read)")

        # Вибір файлу
        file_choice = input(f"➡️ Choose file (1-{len(csv_files)}): ").strip()

        try:
            file_index = int(file_choice) - 1
            if 0 <= file_index < len(csv_files):
                selected_file = os.path.join(data_dir, csv_files[file_index])

                print(f"\n📁 Selected: {csv_files[file_index]}")

                # Показуємо загальну кількість після фільтрації
                try:
                    user_data = self.extract_user_data_from_csv(selected_file)
                    total_contacts = len(user_data)

                    if total_contacts == 0:
                        print(
                            "❌ Немає користувачів для обробки після застосування фільтрів"
                        )
                        print(
                            "💡 Спробуйте інший файл або перевірте структуру CSV"
                        )
                        return

                except Exception as e:
                    print(f"❌ Помилка обробки CSV файлу: {e}")
                    print(
                        "💡 Файл може бути пошкоджений. Спробувати виправити автоматично?"
                    )

                    # Пропонуємо автоматичне виправлення
                    fix_choice = input(
                        "➡️ Спробувати виправити CSV файл? (y/n): "
                    ).lower()
                    if fix_choice == "y":
                        print("🔧 Спробуємо виправити файл...")
                        if self.fix_malformed_csv(selected_file):
                            print("✅ Файл виправлено, спробуємо ще раз...")
                            try:
                                user_data = self.extract_user_data_from_csv(
                                    selected_file
                                )
                                total_contacts = len(user_data)

                                if total_contacts == 0:
                                    print(
                                        "❌ Немає користувачів для обробки після виправлення"
                                    )
                                    return
                                else:
                                    print(
                                        f"✅ Успішно завантажено {total_contacts} контактів після виправлення"
                                    )
                            except Exception as e2:
                                print(
                                    f"❌ Навіть після виправлення виникла помилка: {e2}"
                                )
                                return
                        else:
                            print("❌ Не вдалося виправити файл")
                            return
                    else:
                        print("❌ Перевірте файл вручну та спробуйте ще раз")
                        return

                # Підтвердження та налаштування
                # 1. Ліміт користувачів
                limit_input = input(
                    f"➡️ User limit (default: all {total_contacts} users, or enter number): "
                ).strip()
                try:
                    user_limit = int(limit_input) if limit_input else None
                    if user_limit and user_limit > total_contacts:
                        print(
                            f"⚠️ Ліміт {user_limit} більше доступних користувачів ({total_contacts}), використовуємо всіх"
                        )
                        user_limit = None
                except:
                    user_limit = None

                # Показуємо як буде розподілена робота з урахуванням ліміту
                actual_users = user_limit if user_limit else total_contacts
                half = actual_users // 2

                print(f"\n📊 Work distribution for {actual_users} users:")
                print(
                    f"   👤 Messenger1 ({self.accounts['messenger1']['name']}): {half} contacts"
                )
                print(
                    f"   👤 Messenger2 ({self.accounts['messenger2']['name']}): {actual_users - half} contacts"
                )

                # Показуємо шаблони повідомлень
                print("\n💬 Message templates (will use random selection):")
                for i, template in enumerate(self.follow_up_messages, 1):
                    preview = template.replace("{name}", "[NAME]")
                    preview_short = (
                        preview[:100] + "..."
                        if len(preview) > 100
                        else preview
                    )
                    print(f"   {i}. {preview_short}")

                print(f"\n💬 Automatic follow-up message:")
                print(f"   → {self.second_follow_up_message}")

                print(
                    f"\n⚠️ Will send random message template + automatic follow-up (5s delay) to users without existing chats"
                )

                # 2. Затримка між контактами
                delay = input(
                    "➡️ Delay between contacts in seconds (default 8, includes 5s for follow-up): "
                ).strip()
                try:
                    delay_seconds = (
                        int(delay) if delay else 8
                    )  # Increased default due to automatic follow-up
                except:
                    delay_seconds = 8

                # 3. Фінальне підтвердження
                actual_users = user_limit if user_limit else total_contacts
                confirm = input(
                    f"Start multi-messenger messaging to {actual_users} users with {delay_seconds}s delay? (y/n): "
                ).lower()
                if confirm == "y":
                    self.bulk_message_multi_account(
                        selected_file, delay_seconds, user_limit
                    )
                else:
                    print("❌ Multi-messenger messaging cancelled")
            else:
                print("❌ Invalid file selection")
        except ValueError:
            print("❌ Invalid input")

    def handle_followup_campaigns(self):
        """Обробляє follow-up кампанії"""
        print("\n📬 FOLLOW-UP CAMPAIGNS")
        print("=" * 40)

        # Показуємо поточну дату та дати follow-up
        kyiv_tz = ZoneInfo("Europe/Kiev")
        current_date = datetime.now(kyiv_tz)
        sbc_date = self.sbc_start_date
        days_until_sbc = (sbc_date - current_date).days

        print(f"📅 Поточна дата: {current_date.strftime('%d.%m.%Y')}")
        print(f"📅 Дата SBC Summit: {sbc_date.strftime('%d.%m.%Y')}")
        print(f"⏰ Днів до конференції: {days_until_sbc}")

        print("\n📋 Follow-up правила:")
        print("   📨 Follow-up 1: через 3 дні після першого повідомлення")
        print("   📨 Follow-up 2: через 7 днів після першого повідомлення")
        print("   📨 Follow-up 3: за 1 день до початку SBC Summit")

        print("\n🔧 Режим роботи:")
        print("   1. 🚀 Оптимізований (на основі CSV - швидко)")
        print("   2. 🐌 Повний аналіз (всі чати - повільно)")

        mode_choice = input("➡️ Виберіть режим (1-2): ").strip()

        if mode_choice == "1":
            method_to_use = "optimized"
            print("✅ Використовується оптимізований режим")
        elif mode_choice == "2":
            method_to_use = "full"
            print("✅ Використовується повний аналіз")
        else:
            print("❌ Невірний вибір, використовується оптимізований режим")
            method_to_use = "optimized"

        # Показуємо доступні акаунти для обробки
        messenger_accounts = ["messenger1", "messenger2"]
        print(f"\n🔧 Доступні messenger акаунти:")
        for i, acc_key in enumerate(messenger_accounts, 1):
            acc = self.accounts[acc_key]
            print(f"   {i}. {acc['name']} ({acc['username']})")

        print("   3. Обидва акаунти послідовно")

        # Вибір акаунта
        account_choice = input(
            f"➡️ Виберіть акаунт для обробки (1-3): "
        ).strip()

        try:
            if account_choice == "1":
                # Обробка з messenger1
                if method_to_use == "optimized":
                    stats = self.process_followup_campaigns_optimized(
                        "messenger1"
                    )
                else:
                    stats = self.process_followup_campaigns("messenger1")
            elif account_choice == "2":
                # Обробка з messenger2
                if method_to_use == "optimized":
                    stats = self.process_followup_campaigns_optimized(
                        "messenger2"
                    )
                else:
                    stats = self.process_followup_campaigns("messenger2")
            elif account_choice == "3":
                # Обробка з обома акаунтами
                print("\n🔄 Обробка з обома акаунтами...")

                print("\n" + "=" * 50)
                print("📱 MESSENGER 1")
                print("=" * 50)
                if method_to_use == "optimized":
                    stats1 = self.process_followup_campaigns_optimized(
                        "messenger1"
                    )
                else:
                    stats1 = self.process_followup_campaigns("messenger1")

                print("\n" + "=" * 50)
                print("📱 MESSENGER 2")
                print("=" * 50)
                if method_to_use == "optimized":
                    stats2 = self.process_followup_campaigns_optimized(
                        "messenger2"
                    )
                else:
                    stats2 = self.process_followup_campaigns("messenger2")

                # Об'єднуємо статистику
                if "error" not in stats1 and "error" not in stats2:
                    combined_stats = {}
                    for key in stats1:
                        combined_stats[key] = stats1.get(key, 0) + stats2.get(
                            key, 0
                        )

                    print(f"\n📊 ЗАГАЛЬНА СТАТИСТИКА:")
                    print(
                        f"   📋 Проаналізовано чатів: {combined_stats.get('analyzed', 0)}"
                    )
                    if method_to_use == "full":
                        print(
                            f"   💾 chat_id збережено: {combined_stats.get('chat_ids_stored', 0)}"
                        )
                    print(
                        f"   ✅ З відповідями: {combined_stats.get('has_responses', 0)}"
                    )
                    print(
                        f"   📨 Follow-up 3 дні: {combined_stats.get('day_3_sent', 0)}"
                    )
                    print(
                        f"   📨 Follow-up 7 днів: {combined_stats.get('day_7_sent', 0)}"
                    )
                    print(
                        f"   📨 Фінальний follow-up: {combined_stats.get('final_sent', 0)}"
                    )
                    print(f"   ❌ Помилок: {combined_stats.get('errors', 0)}")

                    total_sent = (
                        combined_stats.get("day_3_sent", 0)
                        + combined_stats.get("day_7_sent", 0)
                        + combined_stats.get("final_sent", 0)
                    )
                    print(f"   📈 Всього відправлено: {total_sent}")
            else:
                print("❌ Невірний вибір")
                return

        except Exception as e:
            print(f"❌ Помилка виконання follow-up кампанії: {e}")

    def handle_update_csv_contacts(self):
        """Обробляє оновлення існуючого CSV з контактами"""
        print("\n📞 UPDATE EXISTING CSV WITH CONTACTS")
        print("=" * 40)

        data_dir = "restricted/data"
        csv_file = os.path.join(data_dir, "SBC - Attendees.csv")

        if not os.path.exists(csv_file):
            print(f"❌ Main CSV file not found: {csv_file}")
            print("   First run 'Scrape new contacts' to create the file")
            return

        print(f"📁 File to update: {csv_file}")
        print(
            "📋 This will extract contacts from introduction text for profiles"
        )
        print(
            "   that don't have contacts in the 'other_contacts' column yet."
        )

        confirm = input(
            "\n🤔 Proceed with contact extraction? (y/n): "
        ).lower()
        if confirm == "y":
            self.update_existing_csv_with_contacts(csv_file)
        else:
            print("❌ Contact extraction cancelled")

    def show_account_status(self):
        """Показує статус всіх акаунтів"""
        print("\n📊 ACCOUNT STATUS")
        print("=" * 40)
        print("=" * 40)
        if self.current_account:
            print(
                f"📍 Currently active: {self.accounts[self.current_account]['name']}"
            )
        else:
            print("📍 Currently active: None")

        print("\n📋 All accounts configuration:")

        for key, account in self.accounts.items():
            status = (
                "🟢 ACTIVE" if key == self.current_account else "⭕ INACTIVE"
            )
            role_emoji = "🔍" if account["role"] == "scraping" else "💬"

            # Перевіряємо чи налаштовані credentials
            is_configured = (
                account["username"]
                and not account["username"].startswith(("MESSENGER", "your_"))
                and account["password"]
                and not account["password"].startswith(("MESSENGER", "your_"))
                and account["user_id"]
                and not account["user_id"].startswith(("MESSENGER", "your_"))
            )
            config_status = (
                "✅ CONFIGURED" if is_configured else "❌ NOT CONFIGURED"
            )

            print(f"   {role_emoji} {key}: {account['name']}")
            print(f"      📧 Email: {account['username']}")
            print(f"      🎭 Role: {account['role']}")
            print(f"      🔄 Status: {status}")
            print(f"      ⚙️ Config: {config_status}")
            print()

        print("ℹ️ Roles:")
        print("   🔍 scraper - Used for scraping new contacts")
        print("   💬 messenger1/messenger2 - Used for sending messages")
        print(
            "\n💡 To configure accounts, edit your .env file with real credentials"
        )

    def update_existing_csv_with_contacts(
        self, csv_file="restricted/data/SBC - Attendees.csv"
    ):
        """Updates existing CSV file to extract contacts for profiles that don't have them yet"""
        print(f"\n📞 ОНОВЛЕННЯ ІСНУЮЧОГО CSV З КОНТАКТАМИ")
        print("=" * 50)

        if not os.path.exists(csv_file):
            print(f"❌ Файл {csv_file} не знайдено")
            return

        try:
            if PANDAS_AVAILABLE:
                # Read CSV with pandas
                df = pd.read_csv(csv_file)

                # Check if other_contacts column exists, if not add it
                if "other_contacts" not in df.columns:
                    df["other_contacts"] = ""
                    print("📋 Додано колонку 'other_contacts'")

                # Count rows that need contact extraction
                needs_extraction = (
                    (
                        df["other_contacts"].isna()
                        | (df["other_contacts"] == "")
                    )
                    & df["introduction"].notna()
                    & (df["introduction"] != "")
                )

                rows_to_update = needs_extraction.sum()
                print(
                    f"🔍 Знайдено {rows_to_update} профілів для екстракції контактів"
                )

                if rows_to_update == 0:
                    print("✅ Всі профілі вже мають оброблені контакти")
                    return

                # Extract contacts for rows that need it
                contacts_extracted = 0
                for idx, row in df.iterrows():
                    if needs_extraction.iloc[idx]:
                        introduction_text = str(row["introduction"])
                        contacts = (
                            self.contact_extractor.extract_contacts_from_text(
                                introduction_text
                            )
                        )
                        contacts_str = (
                            ", ".join(sorted(contacts)) if contacts else ""
                        )

                        if contacts_str:
                            df.at[idx, "other_contacts"] = contacts_str
                            contacts_extracted += 1
                            print(f"   📞 {row['full_name']}: {contacts_str}")

                        # Progress update
                        if (idx + 1) % 50 == 0:
                            print(f"   📊 Оброблено {idx + 1} записів...")

                # Save updated CSV
                df.to_csv(csv_file, index=False)
                print(
                    f"\n✅ Оновлено {contacts_extracted} профілів з контактами"
                )
                print(f"💾 Збережено в {csv_file}")

            else:
                print(
                    "❌ Pandas не доступний. Встановіть pandas для цієї функції."
                )

        except Exception as e:
            print(f"❌ Помилка при оновленні CSV: {e}")

    def close(self):
        """Закриває браузер"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("👋 Браузер закрито")


# Використання
if __name__ == "__main__":
    scraper = SBCAttendeesScraper(headless=False)

    try:
        # Логінимося
        if scraper.start():
            # Запускаємо інтерактивне меню
            scraper.show_main_menu()
        else:
            print("❌ Не вдалося залогінитися")

    except Exception as e:
        print(f"\n❌ Помилка: {e}")
        import traceback

        traceback.print_exc()

    finally:
        scraper.close()
