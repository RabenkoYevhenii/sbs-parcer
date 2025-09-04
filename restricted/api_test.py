from playwright.sync_api import sync_playwright
import json
import csv
import time
from datetime import datetime
import os
import uuid
import re
import random
from typing import List, Dict, Set, Tuple, Optional
from config import settings


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
            "Hello {name} 👋\nI'm thrilled to see you at the SBC Summit in Lisbon the following month! Before things get hectic, it's always a pleasure to connect with other iGaming experts.\nI speak on behalf of Flexify Finance, a company that specializes in smooth payments for high-risk industries. Visit us at Stand E613 if you're looking into new payment options or simply want to discuss innovation.\nWhat is your main objective or priority for the expo this year? I'd love to know what you're thinking about!",
            "Hi {name} 👋\nExcited to connect with fellow SBC Summit attendees! I'm representing Flexify Finance - we provide payment solutions specifically designed for iGaming and high-risk industries.\nWe'll be at Stand E613 during the summit in Lisbon. Would love to learn about your current payment challenges or discuss the latest trends in our industry.\nWhat brings you to SBC Summit this year? Any specific goals or connections you're hoping to make?",
            "Hello {name} 👋\nLooking forward to the SBC Summit in Lisbon! As someone in the iGaming space, I always enjoy connecting with industry professionals before the event buzz begins.\nI'm with Flexify Finance - we specialize in seamless payment processing for high-risk sectors. Feel free to stop by Stand E613 if you'd like to explore new payment innovations.\nWhat are you most excited about at this year's summit? Any particular sessions or networking goals?",
            "Hi {name} 👋 looks like we'll both be at SBC Lisbon this month!\nAlways great to meet fellow iGaming pros before the chaos begins.\nI'm with Flexify Finance, a payments provider for high-risk verticals - you'll find us at Stand E613.\nOut of curiosity, what's your main focus at the expo this year ?",
        ]

        # Second follow-up message that always gets sent after the first one
        self.second_follow_up_message = "Is payments something on your radar to explore ?"

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

        if result["status"] == 200 or result["status"] == 404:  # 404 може означати що вже вийшли
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
        current_time = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        endpoint = "chat/sendMessage"
        data = {
            "chatId": chat_id,
            "messageId": message_id,
            "message": message,
            "createdDate": current_time,
        }

        result = self.api_request("POST", endpoint, data)
        return result is not None

    def send_message_to_user(self, target_user_id: str, message: str, full_name: str = None) -> bool:
        """Повний пайплайн відправки повідомлення користувачу з автоматичним follow-up"""
        # 1. Перевіряємо чи є існуючий чат
        chat_id = self.find_chat_with_user(target_user_id)

        if chat_id:
            # 1.1. Якщо чат існує, перевіряємо чи є в ньому повідомлення
            print(f"       🔍 Перевіряємо чи є повідомлення в існуючому чаті...")
            if self.check_chat_has_messages(chat_id):
                print(f"       ⏭️ Чат вже містить повідомлення, пропускаємо")
                return False  # Не відправляємо, якщо вже є повідомлення
            else:
                print(f"       ✅ Чат порожній, можна відправляти повідомлення")
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

        print(f"       ✅ Друге повідомлення відправлено: '{self.second_follow_up_message}'")
        
        # 6. Оновлюємо CSV файл про відправлені повідомлення
        print(f"       📝 Оновлюємо CSV файл...")
        data_dir = "restricted/data"
        csv_file = os.path.join(data_dir, "SBC - Attendees.csv")
        if full_name:
            self.update_csv_with_messaging_status(csv_file, target_user_id, full_name)
        else:
            print(f"       ⚠️ Не вдалося оновити CSV - відсутнє ім'я користувача")
        
        return True

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
            
            # Читаємо CSV файл
            df = pd.read_csv(csv_file)
            print(f"📊 Загальна кількість записів: {len(df)}")
            
            if apply_filters:
                print("🔍 Застосовуємо фільтри...")
                original_count = len(df)
                
                # 1. Фільтр по порожньому полю 'connected'
                df = df[df["connected"].isna() | (df["connected"] == "")]
                print(f"   Після фільтру 'connected' (порожнє): {len(df)} записів")
                
                # 2. Фільтр по порожньому полю 'Follow-up'
                df = df[df["Follow-up"].isna() | (df["Follow-up"] == "")]
                print(f"   Після фільтру 'Follow-up' (порожнє): {len(df)} записів")
                
                # 3. Фільтр по порожньому полю 'valid'
                df = df[df["valid"].isna() | (df["valid"] == "")]
                print(f"   Після фільтру 'valid' (порожнє): {len(df)} записів")
                
                # 4. Фільтр по gaming_vertical (без "land")
                if "gaming_vertical" in df.columns:
                    df = df[~df["gaming_vertical"].str.contains("land", case=False, na=False)]
                    print(f"   Після фільтру gaming_vertical (без 'land'): {len(df)} записів")
                
                # 5. Фільтр по позиції (містить ключові слова)
                position_keywords = [
                    "chief executive officer", "ceo", "chief operating officer", "coo", 
                    "chief financial officer", "cfo", "chief payments officer", "cpo",
                    "payments", "psp", "operations", "business development", 
                    "partnerships", "relationship", "country manager"
                ]
                if "position" in df.columns:
                    # Конвертуємо позиції в нижній регістр для порівняння
                    df['position_lower'] = df['position'].str.lower().fillna('')
                    
                    # Створюємо маску для позицій що містять ключові слова
                    position_mask = df['position_lower'].str.contains('|'.join(position_keywords), case=False, na=False)
                    
                    # Виключаємо "coordinator" для COO
                    coordinator_mask = df['position_lower'].str.contains('coordinator', case=False, na=False)
                    coo_mask = df['position_lower'].str.contains('coo', case=False, na=False)
                    
                    # Застосовуємо фільтр: включаємо позиції з ключовими словами, але виключаємо coordinator при COO
                    df = df[position_mask & ~(coo_mask & coordinator_mask)]
                    
                    # Видаляємо тимчасову колонку
                    df = df.drop('position_lower', axis=1)
                    
                    print(f"   Після фільтру позиції (ключові слова, виключаючи COO+coordinator): {len(df)} записів")
                
                print(f"📊 Відфільтровано: {original_count} → {len(df)} записів")
            
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
            print("⚠️ pandas не встановлено, використовуємо базову обробку без фільтрів...")
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
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

        print(f"📋 Знайдено {len(user_data)} користувачів для обробки")
        return user_data

    def update_csv_with_messaging_status(self, csv_file: str, user_id: str, full_name: str):
        """Оновлює CSV файл з інформацією про відправлене повідомлення"""
        try:
            # Читаємо весь CSV файл
            import pandas as pd
            df = pd.read_csv(csv_file)
            
            # Знаходимо запис за user_id (витягуємо з source_url)
            mask = df['source_url'].str.contains(user_id, na=False)
            
            if mask.any():
                # Визначаємо автора на основі поточного акаунта
                if self.current_account == "messenger1":
                    author = "Daniil"
                elif self.current_account == "messenger2":
                    author = "Yaroslav"
                else:
                    author = "System"
                
                # Отримуємо поточну дату у форматі d.mm за київським часом
                from zoneinfo import ZoneInfo
                kyiv_tz = ZoneInfo("Europe/Kiev")
                current_date = datetime.now(kyiv_tz).strftime("%-d.%m")
                
                # Оновлюємо поля
                df.loc[mask, 'connected'] = 'Sent'
                df.loc[mask, 'Comment'] = author  # Використовуємо Comment як author field
                df.loc[mask, 'Date'] = current_date
                
                # Зберігаємо оновлений файл
                df.to_csv(csv_file, index=False, encoding='utf-8')
                
                print(f"       📝 CSV оновлено: connected=Sent, author={author}, date={current_date}")
            else:
                print(f"       ⚠️ Не знайдено запис для user_id {user_id} у CSV")
                
        except ImportError:
            print(f"       ⚠️ pandas не встановлено, CSV не оновлено")
        except Exception as e:
            print(f"       ❌ Помилка оновлення CSV: {e}")

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
                user_data = user_data[:user_limit]
                print(f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {len(self.extract_user_data_from_csv(csv_file))} доступних користувачів")
        user_data = self.extract_user_data_from_csv(csv_file)

        if not user_data:
            print("❌ Не знайдено користувачів для обробки")
            return 0, 0

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
                print(f"   💬 + автоматичний follow-up: '{self.second_follow_up_message}'")

                # Відправляємо повідомлення (з автоматичним follow-up та перевіркою чату)
                success = self.send_message_to_user(user_id, message, full_name)

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
                print(f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {len(self.extract_user_data_from_csv(csv_file))} доступних користувачів")

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
                print(f"   💬 + автоматичний follow-up: '{self.second_follow_up_message}'")

                # Відправляємо повідомлення (з автоматичним follow-up та перевіркою чату)
                success = self.send_message_to_user(user_id, message, full_name)

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

    def load_existing_attendees(self, csv_file="attendees.csv"):
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
                "country": profile.get("country", ""),
                "responsibility": profile.get("areaOfResponsibility", ""),
                "gaming_vertical": profile.get("mainGamingVertical", ""),
                "organization_type": profile.get("organizationType", ""),
                "introduction": profile.get("introduction", ""),
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
                "country": "",
                "responsibility": "",
                "gaming_vertical": "",
                "organization_type": "",
                "introduction": "",
                "source_url": f"https://sbcconnect.com/event/sbc-summit-2025/attendees/{attendee_details.get('userId', '')}",
                "profile_image_url": attendee_details.get("photoUrl", ""),
            }

    def save_new_attendees(self, new_attendees_data, csv_file="attendees.csv"):
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
                "country",
                "responsibility",
                "gaming_vertical",
                "organization_type",
                "introduction",
                "source_url",
                "profile_image_url",
            ]

            writer = csv.DictWriter(f, fieldnames=fieldnames)

            # Пишемо заголовки якщо файл новий
            if not file_exists:
                writer.writeheader()

            # Записуємо дані
            for attendee in new_attendees_data:
                writer.writerow(attendee)

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

        return detailed_data

    def run_update(self, csv_file="attendees.csv"):
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
        from zoneinfo import ZoneInfo
        kyiv_tz = ZoneInfo("Europe/Kiev")
        today = datetime.now(kyiv_tz).strftime("%m_%d")
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
                "country",
                "responsibility",
                "gaming_vertical",
                "organization_type",
                "introduction",
                "source_url",
                "profile_image_url",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(detailed_data)

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
            print("3. 🚪 Exit")
            print("=" * 60)

            choice = input("➡️ Choose an action (1-3): ").strip()

            if choice == "1":
                self.handle_scrape_contacts()
            elif choice == "2":
                self.handle_multi_account_messages()
            elif choice == "3":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please select 1, 2, or 3.")

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
            self.run_update("attendees.csv")
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
                user_data = self.extract_user_data_from_csv(selected_file)
                total_contacts = len(user_data)

                # Підтвердження та налаштування
                # 1. Ліміт користувачів
                limit_input = input(
                    f"➡️ User limit (default: all {total_contacts} users, or enter number): "
                ).strip()
                try:
                    user_limit = int(limit_input) if limit_input else None
                    if user_limit and user_limit > total_contacts:
                        print(f"⚠️ Ліміт {user_limit} більше доступних користувачів ({total_contacts}), використовуємо всіх")
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
                    delay_seconds = int(delay) if delay else 8  # Increased default due to automatic follow-up
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

    def show_account_status(self):
        """Показує статус всіх акаунтів"""
        print("\n� ACCOUNT STATUS")
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
