from playwright.sync_api import sync_playwright
import json
import csv
import time
from datetime import datetime
import os
from typing import List, Dict, Set, Tuple


class SBCAttendeesScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.is_logged_in = False

    def start(self):
        """Запускає браузер і логіниться"""
        print("🚀 Запускаємо браузер...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        )
        self.page = self.context.new_page()

        # Логінимося
        return self.login()

    def login(self):
        """Виконує логін"""
        print("📄 Відкриваємо sbcconnect.com...")
        self.page.goto("https://sbcconnect.com", wait_until="domcontentloaded")
        self.page.wait_for_timeout(5000)

        print("🔑 Логінимося...")
        result = self.page.evaluate(
            """
            async () => {
                const response = await fetch('https://sbcconnect.com/api/account/login', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        username: 'at@flexify.finance',
                        password: 'flexify.financeSBC1',
                        rememberMe: true
                    })
                });
                const data = await response.json();
                return {status: response.status, data: data};
            }
        """
        )

        if result["status"] == 200:
            print("✅ Успішно залогінилися!")
            self.is_logged_in = True
            return result["data"]
        else:
            print(f"❌ Помилка логіну: {result}")
            return None

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

        if result.get("status") == 200:
            return result.get("data")
        else:
            print(f"   ❌ Помилка {endpoint}: {result.get('status')}")
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

        # 1. Отримуємо всі результати advanced search
        print("\n📡 Етап 1: Завантаження даних з advanced search...")
        all_results = self.get_all_advanced_search_results()
        print(f"✅ Всього знайдено: {len(all_results)} учасників")

        # 2. Завантажуємо існуючу базу
        print("\n📋 Етап 2: Порівняння з існуючою базою...")
        existing_keys = self.load_existing_attendees(csv_file)

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
        self.save_new_attendees(detailed_data, csv_file)

        # 6. Також зберігаємо окремий файл з новими
        today = datetime.now().strftime("%m_%d")
        new_file = f"attendees_{today}.csv"
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
            # Запускаємо оновлення
            scraper.run_update("attendees.csv")
        else:
            print("❌ Не вдалося залогінитися")

    except Exception as e:
        print(f"\n❌ Помилка: {e}")
        import traceback

        traceback.print_exc()

    finally:
        input("\n⏸️ Натисніть Enter щоб закрити браузер...")
        scraper.close()
