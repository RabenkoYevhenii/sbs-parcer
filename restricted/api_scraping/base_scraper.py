"""
Base scraper functionality for browser management, authentication, and API requests
"""

from playwright.sync_api import sync_playwright
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any
import os


class BaseScraper:
    """Base class for handling browser automation and API requests"""

    def __init__(self, headless=True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.is_logged_in = False
        self.current_account = None

    def get_data_dir(self):
        """Returns the correct path to the data folder"""
        # Get the directory where this script is located (restricted/api_scraping)
        # Go up one level to restricted, then down to data
        script_dir = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
        return os.path.join(script_dir, "data")

    def start(self):
        """Starts the browser and logs in"""
        print("🚀 Запускаємо браузер...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        )
        self.page = self.context.new_page()
        return True

    def login(self, account_key="scraper", accounts=None):
        """Performs login with the specified account"""
        if not accounts or account_key not in accounts:
            print(f"❌ Акаунт {account_key} не знайдено")
            return False

        account = accounts[account_key]
        self.current_account = account_key

        print("📄 Відкриваємо sbcconnect.com...")
        self.page.goto("https://sbcconnect.com", wait_until="domcontentloaded")
        self.page.wait_for_timeout(5000)

        print(f"🔑 Логінимося з {account['name']}...")
        result = self.page.evaluate(
            """
            async (credentials) => {
                const response = await fetch('https://sbcconnect.com/api/account/login', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        username: credentials.username,
                        password: credentials.password,
                        rememberMe: true
                    })
                });
                const data = await response.json();
                return {status: response.status, data: data};
            }
            """,
            {"username": account["username"], "password": account["password"]},
        )

        if result["status"] == 200:
            print(f"✅ Успішно залогінились як {account['name']}")
            self.is_logged_in = True
            return True
        else:
            print(f"❌ Помилка логіну: {result}")
            return False

    def logout(self):
        """Logs out from the current account"""
        if not self.is_logged_in:
            print("⚠️ Немає активного логіну")
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

        if result["status"] == 200 or result["status"] == 404:
            print("✅ Успішно вийшли з акаунта")
            self.is_logged_in = False
            self.current_account = None
            return True
        else:
            print(f"❌ Помилка виходу: {result}")
            return False

    def api_request(
        self, method, endpoint, data=None, max_retries=5, timeout_seconds=10
    ):
        """Performs API request through browser with timeout and retries"""
        if not self.is_logged_in:
            print("❌ Не залогінені для виконання API запитів")
            return None

        url = (
            f"https://sbcconnect.com/api/{endpoint}"
            if not endpoint.startswith("http")
            else endpoint
        )

        for attempt in range(max_retries):
            try:
                # Set timeout for the entire operation
                timeout_ms = timeout_seconds * 1000

                if method.upper() == "GET":
                    result = self.page.evaluate(
                        """
                        async (params) => {
                            const controller = new AbortController();
                            const timeoutId = setTimeout(() => controller.abort(), params.timeoutMs);
                            
                            try {
                                const response = await fetch(params.url, {
                                    method: 'GET',
                                    headers: {'Content-Type': 'application/json'},
                                    signal: controller.signal
                                });
                                clearTimeout(timeoutId);
                                
                                if (response.ok) {
                                    const data = await response.json();
                                    return {status: response.status, data: data, success: true};
                                } else {
                                    return {status: response.status, error: 'HTTP Error', success: false};
                                }
                            } catch (error) {
                                clearTimeout(timeoutId);
                                return {status: 500, error: error.message, success: false};
                            }
                        }
                        """,
                        {"url": url, "timeoutMs": timeout_ms},
                    )
                elif method.upper() == "POST":
                    json_data = json.dumps(data) if data else "{}"
                    result = self.page.evaluate(
                        """
                        async (params) => {
                            const controller = new AbortController();
                            const timeoutId = setTimeout(() => controller.abort(), params.timeoutMs);
                            
                            try {
                                const response = await fetch(params.url, {
                                    method: 'POST',
                                    headers: {'Content-Type': 'application/json'},
                                    body: params.jsonData,
                                    signal: controller.signal
                                });
                                clearTimeout(timeoutId);
                                
                                if (response.ok) {
                                    try {
                                        const data = await response.json();
                                        return {status: response.status, data: data, success: true};
                                    } catch (e) {
                                        // Some responses might not be JSON
                                        return {status: response.status, data: true, success: true};
                                    }
                                } else {
                                    return {status: response.status, error: 'HTTP Error', success: false};
                                }
                            } catch (error) {
                                clearTimeout(timeoutId);
                                return {status: 500, error: error.message, success: false};
                            }
                        }
                        """,
                        {
                            "url": url,
                            "jsonData": json_data,
                            "timeoutMs": timeout_ms,
                        },
                    )
                else:
                    print(f"❌ Непідтримуваний HTTP метод: {method}")
                    return None

                if result and result.get("success"):
                    return result.get("data")
                else:
                    if attempt < max_retries - 1:
                        # Exponential backoff: 2, 4, 8, 16 seconds
                        delay = 2 ** (attempt + 1)
                        print(
                            f"⚠️ Спроба {attempt + 1}/{max_retries} невдала, повторюємо через {delay} сек..."
                        )
                        if result:
                            print(
                                f"   📊 Статус: {result.get('status')}, Помилка: {result.get('error', 'Невідома')}"
                            )
                        time.sleep(delay)
                    else:
                        print(f"❌ Всі {max_retries} спроб API запиту невдалі")
                        if result:
                            print(
                                f"   📊 Останній статус: {result.get('status')}, Помилка: {result.get('error', 'Невідома')}"
                            )

            except Exception as e:
                if attempt < max_retries - 1:
                    # Exponential backoff: 2, 4, 8, 16 seconds
                    delay = 2 ** (attempt + 1)
                    print(
                        f"⚠️ Помилка на спробі {attempt + 1}/{max_retries}: {e}, повторюємо через {delay} сек..."
                    )
                    time.sleep(delay)
                else:
                    print(f"❌ Критична помилка API запиту: {e}")

        return None

    def advanced_search(self, from_index=0, size=2000):
        """Performs advanced search with filters"""
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
        """Gets all advanced search results"""
        all_results = []
        from_index = 0
        size = 2000  # Maximum size

        while True:
            print(f"📥 Отримуємо результати з індексу {from_index}...")
            results = self.advanced_search(from_index, size)

            if not results:
                print("❌ Не вдалося отримати результати пошуку")
                break

            if not isinstance(results, list):
                print("❌ Неочікуваний формат результатів")
                break

            print(f"✅ Отримано {len(results)} результатів")
            all_results.extend(results)

            # If we got less than the requested size, we've reached the end
            if len(results) < size:
                print("✅ Отримали всі результати")
                break

            from_index += size

        return all_results

    def get_user_details(self, user_id):
        """Gets detailed user information"""
        endpoint = f"user/getById?userId={user_id}&eventPath=sbc-summit-2025"
        return self.api_request("GET", endpoint)

    def switch_account(self, account_key, accounts):
        """Switches to a different account"""
        if account_key == self.current_account:
            print(f"✅ Вже використовуємо акаунт {account_key}")
            return True

        print(f"🔄 Перемикаємося на акаунт {account_key}...")

        # Logout from current account
        if self.is_logged_in:
            self.logout()

        # Login with new account
        return self.login(account_key, accounts)

    def close(self):
        """Closes the browser and cleans up resources"""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("🔒 Браузер закрито")
