"""
Main scraper class that orchestrates all components and contains the menu system
"""

import csv
import os
import sys
import random
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
)
from extract_contacts import ContactExtractor
from .base_scraper import BaseScraper
from .company_filter import CompanyFilter
from .data_processor import DataProcessor
from .messaging import MessagingHandler


class SBCAttendeesScraper:
    """Main class that orchestrates the SBC attendees scraping and messaging system"""

    def __init__(self, headless=True, proxy_config: Dict[str, str] = None):
        # Initialize core components
        self.base_scraper = BaseScraper(headless, proxy_config)
        self.company_filter = CompanyFilter(self.base_scraper.get_data_dir())
        self.data_processor = DataProcessor(self.base_scraper.get_data_dir())
        self.messaging = MessagingHandler(
            self.base_scraper, self.company_filter, self.data_processor
        )

        # Initialize contact extractor for immediate contact extraction during scraping
        self.contact_extractor = ContactExtractor()

        # Validate required environment variables
        self._validate_env_variables()

        # Account configuration from environment variables
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
            "messenger3": {
                "username": settings.messenger3_username,
                "password": settings.messenger3_password,
                "user_id": settings.messenger3_user_id,
                "name": f"Messenger Account 3 ({settings.messenger3_username})",
                "role": "messaging",
            },
        }

        # SBC Summit start date (September 16, 2025) in Kyiv timezone
        kyiv_tz = ZoneInfo("Europe/Kiev")
        self.sbc_start_date = datetime(2025, 9, 16, tzinfo=kyiv_tz)

    def _validate_env_variables(self):
        """Validates the presence of required environment variables"""
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
            ("MESSENGER3_USERNAME", settings.messenger3_username),
            ("MESSENGER3_PASSWORD", settings.messenger3_password),
            ("MESSENGER3_USER_ID", settings.messenger3_user_id),
        ]

        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)

        if missing_vars:
            print(f"❌ Відсутні обов'язкові environment variables:")
            for var in missing_vars:
                print(f"   - {var}")
            print(f"\nВстановіть їх у файлі .env або як системні змінні")
            exit(1)

    def start(self):
        """Starts the browser and logs in"""
        self.base_scraper.start()
        return self.base_scraper.login("scraper", self.accounts)

    def switch_account(self, account_key):
        """Switches to a different account"""
        return self.base_scraper.switch_account(account_key, self.accounts)

    def show_main_menu(self):
        """Shows main menu and handles user choice"""
        while True:
            print("\n" + "=" * 60)
            print("🎯 SBC ATTENDEES MANAGER")
            print("=" * 60)
            print(
                f"📍 Current account: {self.accounts[self.base_scraper.current_account]['name']}"
            )
            print("-" * 60)
            print("1. 📥 Scrape new contacts (uses scraper account)")
            print("2. 👥 Send messages (dual messenger accounts)")
            print("3. 📞 Follow-up campaigns")
            print("4. 🎯 Conference followup for positive conversations")
            print("5. 📬 Check for responses and update CSV status")
            print("6. 📝 Update existing CSV with contacts")
            print("7. 🚫 Manage excluded companies")
            print("8. 📊 Account status")
            print("9. 🚪 Exit")
            print("=" * 60)
            print(
                f"🚫 Excluded companies: {len(self.company_filter.excluded_companies)}"
            )

            choice = input("➡️ Choose an action (1-9): ").strip()

            if choice == "1":
                self.handle_scrape_contacts()
            elif choice == "2":
                self.handle_multi_account_messages()
            elif choice == "3":
                self.handle_followup_campaigns()
            elif choice == "4":
                self.handle_conference_followup()
            elif choice == "5":
                self.handle_check_responses()
            elif choice == "6":
                self.handle_update_csv_contacts()
            elif choice == "7":
                self.handle_excluded_companies()
            elif choice == "8":
                self.show_account_status()
            elif choice == "9":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please select 1-9.")

    def handle_scrape_contacts(self):
        """Handles scraping new contacts"""
        print("\n🔄 SCRAPING NEW CONTACTS")
        print("=" * 40)

        # Check if using scraper account
        if self.base_scraper.current_account != "scraper":
            print("🔄 Переключаємося на scraper акаунт...")
            self.switch_account("scraper")

        confirm = input("Start scraping new attendees? (y/n): ").lower()
        if confirm == "y":
            self.run_update()
        else:
            print("❌ Scraping cancelled")

    def handle_multi_account_messages(self):
        """Handles sending messages using one or more messenger accounts (full parity with original)"""
        print("\n👥 SEND MESSAGES - ACCOUNT SELECTION")
        print("=" * 40)

        # Get all messenger accounts
        messenger_accounts = []
        for account_key, account_info in self.accounts.items():
            if account_info["role"] == "messaging" and account_info.get(
                "username"
            ):
                # Exclude 'yaroslav' account
                if "yaroslav" not in account_info["username"].lower():
                    messenger_accounts.append(account_key)

        if not messenger_accounts:
            print("❌ No messenger accounts configured!")
            return

        print("� Available messenger accounts:")
        for i, account_key in enumerate(messenger_accounts, 1):
            account_info = self.accounts[account_key]
            print(
                f"   {i}. {account_key}: {account_info['name']} ({account_info['username']})"
            )

        # Show mode options
        print("\n📋 Messaging mode:")
        for i, account_key in enumerate(messenger_accounts, 1):
            print(f"   {i}. 👤 Single account ({account_key})")
        if len(messenger_accounts) > 1:
            print(f"   A. 👥 All accounts (even split)")
            print(f"   C. 🎯 Choose specific accounts (e.g. 1,3 or 2,3)")

        print(
            f"\n💡 For single account enter number (1-{len(messenger_accounts)})"
        )
        if len(messenger_accounts) > 1:
            print(f"💡 For all accounts enter 'A'")
            print(
                f"💡 For custom accounts enter 'C' or numbers separated by comma (e.g. 1,3)"
            )

        mode_choice = input("➡️ Your choice: ").strip().upper()
        selected_accounts = []
        selected_mode = ""

        if mode_choice == "A" and len(messenger_accounts) > 1:
            selected_accounts = messenger_accounts
            selected_mode = "multi_messenger"
        elif mode_choice == "C" and len(messenger_accounts) > 1:
            account_choice = input(
                f"➡️ Enter account numbers separated by comma (1-{len(messenger_accounts)}): "
            ).strip()
            try:
                account_numbers = [
                    int(x.strip()) for x in account_choice.split(",")
                ]
                if all(
                    1 <= num <= len(messenger_accounts)
                    for num in account_numbers
                ):
                    selected_accounts = [
                        messenger_accounts[num - 1] for num in account_numbers
                    ]
                    selected_mode = (
                        "custom_multi"
                        if len(selected_accounts) > 1
                        else f"single_{selected_accounts[0]}"
                    )
                else:
                    print("❌ Invalid account numbers. Operation cancelled.")
                    return
            except ValueError:
                print(
                    "❌ Invalid format. Use numbers separated by comma (e.g. 1,3). Operation cancelled."
                )
                return
        elif "," in mode_choice:
            try:
                account_numbers = [
                    int(x.strip()) for x in mode_choice.split(",")
                ]
                if all(
                    1 <= num <= len(messenger_accounts)
                    for num in account_numbers
                ):
                    selected_accounts = [
                        messenger_accounts[num - 1] for num in account_numbers
                    ]
                    selected_mode = (
                        "custom_multi"
                        if len(selected_accounts) > 1
                        else f"single_{selected_accounts[0]}"
                    )
                else:
                    print("❌ Invalid account numbers. Operation cancelled.")
                    return
            except ValueError:
                print(
                    "❌ Invalid format. Use numbers separated by comma (e.g. 1,3). Operation cancelled."
                )
                return
        elif mode_choice.isdigit():
            choice_num = int(mode_choice)
            if 1 <= choice_num <= len(messenger_accounts):
                selected_accounts = [messenger_accounts[choice_num - 1]]
                selected_mode = f"single_{selected_accounts[0]}"
            else:
                print("❌ Invalid account number. Operation cancelled.")
                return
        else:
            print("❌ Invalid choice. Operation cancelled.")
            return

        # CSV file selection
        data_dir = self.base_scraper.get_data_dir()
        csv_files = []
        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith(".csv"):
                    csv_files.append(file)
        if not csv_files:
            print(f"❌ No CSV files found in {data_dir}/")
            return

        print("\n� Available CSV files:")
        for i, file in enumerate(csv_files, 1):
            file_path = os.path.join(data_dir, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    count = sum(1 for row in reader)
                print(f"   {i}. {file} ({count} contacts)")
            except:
                print(f"   {i}. {file} (unable to read)")

        file_choice = input(f"➡️ Choose file (1-{len(csv_files)}): ").strip()
        try:
            file_index = int(file_choice) - 1
            if 0 <= file_index < len(csv_files):
                selected_file = os.path.join(data_dir, csv_files[file_index])
                print(f"\n📁 Selected: {csv_files[file_index]}")
            else:
                print("❌ Invalid file selection")
                return
        except ValueError:
            print("❌ Invalid input")
            return

        # Filter settings
        print("\n🔧 FILTER SETTINGS")
        print("=" * 30)
        print("Available filters:")
        print(
            "1. Gaming vertical filter: Excludes 'land-based' companies (always enabled)"
        )
        print(
            "2. Position filter: Only includes relevant positions like CEO, CFO, Payments, etc."
        )
        position_filter_choice = (
            input("➡️ Enable position filter? (y/n, default: y): ")
            .strip()
            .lower()
        )
        enable_position_filter = position_filter_choice != "n"
        if enable_position_filter:
            print(
                "✅ Position filter enabled - will only target relevant positions"
            )
        else:
            print("⚠️ Position filter disabled - will target ALL positions")

        # Show filtered contact count
        try:
            user_data = self.data_processor.extract_user_data_from_csv(
                selected_file,
                apply_filters=True,
                enable_position_filter=enable_position_filter,
            )
            total_contacts = len(user_data)
            if total_contacts == 0:
                print("❌ No users to process after applying filters")
                print("💡 Try another file or check CSV structure")
                if not enable_position_filter:
                    print("💡 Or try enabling the position filter")
                return
        except Exception as e:
            print(f"❌ Error processing CSV file: {e}")
            fix_choice = input("➡️ Try to auto-fix CSV file? (y/n): ").lower()
            if fix_choice == "y":
                print("🔧 Attempting to fix file...")
                if self.data_processor.fix_malformed_csv(selected_file):
                    print("✅ File fixed, retrying...")
                    try:
                        user_data = (
                            self.data_processor.extract_user_data_from_csv(
                                selected_file,
                                apply_filters=True,
                                enable_position_filter=enable_position_filter,
                            )
                        )
                        total_contacts = len(user_data)
                        if total_contacts == 0:
                            print("❌ No users to process after fixing")
                            return
                        else:
                            print(
                                f"✅ Loaded {total_contacts} contacts after fixing"
                            )
                    except Exception as e2:
                        print(f"❌ Still error after fixing: {e2}")
                        return
                else:
                    print("❌ Could not fix file")
                    return
            else:
                print("❌ Please check the file manually and try again")
                return

        # User limit
        limit_input = input(
            f"➡️ User limit (default: all {total_contacts} users, or enter number): "
        ).strip()
        try:
            user_limit = int(limit_input) if limit_input else None
            if user_limit and user_limit > total_contacts:
                print(
                    f"⚠️ Limit {user_limit} exceeds available users ({total_contacts}), using all"
                )
                user_limit = None
        except:
            user_limit = None

        # Show work distribution
        actual_users = user_limit if user_limit else total_contacts
        if selected_mode == "multi_messenger":
            split = actual_users // len(selected_accounts)
            print(f"\n📊 Work distribution for {actual_users} users:")
            for idx, acc in enumerate(selected_accounts):
                count = (
                    split
                    if idx < len(selected_accounts) - 1
                    else actual_users - split * (len(selected_accounts) - 1)
                )
                print(f"   👤 {self.accounts[acc]['name']}: {count} contacts")
        elif selected_mode == "custom_multi":
            split = actual_users // len(selected_accounts)
            print(f"\n� Work distribution for {actual_users} users:")
            for idx, acc in enumerate(selected_accounts):
                count = (
                    split
                    if idx < len(selected_accounts) - 1
                    else actual_users - split * (len(selected_accounts) - 1)
                )
                print(f"   👤 {self.accounts[acc]['name']}: {count} contacts")
        else:
            acc = selected_accounts[0]
            print(
                f"\n👤 Single account: {self.accounts[acc]['name']} will process {actual_users} users"
            )

        # Show message templates
        print("\n💬 Message templates (random selection):")
        for i, template in enumerate(self.messaging.follow_up_messages, 1):
            preview = template.replace("{name}", "[NAME]")
            preview_short = preview[:100] + (
                "..." if len(preview) > 100 else ""
            )
            print(f"   {i}. {preview_short}")
        print(f"\n💬 Automatic follow-up message:")
        print(f"   → {self.messaging.second_follow_up_message}")
        print(
            f"\n⚠️ Will send random message template + automatic follow-up (5s delay) to users without existing chats"
        )

        # Delay
        delay = input(
            "➡️ Delay between contacts in seconds (default 8, includes 5s for follow-up): "
        ).strip()
        try:
            delay_seconds = int(delay) if delay else 8
        except:
            delay_seconds = 8

        # Final confirmation
        if selected_mode == "multi_messenger":
            mode_text = (
                f"from all {len(selected_accounts)} accounts (even split)"
            )
        elif selected_mode == "custom_multi":
            account_names = [
                self.accounts[acc]["name"] for acc in selected_accounts
            ]
            mode_text = f"from selected accounts: {', '.join(account_names)} (even split)"
        else:
            acc = selected_accounts[0]
            mode_text = f"from single account ({self.accounts[acc]['name']})"
        confirm = input(
            f"Start messaging {mode_text} for {actual_users} users with {delay_seconds}s delay? (y/n): "
        ).lower()
        if confirm != "y":
            print("❌ Multi-messenger messaging cancelled")
            return

        # Call appropriate bulk messaging method
        if selected_mode == "multi_messenger":
            self.bulk_message_multi_account(
                selected_file,
                delay_seconds,
                user_limit,
                enable_position_filter,
            )
        elif selected_mode == "custom_multi":
            self.bulk_message_custom_accounts(
                selected_file,
                selected_accounts,
                delay_seconds,
                user_limit,
                enable_position_filter,
            )
        else:
            acc = selected_accounts[0]
            self.bulk_message_single_account(
                selected_file,
                acc,
                delay_seconds,
                user_limit,
                enable_position_filter,
            )

    def bulk_message_multi_account(
        self,
        csv_file: str,
        delay_seconds: int = 3,
        user_limit: int = None,
        enable_position_filter: bool = True,
    ):
        """Відправляє повідомлення з CSV файлу розподіляючи дані між доступними messenger акаунтами"""
        # Отримуємо список доступних messenger акаунтів (тільки messenger1 та messenger3)
        messenger_accounts = ["messenger1", "messenger3"]

        if not messenger_accounts:
            print("❌ Немає доступних messenger акаунтів")
            return 0, 0

        print(
            f"\n📬 РОЗСИЛКА ПОВІДОМЛЕНЬ З {len(messenger_accounts)} MESSENGER АКАУНТІВ: {csv_file}"
        )

        # Витягуємо дані користувачів з CSV
        user_data = self.data_processor.extract_user_data_from_csv(
            csv_file,
            apply_filters=True,
            enable_position_filter=enable_position_filter,
        )

        if not user_data:
            print("❌ Не знайдено користувачів для обробки")
            return 0, 0

        # Застосовуємо ліміт користувачів якщо вказано
        if user_limit and user_limit > 0:
            if len(user_data) > user_limit:
                user_data = user_data[:user_limit]
                original_count = len(
                    self.data_processor.extract_user_data_from_csv(
                        csv_file,
                        apply_filters=True,
                        enable_position_filter=enable_position_filter,
                    )
                )
                print(
                    f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {original_count} доступних користувачів"
                )

        # Розділяємо дані між доступними messenger акаунтами
        total_users = len(user_data)
        num_accounts = len(messenger_accounts)

        # Створюємо батчі для кожного акаунта
        batches = []
        batch_size = total_users // num_accounts
        remainder = total_users % num_accounts

        start_idx = 0
        for i in range(num_accounts):
            # Останній акаунт отримує залишок
            current_batch_size = batch_size + (1 if i < remainder else 0)
            end_idx = start_idx + current_batch_size
            batches.append(user_data[start_idx:end_idx])
            start_idx = end_idx

        print(f"📊 Розподіл контактів:")
        for i, account_key in enumerate(messenger_accounts):
            print(
                f"   👤 {account_key} ({self.accounts[account_key]['name']}): {len(batches[i])} контактів"
            )

        total_success = 0
        total_failed = 0

        # Обробляємо кожен акаунт
        for i, account_key in enumerate(messenger_accounts):
            if not batches[i]:
                continue

            print(f"\n🔄 Переключаємося на {account_key}...")
            self.switch_account(account_key)

            # Додаткова затримка після переключення акаунтів (крім першого)
            if i > 0:
                print(f"   ⏱️ Чекаємо 5 секунд після переключення акаунта...")
                time.sleep(5)

            success, failed = self._process_user_batch(
                batches[i], delay_seconds, account_key
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

    def bulk_message_custom_accounts(
        self,
        csv_file: str,
        selected_accounts: list,
        delay_seconds: int = 3,
        user_limit: int = None,
        enable_position_filter: bool = True,
    ):
        """Відправляє повідомлення з CSV файлу розподіляючи дані між вибраними messenger акаунтами"""
        print(
            f"\n📬 РОЗСИЛКА ПОВІДОМЛЕНЬ З ВИБРАНИХ MESSENGER АКАУНТІВ: {csv_file}"
        )

        account_names = [
            self.accounts[acc]["name"] for acc in selected_accounts
        ]
        print(f"🎯 Вибрані акаунти: {', '.join(account_names)}")

        # Витягуємо дані користувачів з CSV
        user_data = self.data_processor.extract_user_data_from_csv(
            csv_file,
            apply_filters=True,
            enable_position_filter=enable_position_filter,
        )

        if not user_data:
            print("❌ Не знайдено користувачів для обробки")
            return 0, 0

        # Застосовуємо ліміт користувачів якщо вказано
        if user_limit and user_limit > 0:
            if len(user_data) > user_limit:
                user_data = user_data[:user_limit]
                original_count = len(
                    self.data_processor.extract_user_data_from_csv(
                        csv_file,
                        apply_filters=True,
                        enable_position_filter=enable_position_filter,
                    )
                )
                print(
                    f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {original_count} доступних користувачів"
                )

        # Розділяємо дані між вибраними messenger акаунтами
        total_users = len(user_data)
        num_accounts = len(selected_accounts)

        # Создаем батчи для каждого аккаунта
        user_batches = []
        users_per_batch = total_users // num_accounts
        remainder = total_users % num_accounts

        start_idx = 0
        for i, account_key in enumerate(selected_accounts):
            # Добавляем по одному дополнительному пользователю к первым remainder батчам
            batch_size = users_per_batch + (1 if i < remainder else 0)
            end_idx = start_idx + batch_size
            batch_data = user_data[start_idx:end_idx]
            user_batches.append((account_key, batch_data))
            start_idx = end_idx

        print(f"📊 Розподіл контактів:")
        for account_key, batch_data in user_batches:
            account_name = self.accounts[account_key]["name"]
            print(
                f"   👤 {account_key} ({account_name}): {len(batch_data)} контактів"
            )

        total_success = 0
        total_failed = 0

        # Обробляємо кожним вибраним акаунтом
        for i, (account_key, batch_data) in enumerate(user_batches):
            if batch_data:
                print(f"\n🔄 Переключаємося на {account_key}...")
                self.switch_account(account_key)

                if (
                    i > 0
                ):  # Додаткова затримка після переключення акаунтів (крім першого)
                    print(
                        f"   ⏱️ Чекаємо 5 секунд після переключення акаунта..."
                    )
                    time.sleep(5)

                account_name = self.accounts[account_key]["name"].replace(
                    "Messenger Account ", "Messenger"
                )
                success, failed = self._process_user_batch(
                    batch_data, delay_seconds, account_name
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

    def bulk_message_single_account(
        self,
        csv_file: str,
        account_key: str,
        delay_seconds: int = 3,
        user_limit: int = None,
        enable_position_filter: bool = True,
    ):
        """Відправляє повідомлення з CSV файлу використовуючи лише один messenger акаунт"""
        print(
            f"\n📬 РОЗСИЛКА ПОВІДОМЛЕНЬ З ОДНОГО MESSENGER АКАУНТА ({self.accounts[account_key]['name']}): {csv_file}"
        )

        # Витягуємо дані користувачів з CSV
        user_data = self.data_processor.extract_user_data_from_csv(
            csv_file,
            apply_filters=True,
            enable_position_filter=enable_position_filter,
        )

        if not user_data:
            print("❌ Не знайдено користувачів для обробки")
            return 0, 0

        # Застосовуємо ліміт користувачів якщо вказано
        if user_limit and user_limit > 0:
            if len(user_data) > user_limit:
                user_data = user_data[:user_limit]
                original_count = len(
                    self.data_processor.extract_user_data_from_csv(
                        csv_file,
                        apply_filters=True,
                        enable_position_filter=enable_position_filter,
                    )
                )
                print(
                    f"🔢 Застосовано ліміт: оброблятимемо {user_limit} з {original_count} доступних користувачів"
                )

        print(f"📊 Інформація про розсилку:")
        print(f"   👤 Акаунт: {self.accounts[account_key]['name']}")
        print(f"   📧 Контактів: {len(user_data)}")

        # Переключаємося на вказаний акаунт
        print(f"\n🔄 Переключаємося на {account_key}...")
        if not self.switch_account(account_key):
            print(f"❌ Помилка переключення на акаунт {account_key}")
            return 0, 0

        # Обробляємо всіх користувачів одним акаунтом
        success, failed = self._process_user_batch(
            user_data, delay_seconds, f"Single {account_key}"
        )

        print(f"\n📊 ПІДСУМОК РОЗСИЛКИ:")
        print(f"   ✅ Успішно: {success}")
        print(f"   ❌ Помилок: {failed}")
        print(
            f"   📈 Успішність: {(success/(success+failed)*100):.1f}%"
            if (success + failed) > 0
            else "N/A"
        )

        return success, failed

    def _process_user_batch(self, user_data, delay_seconds, account_name):
        """Обробляє групу користувачів для одного акаунта з перевіркою виключених компаній"""
        print(f"\n📬 Обробка {len(user_data)} контактів для {account_name}")

        # Завантажуємо існуючі чати для поточного акаунта
        print("📥 Завантажуємо існуючі чати...")
        self.messaging.load_chats_list(self.accounts)

        success_count = 0
        failed_count = 0
        skipped_count = 0
        excluded_count = 0

        for i, user_info in enumerate(user_data, 1):
            user_id = user_info["user_id"]
            first_name = user_info["first_name"]
            full_name = user_info["full_name"]
            company_name = user_info.get("company_name", "")

            company_info = f" ({company_name})" if company_name else ""
            print(
                f"\n[{i}/{len(user_data)}] Обробляємо {full_name}{company_info} (ID: {user_id})..."
            )

            try:
                # Використовуємо звичайні повідомлення (з автоматичним follow-up)
                message_template = random.choice(
                    self.messaging.follow_up_messages
                )
                message = message_template.format(name=first_name)

                print(
                    f"   💬 Відправляємо: '{message_template[:50]}...' з ім'ям '{first_name}'"
                )
                print(
                    f"   💬 + автоматичний follow-up: '{self.messaging.second_follow_up_message}'"
                )

                # Відправляємо повідомлення (з автоматичним follow-up та перевіркою чату)
                success = self.messaging.send_message_to_user(
                    user_id, message, self.accounts, full_name, company_name
                )

                if success == "success":
                    print(f"   ✅ Повідомлення відправлено")
                    success_count += 1
                elif success == "already_contacted":
                    print(f"   ⏭️ Пропущено (чат вже має повідомлення)")
                    skipped_count += 1
                elif success == "excluded_company":
                    print(f"   🚫 Пропущено (компанія виключена)")
                    excluded_count += 1
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
        print(f"   ⏭️ Пропущено (чат існує): {skipped_count}")
        print(f"   🚫 Виключено (компанія): {excluded_count}")
        print(f"   ❌ Помилок: {failed_count}")

        return success_count, failed_count

    def handle_followup_campaigns(self):
        """Handles follow-up campaigns (full parity with original)"""
        print("\n📬 FOLLOW-UP CAMPAIGNS")
        print("=" * 40)

        from datetime import datetime
        from zoneinfo import ZoneInfo

        kyiv_tz = ZoneInfo("Europe/Kiev")
        current_date = datetime.now(kyiv_tz)
        sbc_date = self.sbc_start_date
        days_until_sbc = (sbc_date - current_date).days

        print(f"📅 Current date: {current_date.strftime('%d.%m.%Y')}")
        print(f"📅 SBC Summit date: {sbc_date.strftime('%d.%m.%Y')}")
        print(f"⏰ Days until conference: {days_until_sbc}")

        print("\n📋 Follow-up rules:")
        print("   � Follow-up 1: 3 days after first message")
        print("   📧 Follow-up 2: 7 days after first message")
        print("   📧 Follow-up 3: 1 day before SBC Summit")

        print("\n🔧 Mode:")
        print("   1. � Optimized (CSV filter - fast)")
        print(
            "      • Analyzes only contacts with 'Sent' status and no response"
        )
        print("      • Filters by message author from CSV")
        print("      • Checks dates and sends follow-up as per rules")
        print("   2. � Full analysis (all chats - slow)")
        print("      • Loads all chats from account")
        print("      • Analyzes each chat for follow-up")
        print("   3. � By author (auto-split by accounts)")
        print("      • Splits contacts by 'author' field in CSV")
        print("      • Uses corresponding account for each author")

        mode_choice = input("➡️ Choose mode (1-3): ").strip()

        if mode_choice == "1":
            method_to_use = "optimized"
            print("✅ Using optimized mode")
            filter_choice = (
                input("➡️ Use gaming vertical filters? (y/n): ").strip().lower()
            )
            use_filters = filter_choice == "y"
            position_filter_choice = (
                input("➡️ Use position filter (CEO, CFO, etc)? (y/n): ")
                .strip()
                .lower()
            )
            enable_position_filter = position_filter_choice == "y"
            if enable_position_filter:
                print("🎯 Position filter enabled")
            else:
                print("⚠️ Position filter disabled - all positions included")
        elif mode_choice == "2":
            method_to_use = "full"
            use_filters = False
            enable_position_filter = False
            print("✅ Using full analysis mode")
        elif mode_choice == "3":
            method_to_use = "by_author"
            use_filters = False
            print("✅ Using by author mode")
            position_filter_choice = (
                input("➡️ Use position filter (CEO, CFO, etc)? (y/n): ")
                .strip()
                .lower()
            )
            enable_position_filter = position_filter_choice == "y"
            if enable_position_filter:
                print("🎯 Position filter enabled for by author mode")
            else:
                print("⚠️ Position filter disabled - all positions included")
        else:
            print("❌ Invalid choice, using optimized mode")
            method_to_use = "optimized"
            use_filters = False
            enable_position_filter = False

        # Special handling for by_author method
        if method_to_use == "by_author":
            print("\n🚀 Starting follow-up campaigns by author...")
            stats = self.process_followup_campaigns_by_author(
                enable_position_filter
            )
            return

        # Show available messenger accounts
        messenger_accounts = [
            k for k, v in self.accounts.items() if v["role"] == "messaging"
        ]
        print(f"\n🔧 Available messenger accounts:")
        for i, acc_key in enumerate(messenger_accounts, 1):
            acc = self.accounts[acc_key]
            print(f"   {i}. {acc['name']} ({acc['username']})")
        print(f"   {len(messenger_accounts)+1}. All accounts sequentially")

        account_choice = input(
            f"➡️ Choose account for processing (1-{len(messenger_accounts)+1}): "
        ).strip()
        try:
            if account_choice == str(len(messenger_accounts) + 1):
                print("\n🔄 Processing with all accounts sequentially...")
                combined_stats = {}
                for idx, acc_key in enumerate(messenger_accounts, 1):
                    print("\n" + "=" * 50)
                    print(f"� MESSENGER {idx}")
                    print("=" * 50)
                    if method_to_use == "optimized":
                        stats = self.process_followup_campaigns_optimized(
                            acc_key, use_filters, enable_position_filter
                        )
                    else:
                        stats = self.process_followup_campaigns(acc_key)
                    for key in stats:
                        combined_stats[key] = combined_stats.get(
                            key, 0
                        ) + stats.get(key, 0)
                print(f"\n📊 OVERALL STATISTICS:")
                print(
                    f"   📋 Chats analyzed: {combined_stats.get('analyzed', 0)}"
                )
                if method_to_use == "full":
                    print(
                        f"   💾 chat_id stored: {combined_stats.get('chat_ids_stored', 0)}"
                    )
                print(
                    f"   ✅ With responses: {combined_stats.get('has_responses', 0)}"
                )
                print(
                    f"   📧 Follow-up 3 days: {combined_stats.get('day_3_sent', 0)}"
                )
                print(
                    f"   📧 Follow-up 7 days: {combined_stats.get('day_7_sent', 0)}"
                )
                print(
                    f"   📧 Final follow-up: {combined_stats.get('final_sent', 0)}"
                )
                print(f"   ❌ Errors: {combined_stats.get('errors', 0)}")
                total_sent = (
                    combined_stats.get("day_3_sent", 0)
                    + combined_stats.get("day_7_sent", 0)
                    + combined_stats.get("final_sent", 0)
                )
                print(f"   📈 Total sent: {total_sent}")
            elif account_choice.isdigit() and 1 <= int(account_choice) <= len(
                messenger_accounts
            ):
                acc_key = messenger_accounts[int(account_choice) - 1]
                if method_to_use == "optimized":
                    stats = self.process_followup_campaigns_optimized(
                        acc_key, use_filters, enable_position_filter
                    )
                else:
                    stats = self.process_followup_campaigns(acc_key)
            else:
                print("❌ Invalid choice")
                return
        except Exception as e:
            print(f"❌ Error running follow-up campaign: {e}")

    def handle_conference_followup(self):
        """Handles conference followup for positive conversations (full parity with original)"""
        print("\n👁️ CONFERENCE FOLLOWUP FOR POSITIVE CONVERSATIONS")
        print("=" * 60)
        print("Цей режим:")
        print("• Перевіряє всі messenger акаунти")
        print("• Аналізує чати з відповідями")
        print("• Визначає мову повідомлень")
        print("• Розпізнає позитивний сентимент")
        print("• Відправляє conference followup тільки для позитивних розмов")
        print("=" * 60)

        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
        if not os.path.exists(csv_file):
            print(f"❌ Main CSV file not found: {csv_file}")
            print(
                "   First run 'Check for responses' to populate response data"
            )
            return

        print(f"📁 Використовуємо CSV: {csv_file}")

        # Show current CSV status for responses
        print("\n📊 Поточний статус CSV:")
        response_stats = self.show_csv_status_for_responses(csv_file)
        if response_stats.get("with_responses", 0) == 0:
            print("❌ Немає чатів з відповідями для аналізу")
            print(
                "   Спочатку запустіть 'Check for responses and update CSV status'"
            )
            return

        print(
            f"\n📬 Знайдено {response_stats['with_responses']} чатів з відповідями"
        )

        confirm = input(
            "\n➡️ Продовжити з conference followup кампанією? (y/n): "
        ).lower()
        if confirm != "y":
            print("❌ Операцію скасовано")
            return

        try:
            print(f"\n🚀 Запускаємо conference followup кампанію...")
            stats = self.messaging.process_positive_conversation_followups(
                csv_file, self.accounts
            )
            if stats.get("error"):
                print("❌ Кампанія завершена з помилками")
            else:
                print(f"\n✅ КАМПАНІЯ ЗАВЕРШЕНА УСПІШНО!")
                print(f"📈 Результати:")
                print(
                    f"   📬 Чатів проаналізовано: {stats.get('total_chats_checked', 0)}"
                )
                print(
                    f"   ✅ Позитивних розмов: {stats.get('positive_conversations', 0)}"
                )
                print(
                    f"   📨 Conference followup відправлено: {stats.get('conference_followups_sent', 0)}"
                )
                if stats.get("positive_conversations", 0) > 0:
                    success_rate = (
                        stats.get("conference_followups_sent", 0)
                        / stats.get("positive_conversations", 0)
                    ) * 100
                    print(f"   📊 Успішність: {success_rate:.1f}%")
        except Exception as e:
            print(f"❌ Помилка виконання conference followup кампанії: {e}")
            import traceback

            traceback.print_exc()

    def handle_check_responses(self):
        """Handles checking responses and updating CSV status (full parity with original)"""
        print("\n📬 CHECK FOR RESPONSES IN ALL CHATS (OPTIMIZED)")
        print("=" * 40)

        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
        if not os.path.exists(csv_file):
            print(f"❌ Main CSV file not found: {csv_file}")
            print("   First run 'Scrape new contacts' to create the file")
            return

        # Show CSV statistics before starting
        print("📊 CSV статистика:")
        csv_stats = self.show_csv_status_for_responses(csv_file)
        if csv_stats:
            print(f"   📋 Всього записів: {csv_stats['total_records']}")
            print(f"   📤 'Sent': {csv_stats['sent_status']}")
            print(f"   ✅ 'Sent Answer': {csv_stats['sent_answer_status']}")
            print(f"   📨 З 'answer': {csv_stats['answer_status']}")
            print(f"   📭 Порожні: {csv_stats['empty_status']}")
            print(f"   ✓ 'True': {csv_stats['true_status']}")
            print(f"   💬 З chat_id: {csv_stats['has_chat_id']}")
            print(f"   🔍 Потребують перевірки: {csv_stats['needs_checking']}")

        print(f"\n📁 CSV file: {csv_file}")
        print(
            "📋 ОПТИМІЗОВАНИЙ процес - перевіряє ТІЛЬКИ чати контактів зі статусом:"
        )
        print("   ✅ 'Sent' (відправлено повідомлення)")
        print("   ✅ Порожнє значення")
        print("   ✅ 'True'")
        print("   ✅ І у яких є chat_id (встановлений контакт)")
        print("\n🚫 НЕ перевіряє:")
        print("   ❌ Будь-який статус з 'answer' (вже є відповідь)")
        print("   ❌ Записи без chat_id")
        print("   ❌ Групові чати")

        print("\n🔧 Optimized Process:")
        print("   1. 🔍 Фільтрація CSV за статусом (тільки релевантні записи)")
        print("   2. 📬 Check only filtered chats from messenger1 account")
        print("   3. 📬 Check only filtered chats from messenger2 account")
        print(
            "   4. 📝 Update CSV status to 'Sent Answer' for responded contacts"
        )
        print(
            "   5. 🏷️ Set Follow-up column to 'Answer' for responded contacts"
        )

        print(f"\n👥 Messenger accounts to check:")
        messenger_accounts = [
            k for k, v in self.accounts.items() if v["role"] == "messaging"
        ]
        for account_key in messenger_accounts:
            if account_key in self.accounts:
                account = self.accounts[account_key]
                print(f"   • {account['name']} ({account['username']})")
            else:
                print(f"   ⚠️ {account_key} - not configured")

        if csv_stats.get("needs_checking", 0) == 0:
            print(
                "\n✅ Немає записів для перевірки! Всі контакти вже оброблені."
            )
            return

        confirm = input(
            f"\n🤔 Proceed with checking {csv_stats.get('needs_checking', 0)} filtered chats? (y/n): "
        ).lower()
        if confirm == "y":
            try:
                print(
                    f"\n🚀 Starting optimized response check for {csv_stats.get('needs_checking', 0)} chats..."
                )
                stats = self.messaging.check_all_responses_and_update_csv(
                    csv_file, self.accounts
                )
                if "error" in stats:
                    print("❌ Process failed")
                else:
                    print("\n✅ Response check completed successfully!")
                    print("\n� Оновлена статистика:")
                    new_stats = self.show_csv_status_for_responses(csv_file)
                    if new_stats:
                        old_answered = csv_stats.get("sent_answer_status", 0)
                        new_answered = new_stats.get("sent_answer_status", 0)
                        increase = new_answered - old_answered
                        print(
                            f"   ✅ 'Sent Answer': {old_answered} → {new_answered} (+{increase})"
                        )
                        print(
                            f"   🔍 Потребують перевірки: {csv_stats.get('needs_checking', 0)} → {new_stats.get('needs_checking', 0)}"
                        )
            except Exception as e:
                print(f"❌ Error during response check: {e}")
                import traceback

                traceback.print_exc()
        else:
            print("❌ Response check cancelled")

    def handle_update_csv_contacts(self):
        """Handles updating existing CSV with contacts (full parity with original)"""
        print("\n� UPDATE EXISTING CSV WITH CONTACTS")
        print("=" * 40)

        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
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

    def handle_excluded_companies(self):
        """Handles excluded companies management (full parity with original)"""
        while True:
            print("\n🚫 УПРАВЛІННЯ ВИКЛЮЧЕНИМИ КОМПАНІЯМИ")
            print("=" * 40)
            count = (
                len(self.company_filter.excluded_companies)
                if hasattr(self.company_filter, "excluded_companies")
                else 0
            )
            print(f"� Поточно виключено: {count} компаній")
            print("1. � Показати список виключених компаній")
            print("2. 🧪 Тестувати компанію")
            print("3. 🔄 Перезавантажити список з файлу")
            print("4. ↩️ Повернутися до головного меню")

            choice = input("➡️ Виберіть опцію (1-4): ").strip()

            if choice == "1":
                self.company_filter.show_excluded_companies()
                input("\n⏎ Натисніть Enter для продовження...")
            elif choice == "2":
                company_name = input(
                    "\n🏢 Введіть назву компанії для тесту: "
                ).strip()
                if company_name:
                    self.company_filter.test_company_exclusion(company_name)
                else:
                    print("❌ Назва компанії не може бути порожньою")
                input("\n⏎ Натисніть Enter для продовження...")
            elif choice == "3":
                print("🔄 Перезавантажуємо список виключених компаній...")
                old_count = count
                self.company_filter.reload_excluded_companies()
                new_count = (
                    len(self.company_filter.excluded_companies)
                    if hasattr(self.company_filter, "excluded_companies")
                    else 0
                )
                print(f"✅ Готово! Компаній: {old_count} → {new_count}")
                input("\n⏎ Натисніть Enter для продовження...")
            elif choice == "4":
                break
            else:
                print("❌ Невірний вибір. Виберіть 1-4.")
                input("\n⏎ Натисніть Enter для продовження...")

    def show_account_status(self):
        """Shows account status information (full parity with original)"""
        print("\n📊 ACCOUNT STATUS")
        print("=" * 40)
        print("=" * 40)
        current = (
            self.base_scraper.current_account
            if hasattr(self.base_scraper, "current_account")
            else None
        )
        if current:
            print(f"� Currently active: {self.accounts[current]['name']}")
        else:
            print("� Currently active: None")

        print("\n📋 All accounts configuration:")
        for key, account in self.accounts.items():
            status = "🟢 ACTIVE" if key == current else "⭕ INACTIVE"
            role_emoji = "🔍" if account["role"] == "scraping" else "💬"
            is_configured = (
                account.get("username")
                and not str(account["username"]).startswith(
                    ("MESSENGER", "your_")
                )
                and account.get("password")
                and not str(account["password"]).startswith(
                    ("MESSENGER", "your_")
                )
                and account.get("user_id")
                and not str(account["user_id"]).startswith(
                    ("MESSENGER", "your_")
                )
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
        print(
            "   💬 messenger1/messenger2/messenger3 - Used for sending messages"
        )
        print(
            "\n💡 To configure accounts, edit your .env file with real credentials"
        )
        count = (
            len(self.company_filter.excluded_companies)
            if hasattr(self.company_filter, "excluded_companies")
            else 0
        )
        print(f"\n🚫 Company Exclusions: {count} companies loaded")

    def run_update(self):
        """Runs the attendee update process with detailed logging and robust comparison"""
        print("\n============================================================")
        print("🔄 ОНОВЛЕННЯ БАЗИ УЧАСНИКІВ SBC SUMMIT 2025")
        print("============================================================\n")

        print("📡 Етап 1: Завантаження даних з advanced search...")
        all_results = []
        batch_size = 2000
        total_fetched = 0
        for from_index in range(0, 20000, batch_size):
            print(f"📥 Завантажуємо з індексу {from_index}...")
            batch = self.base_scraper.advanced_search(
                from_index=from_index, size=batch_size
            )
            if not batch:
                print(f"   ❌ Не вдалося отримати дані з індексу {from_index}")
                break
            all_results.extend(batch)
            total_fetched += len(batch)
            print(
                f"   ✅ Отримано {len(batch)} записів (всього: {total_fetched})"
            )
            if len(batch) < batch_size:
                print(
                    f"   📊 Досягнуто кінця (отримано {len(batch)} < {batch_size})"
                )
                break
        print(f"✅ Всього знайдено: {len(all_results)} учасників\n")

        print("📋 Етап 2: Порівняння з існуючою базою...")
        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
        existing_keys = self.load_existing_attendees(csv_file)
        print(
            f"📋 Завантажено {len(existing_keys)} існуючих записів з {csv_file}"
        )

        new_attendees = self.find_new_attendees(all_results, existing_keys)
        print(f"🆕 Знайдено нових: {len(new_attendees)} учасників\n")

        if not new_attendees:
            print("✅ Немає нових учасників для додавання")
            return

        print("🔍 Етап 3: Отримання детальних даних...")
        detailed_attendees = self.process_new_attendees(new_attendees)

        self.save_new_attendees(detailed_attendees, csv_file)
        print(
            f"\n✅ Додавання нових учасників завершено. Всього додано: {len(detailed_attendees)}"
        )

    def load_existing_attendees(self, csv_file=None):
        """Loads existing attendees from CSV"""
        if not csv_file:
            csv_file = os.path.join(
                self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
            )

        existing_keys = set()

        if os.path.exists(csv_file):
            try:
                # Check if pandas is available by attempting import
                try:
                    import pandas as pd

                    pandas_available = True
                except ImportError:
                    pandas_available = False

                if pandas_available:
                    df = pd.read_csv(csv_file)
                    for _, row in df.iterrows():
                        source_url = row.get("source_url", "")
                        if source_url:
                            user_id = (
                                self.data_processor.extract_user_id_from_url(
                                    source_url
                                )
                            )
                            if user_id:
                                existing_keys.add(user_id)
                else:
                    # Fallback to basic CSV processing
                    import csv

                    with open(csv_file, "r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            source_url = row.get("source_url", "")
                            if source_url:
                                user_id = self.data_processor.extract_user_id_from_url(
                                    source_url
                                )
                                if user_id:
                                    existing_keys.add(user_id)

                print(f"📋 Loaded {len(existing_keys)} existing attendees")
            except Exception as e:
                print(f"⚠️ Error loading existing attendees: {e}")

        return existing_keys

    def find_new_attendees(self, search_results, existing_keys):
        """Finds new attendees not in existing database"""
        new_attendees = []

        for attendee in search_results:
            attendee_id = attendee.get("id")
            if attendee_id and attendee_id not in existing_keys:
                new_attendees.append(attendee)

        return new_attendees

    def process_new_attendees(self, new_attendees):
        """Processes new attendees to get detailed information"""
        detailed_attendees = []

        for i, attendee in enumerate(new_attendees, 1):
            attendee_id = attendee.get("id")
            print(f"📊 [{i}/{len(new_attendees)}] Processing {attendee_id}...")

            # Get detailed user information
            user_details = self.base_scraper.get_user_details(attendee_id)

            if user_details:
                formatted_attendee = self.format_attendee_for_csv(user_details)
                if formatted_attendee:
                    detailed_attendees.append(formatted_attendee)

            # Small delay to avoid overwhelming the API
            time.sleep(0.5)

        return detailed_attendees

    def format_attendee_for_csv(self, attendee_details):
        """Formats attendee details for CSV output"""
        try:
            # Extract basic information
            user_id = attendee_details.get("id", "")
            first_name = attendee_details.get("firstName", "")
            last_name = attendee_details.get("lastName", "")
            company_name = attendee_details.get("companyName", "")
            position = attendee_details.get("title", "")

            # Create full name
            full_name = f"{first_name} {last_name}".strip()

            # Create source URL
            source_url = f"https://sbcconnect.com/attendees/{user_id}"

            # Extract other details
            email = attendee_details.get("email", "")
            phone = attendee_details.get("phone", "")

            # Extract gaming vertical and organization type
            gaming_vertical = ""
            organization_type = ""

            if "gamingVerticals" in attendee_details:
                gaming_verticals = attendee_details["gamingVerticals"]
                if gaming_verticals:
                    gaming_vertical = ", ".join(gaming_verticals)

            if "organizationTypes" in attendee_details:
                org_types = attendee_details["organizationTypes"]
                if org_types:
                    organization_type = ", ".join(org_types)

            return {
                "source_url": source_url,
                "full_name": full_name,
                "company_name": company_name,
                "position": position,
                "email": email,
                "phone": phone,
                "gaming_vertical": gaming_vertical,
                "organization_type": organization_type,
                "connected": "",
                "Follow-up": "",
                "author": "",
                "Date": "",
                "valid": "",
                "Responded": "",
                "chat_id": "",
            }

        except Exception as e:
            print(f"❌ Error formatting attendee: {e}")
            return None

    def save_new_attendees(self, new_attendees_data, csv_file=None):
        """Saves new attendees to CSV file"""
        if not csv_file:
            csv_file = os.path.join(
                self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
            )

        if not new_attendees_data:
            print("No new attendees to save")
            return

        try:
            # Check if file exists to determine if we need headers
            file_exists = os.path.exists(csv_file)

            import csv

            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                if new_attendees_data:
                    fieldnames = new_attendees_data[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)

                    # Write header only if file is new
                    if not file_exists:
                        writer.writeheader()

                    # Write data
                    writer.writerows(new_attendees_data)

            print(
                f"✅ Saved {len(new_attendees_data)} new attendees to {csv_file}"
            )

        except Exception as e:
            print(f"❌ Error saving attendees: {e}")

    def process_followup_campaigns_optimized(
        self, csv_file: str, followup_type: str
    ) -> Dict[str, int]:
        """Optimized follow-up campaign processing"""
        print(f"\n📬 ОПТИМІЗОВАНА КАМПАНІЯ: {followup_type.upper()}")
        print(f"📁 CSV файл: {csv_file}")
        print("=" * 60)

        stats = {
            "total_accounts": 0,
            "total_candidates": 0,
            "messages_sent": 0,
            "already_sent": 0,
            "chat_not_found": 0,
            "errors": 0,
            "accounts_processed": [],
        }

        # Define account lists for different followup types
        account_mapping = {
            "weekly": ["messenger1", "messenger2"],
            "monthly": ["messenger1", "messenger3"],
            "conference_active": ["messenger1", "messenger2", "messenger3"],
        }

        accounts_to_use = account_mapping.get(followup_type, ["messenger1"])
        original_account = self.base_scraper.current_account

        try:
            # Get candidates from CSV
            candidates = self.data_processor.get_followup_candidates_from_csv(
                csv_file, followup_type
            )
            stats["total_candidates"] = len(candidates)

            print(f"🎯 Знайдено кандидатів: {len(candidates)}")

            for account_key in accounts_to_use:
                if account_key not in self.accounts:
                    continue

                print(f"\n🔄 Переключаємося на {account_key}...")
                if not self.base_scraper.switch_account(
                    account_key, self.accounts
                ):
                    print(f"❌ Не вдалося переключитись на {account_key}")
                    continue

                stats["accounts_processed"].append(account_key)
                stats["total_accounts"] += 1

                # Process candidates for this account
                for candidate in candidates:
                    chat_id = candidate["chat_id"]

                    try:
                        # Check if chat exists and is accessible
                        chat_details = self.messaging.load_chat_details(
                            chat_id
                        )
                        if not chat_details:
                            stats["chat_not_found"] += 1
                            continue

                        # Check if already sent
                        already_sent = (
                            self.messaging.check_followup_already_sent(
                                csv_file,
                                chat_id,
                                followup_type,
                                chat_details,
                                self.accounts,
                            )
                        )

                        if already_sent:
                            stats["already_sent"] += 1
                            continue

                        # Send followup message
                        participant_name = f"{candidate['first_name']} {candidate['last_name']}".strip()
                        language = self.messaging.detect_language(
                            participant_name
                        )

                        success = self.messaging.send_followup_message(
                            chat_id, followup_type, participant_name, language
                        )

                        if success:
                            stats["messages_sent"] += 1
                            # Update CSV
                            self.data_processor.update_csv_followup_status(
                                csv_file, chat_id, followup_type
                            )

                            # Small delay between messages
                            time.sleep(2)
                        else:
                            stats["errors"] += 1

                    except Exception as e:
                        print(f"⚠️ Помилка обробки кандидата {chat_id}: {e}")
                        stats["errors"] += 1

        except Exception as e:
            print(f"❌ Критична помилка в кампанії: {e}")
            stats["errors"] += 1

        finally:
            # Restore original account
            if (
                original_account
                and original_account != self.base_scraper.current_account
            ):
                self.base_scraper.switch_account(
                    original_account, self.accounts
                )

        # Print summary
        print(f"\n📊 ПІДСУМКИ КАМПАНІЇ {followup_type.upper()}:")
        print(f"   👥 Акаунтів використано: {stats['total_accounts']}")
        print(f"   🎯 Кандидатів знайдено: {stats['total_candidates']}")
        print(f"   📨 Повідомлень відправлено: {stats['messages_sent']}")
        print(f"   ⏭️ Вже відправлені: {stats['already_sent']}")
        print(f"   🔍 Чат не знайдено: {stats['chat_not_found']}")
        print(f"   ❌ Помилок: {stats['errors']}")
        print(f"   🔧 Акаунти: {', '.join(stats['accounts_processed'])}")

        return stats

    def process_followup_campaigns_by_author(
        self, csv_file: str, author_name: str
    ) -> Dict[str, int]:
        """Process follow-up campaigns filtered by specific author"""
        print(f"\n📬 КАМПАНІЯ ЗА АВТОРОМ: {author_name}")
        print(f"📁 CSV файл: {csv_file}")
        print("=" * 60)

        stats = {
            "total_checked": 0,
            "matching_author": 0,
            "weekly_sent": 0,
            "monthly_sent": 0,
            "already_sent": 0,
            "errors": 0,
        }

        try:
            with open(csv_file, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)

                for row in reader:
                    stats["total_checked"] += 1

                    # Check if this row matches the author
                    row_author = row.get("Author", "").strip()
                    if row_author != author_name:
                        continue

                    stats["matching_author"] += 1
                    chat_id = row.get("Chat ID", "").strip()

                    if not chat_id:
                        continue

                    # Determine followup type based on message status and timing
                    sent_status = row.get("Sent", "").strip().lower()
                    if sent_status not in ["true", "yes", "1", "sent"]:
                        continue

                    # Check what followups haven't been sent yet
                    weekly_sent = (
                        row.get("Weekly Follow-up", "").strip().lower()
                    )
                    monthly_sent = (
                        row.get("Monthly Follow-up", "").strip().lower()
                    )

                    participant_name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()

                    try:
                        # Try weekly followup first
                        if weekly_sent not in ["true", "yes", "1", "sent"]:
                            success = self.messaging.send_followup_message(
                                chat_id, "weekly", participant_name, "en"
                            )
                            if success:
                                stats["weekly_sent"] += 1
                                self.data_processor.update_csv_followup_status(
                                    csv_file, chat_id, "weekly"
                                )
                                time.sleep(2)

                        # Then try monthly followup
                        elif monthly_sent not in ["true", "yes", "1", "sent"]:
                            success = self.messaging.send_followup_message(
                                chat_id, "monthly", participant_name, "en"
                            )
                            if success:
                                stats["monthly_sent"] += 1
                                self.data_processor.update_csv_followup_status(
                                    csv_file, chat_id, "monthly"
                                )
                                time.sleep(2)
                        else:
                            stats["already_sent"] += 1

                    except Exception as e:
                        print(f"⚠️ Помилка відправки для {chat_id}: {e}")
                        stats["errors"] += 1

        except Exception as e:
            print(f"❌ Помилка обробки файлу: {e}")
            stats["errors"] += 1

        # Print summary
        print(f"\n📊 ПІДСУМКИ КАМПАНІЇ ЗА АВТОРОМ '{author_name}':")
        print(f"   📋 Рядків перевірено: {stats['total_checked']}")
        print(f"   👤 Збігів за автором: {stats['matching_author']}")
        print(f"   📨 Weekly відправлено: {stats['weekly_sent']}")
        print(f"   📅 Monthly відправлено: {stats['monthly_sent']}")
        print(f"   ⏭️ Вже відправлені: {stats['already_sent']}")
        print(f"   ❌ Помилок: {stats['errors']}")

        return stats

    def show_csv_status_for_responses(
        self, csv_file: str = None
    ) -> Dict[str, int]:
        """Показує статистику CSV файлу для перевірки відповідей"""
        if not csv_file:
            csv_file = os.path.join(
                self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
            )

        if not PANDAS_AVAILABLE:
            print("❌ pandas не встановлено")
            return {}

        if not os.path.exists(csv_file):
            print(f"❌ Файл {csv_file} не знайдено")
            return {}

        try:
            df = pd.read_csv(csv_file)

            # Підраховуємо статистику
            total_records = len(df)

            # Записи з різними статусами
            sent_status = len(df[df["connected"] == "Sent"])
            sent_answer_status = len(df[df["connected"] == "Sent Answer"])
            answer_status = len(
                df[
                    df["connected"].str.contains(
                        "answer", case=False, na=False
                    )
                ]
            )
            empty_status = len(
                df[df["connected"].isna() | (df["connected"] == "")]
            )
            true_status = len(df[df["connected"] == "True"])

            # Записи з chat_id
            has_chat_id = len(
                df[df["chat_id"].notna() & (df["chat_id"] != "")]
            )

            # Записи які потребують перевірки
            check_mask = (
                (
                    (df["connected"] == "Sent")
                    | (df["connected"].isna())
                    | (df["connected"] == "")
                    | (df["connected"] == "True")
                )
                & (
                    # Виключаємо тих, хто вже має відповідь (будь-яке значення що містить "answer")
                    (
                        ~df["connected"].str.contains(
                            "answer", case=False, na=False
                        )
                    )
                )
                & ((df["chat_id"].notna()) & (df["chat_id"] != ""))
            )
            needs_checking = len(df[check_mask])

            # Count responses from both connected and Comment columns
            with_responses_mask = (
                (df["connected"].str.contains("answer", case=False, na=False))
                | (
                    df["Comment"].str.contains(
                        "answered", case=False, na=False
                    )
                )
                | (
                    df["Comment"].str.contains(
                        "responded", case=False, na=False
                    )
                )
                | (df["Comment"].str.contains("replied", case=False, na=False))
            )
            with_responses = len(df[with_responses_mask])

            stats = {
                "total_records": total_records,
                "sent_status": sent_status,
                "sent_answer_status": sent_answer_status,
                "answer_status": answer_status,
                "empty_status": empty_status,
                "true_status": true_status,
                "has_chat_id": has_chat_id,
                "needs_checking": needs_checking,
                "with_responses": with_responses,
            }

            return stats

        except Exception as e:
            print(f"❌ Помилка читання CSV: {e}")
            return {}

    def update_existing_csv_with_contacts(self, csv_file=None):
        """Updates existing CSV file to extract contacts for profiles that don't have them yet"""
        if csv_file is None:
            csv_file = os.path.join(
                self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
            )

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
        """Closes browser and cleans up resources"""
        self.base_scraper.close()
