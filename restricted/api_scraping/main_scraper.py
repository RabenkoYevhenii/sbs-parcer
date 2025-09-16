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

    def __init__(self, headless=True):
        # Initialize core components
        self.base_scraper = BaseScraper(headless)
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

    def bulk_message_users_from_csv(
        self,
        csv_file: str,
        delay_seconds: int = 3,
        user_limit: int = None,
        enable_position_filter: bool = True,
    ):
        """Sends messages to all users from CSV file with excluded companies check"""
        print(f"\n📬 РОЗСИЛКА ПОВІДОМЛЕНЬ З ФАЙЛУ: {csv_file}")
        print(
            f"🚫 Виключено компаній: {len(self.company_filter.excluded_companies)}"
        )

        # Load existing chats
        print("📥 Завантажуємо існуючі чати...")
        self.messaging.load_chats_list(self.accounts)

        # Extract user data from CSV
        user_data = self.data_processor.extract_user_data_from_csv(
            csv_file,
            apply_filters=True,
            enable_position_filter=enable_position_filter,
        )

        if not user_data:
            print("❌ Немає користувачів для обробки")
            return

        # Apply user limit if specified
        if user_limit and user_limit > 0:
            print(f"🔢 Застосовуємо ліміт: {user_limit} користувачів")
            user_data = user_data[:user_limit]
            print(f"📋 Обробляємо {len(user_data)} користувачів")

        success_count = 0
        failed_count = 0
        skipped_count = 0
        excluded_count = 0

        for i, user_info in enumerate(user_data, 1):
            user_id = user_info["user_id"]
            first_name = user_info["first_name"]
            full_name = user_info["full_name"]
            company_name = user_info.get("company_name", "")

            print(f"\n📤 [{i}/{len(user_data)}] {full_name} ({company_name})")

            # Choose random message template
            random_message = random.choice(self.messaging.follow_up_messages)
            formatted_message = random_message.format(name=first_name)

            # Send message
            result = self.messaging.send_message_to_user(
                user_id,
                formatted_message,
                self.accounts,
                full_name,
                company_name,
            )

            if result == "success":
                success_count += 1
                print(f"       ✅ Успішно відправлено")
            elif result == "already_contacted":
                skipped_count += 1
                print(f"       ⏭️ Вже є контакт")
            elif result == "excluded_company":
                excluded_count += 1
                print(f"       🚫 Компанія виключена")
            else:
                failed_count += 1
                print(f"       ❌ Помилка відправки")

            # Delay between messages
            if i < len(user_data):
                print(f"       ⏳ Чекаємо {delay_seconds} секунд...")
                time.sleep(delay_seconds)

        print(f"\n📊 ПІДСУМОК РОЗСИЛКИ:")
        print(f"   ✅ Успішно: {success_count}")
        print(f"   ⏭️ Пропущено (чат існує): {skipped_count}")
        print(f"   🚫 Виключено (компанія): {excluded_count}")
        print(f"   ❌ Помилок: {failed_count}")
        print(
            f"   📈 Успішність: {(success_count/(success_count+failed_count)*100):.1f}%"
            if (success_count + failed_count) > 0
            else "N/A"
        )

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
        """Handles message sending using messenger accounts"""
        print("\n👥 SEND MESSAGES - ACCOUNT SELECTION")
        print("=" * 40)

        # Get list of all messenger accounts
        messenger_accounts = []
        for account_key, account_info in self.accounts.items():
            if account_info["role"] == "messaging":
                messenger_accounts.append(account_key)

        print("Available messenger accounts:")
        for i, account_key in enumerate(messenger_accounts, 1):
            account_info = self.accounts[account_key]
            print(f"{i}. {account_info['name']}")

        print(
            f"{len(messenger_accounts) + 1}. All messenger accounts (multi-account mode)"
        )

        choice = input(
            f"➡️ Choose account (1-{len(messenger_accounts) + 1}): "
        ).strip()

        try:
            choice_idx = int(choice) - 1
            if choice_idx == len(messenger_accounts):
                # Multi-account mode
                self.handle_multi_account_bulk_messaging()
            elif 0 <= choice_idx < len(messenger_accounts):
                # Single account mode
                selected_account = messenger_accounts[choice_idx]
                self.handle_single_account_messaging(selected_account)
            else:
                print("❌ Invalid choice")
        except ValueError:
            print("❌ Invalid input")

    def handle_single_account_messaging(self, account_key: str):
        """Handles messaging from a single account"""
        print(
            f"\n📤 SINGLE ACCOUNT MESSAGING - {self.accounts[account_key]['name']}"
        )
        print("=" * 60)

        # Switch to selected account
        if self.base_scraper.current_account != account_key:
            print(f"🔄 Switching to {account_key}...")
            if not self.switch_account(account_key):
                print("❌ Failed to switch account")
                return

        # Get CSV file
        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )

        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return

        # Get messaging parameters
        try:
            user_limit = input("User limit (empty for no limit): ").strip()
            user_limit = int(user_limit) if user_limit else None

            delay = input(
                "Delay between messages (default 3 seconds): "
            ).strip()
            delay_seconds = int(delay) if delay else 3

            filter_choice = (
                input("Enable position filter? (y/n, default y): ")
                .strip()
                .lower()
            )
            enable_position_filter = filter_choice != "n"

        except ValueError:
            print("❌ Invalid input")
            return

        # Execute bulk messaging
        self.bulk_message_users_from_csv(
            csv_file, delay_seconds, user_limit, enable_position_filter
        )

    def handle_multi_account_bulk_messaging(self):
        """Handles bulk messaging across multiple accounts"""
        print("\n📤 MULTI-ACCOUNT BULK MESSAGING")
        print("=" * 50)

        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )

        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return

        # Get parameters
        try:
            user_limit = input(
                "Total user limit (empty for no limit): "
            ).strip()
            user_limit = int(user_limit) if user_limit else None

            delay = input(
                "Delay between messages (default 3 seconds): "
            ).strip()
            delay_seconds = int(delay) if delay else 3

            filter_choice = (
                input("Enable position filter? (y/n, default y): ")
                .strip()
                .lower()
            )
            enable_position_filter = filter_choice != "n"

        except ValueError:
            print("❌ Invalid input")
            return

        # Execute multi-account messaging (implementation would go here)
        print("🚀 Starting multi-account messaging...")
        # This would implement the multi-account distribution logic

    def handle_followup_campaigns(self):
        """Handles follow-up campaigns"""
        print("\n📞 FOLLOW-UP CAMPAIGNS")
        print("=" * 40)
        print("1. 📅 Weekly follow-up campaign")
        print("2. 📅 Monthly follow-up campaign")
        print("3. 👤 Follow-up by specific author")
        print("4. 🔙 Back to main menu")

        choice = input("\nSelect option (1-4): ").strip()

        if choice == "1":
            csv_file = input("Enter CSV file path: ").strip()
            if os.path.exists(csv_file):
                self.process_followup_campaigns_optimized(csv_file, "weekly")
            else:
                print("❌ File not found")

        elif choice == "2":
            csv_file = input("Enter CSV file path: ").strip()
            if os.path.exists(csv_file):
                self.process_followup_campaigns_optimized(csv_file, "monthly")
            else:
                print("❌ File not found")

        elif choice == "3":
            csv_file = input("Enter CSV file path: ").strip()
            author_name = input("Enter author name: ").strip()
            if os.path.exists(csv_file) and author_name:
                self.process_followup_campaigns_by_author(
                    csv_file, author_name
                )
            else:
                print("❌ Invalid input")

        elif choice == "4":
            return
        else:
            print("❌ Invalid choice")

    def handle_conference_followup(self):
        """Handles conference follow-up for positive conversations"""
        print("\n🎯 CONFERENCE FOLLOWUP")
        print("=" * 40)

        default_path = "restricted/data/SBC - Attendees.csv"
        csv_file = input(
            f"Enter CSV file path (default: {default_path}): "
        ).strip()

        # Use default if empty input
        if not csv_file:
            csv_file = default_path

        if not os.path.exists(csv_file):
            print("❌ File not found")
            return

        confirm = input(
            "Process positive conversations for conference followup? (y/n): "
        ).lower()
        if confirm == "y":
            stats = self.messaging.process_positive_conversation_followups(
                csv_file, self.accounts
            )
            print(f"\n✅ Conference followup campaign completed!")
            print(f"📨 Sent: {stats.get('conference_followups_sent', 0)}")
        else:
            print("❌ Operation cancelled")

    def handle_check_responses(self):
        """Handles checking responses and updating CSV status"""
        print("\n📬 CHECK RESPONSES")
        print("=" * 40)

        csv_file = input("Enter CSV file path: ").strip()
        if not os.path.exists(csv_file):
            print("❌ File not found")
            return

        confirm = input("Check all responses and update CSV? (y/n): ").lower()
        if confirm == "y":
            stats = self.messaging.check_all_responses_and_update_csv(
                csv_file, self.accounts
            )
            print(f"\n✅ Response checking completed!")
            print(f"📬 Responses found: {stats.get('responses_found', 0)}")
            print(f"📝 CSV updated: {stats.get('csv_updated', 0)}")
        else:
            print("❌ Operation cancelled")

    def handle_update_csv_contacts(self):
        """Handles updating existing CSV with contacts"""
        print("\n📝 UPDATE CSV CONTACTS")
        print("=" * 40)
        print("Not yet implemented in refactored version")

    def handle_excluded_companies(self):
        """Handles excluded companies management"""
        print("\n🚫 MANAGE EXCLUDED COMPANIES")
        print("=" * 40)

        while True:
            print("\n📋 OPTIONS:")
            print("1. 📄 Show excluded companies list")
            print("2. 🔄 Reload excluded companies from file")
            print("3. 🧪 Test company exclusion")
            print("4. 🔙 Back to main menu")

            choice = input("➡️ Choose option (1-4): ").strip()

            if choice == "1":
                self.company_filter.show_excluded_companies()
            elif choice == "2":
                self.company_filter.reload_excluded_companies()
                print("✅ Excluded companies list reloaded")
            elif choice == "3":
                company_name = input("Enter company name to test: ").strip()
                if company_name:
                    self.company_filter.test_company_exclusion(company_name)
            elif choice == "4":
                break
            else:
                print("❌ Invalid choice. Please select 1-4.")

    def show_account_status(self):
        """Shows account status information"""
        print("\n📊 ACCOUNT STATUS")
        print("=" * 40)
        print(
            f"🔗 Browser connected: {'Yes' if self.base_scraper.page else 'No'}"
        )
        print(
            f"🔑 Logged in: {'Yes' if self.base_scraper.is_logged_in else 'No'}"
        )
        print(f"👤 Current account: {self.base_scraper.current_account}")

        if self.base_scraper.current_account:
            account_info = self.accounts[self.base_scraper.current_account]
            print(f"📧 Username: {account_info['username']}")
            print(f"🎭 Role: {account_info['role']}")
            print(f"🆔 User ID: {account_info['user_id']}")

        print(f"\n📁 Data directory: {self.base_scraper.get_data_dir()}")
        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
        print(
            f"📄 Main CSV exists: {'Yes' if os.path.exists(csv_file) else 'No'}"
        )

    def run_update(self):
        """Runs the attendee update process"""
        print("🚀 Starting attendee scraping process...")

        # Get all advanced search results
        print("📥 Получаем все результаты поиска...")
        search_results = self.base_scraper.get_all_advanced_search_results()

        if not search_results:
            print("❌ No search results obtained")
            return

        print(f"✅ Получено {len(search_results)} результатов поиска")

        # Load existing attendees
        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
        existing_keys = self.load_existing_attendees(csv_file)

        # Find new attendees
        new_attendees = self.find_new_attendees(search_results, existing_keys)

        if not new_attendees:
            print("✅ No new attendees found")
            return

        print(f"🆕 Found {len(new_attendees)} new attendees")

        # Process new attendees to get detailed info
        detailed_attendees = self.process_new_attendees(new_attendees)

        # Save new attendees
        self.save_new_attendees(detailed_attendees, csv_file)

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

    def close(self):
        """Closes browser and cleans up resources"""
        self.base_scraper.close()
