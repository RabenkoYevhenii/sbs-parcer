"""
Affiliate messaging script for sending targeted messages to affiliates and operators
"""

import csv
import os
import sys
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
from base_scraper import BaseScraper
from company_filter import CompanyFilter
from data_processor import DataProcessor
from messaging import MessagingHandler


class AffiliateMessageSender:
    """Handles affiliate messaging campaigns"""

    def __init__(self, headless=True):
        # Initialize core components
        self.base_scraper = BaseScraper(headless)
        self.company_filter = CompanyFilter(self.base_scraper.get_data_dir())
        self.data_processor = DataProcessor(self.base_scraper.get_data_dir())
        self.messaging = MessagingHandler(
            self.base_scraper, self.company_filter, self.data_processor
        )

        # Validate required environment variables
        self._validate_env_variables()

        # Affiliate account configuration
        self.accounts = {
            "affiliate": {
                "username": settings.affiliate_username,
                "password": settings.affiliate_password,
                "user_id": settings.affiliate_user_id,
                "name": f"Affiliate Account ({settings.affiliate_username})",
                "role": "affiliate_messaging",
            }
        }

        # Affiliate message template
        self.affiliate_message = (
            "Hello, I represent Flexify, namely our media buying team. "
            "We are currently working with Latin American and African countries "
            "and will soon begin testing in Europe. We are seeking new and "
            "interesting partners, so if you are interested, I would be happy "
            "to meet and discuss possible cooperation."
        )

    def _validate_env_variables(self):
        """Validates the presence of required environment variables for affiliate account"""
        required_vars = [
            ("AFFILIATE_USERNAME", settings.affiliate_username),
            ("AFFILIATE_PASSWORD", settings.affiliate_password),
            ("AFFILIATE_USER_ID", settings.affiliate_user_id),
        ]

        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)

        if missing_vars:
            print(f"❌ Missing required environment variables:")
            for var in missing_vars:
                print(f"   - {var}")
            print(f"\nSet them in .env file or as system variables")
            exit(1)

    def start(self):
        """Starts the browser and logs in to affiliate account"""
        self.base_scraper.start()
        return self.base_scraper.login("affiliate", self.accounts)

    def filter_affiliate_targets(self, csv_file: str) -> List[Dict]:
        """Filters CSV data for affiliate targets based on specified criteria"""
        print(f"\n🎯 FILTERING AFFILIATE TARGETS FROM: {csv_file}")
        print("=" * 50)

        targets = []
        
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return targets

        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    # Extract relevant fields
                    organization_type = row.get('organization_type', '').lower()
                    position = row.get('position', '').lower()
                    source_url = row.get('source_url', '')
                    full_name = row.get('full_name', '')
                    company_name = row.get('company_name', '')
                    
                    # Check if already processed (connected = 'Sent' or author = 'affiliate')
                    connected_status = row.get('connected', '').lower()
                    author = row.get('author', '').lower()
                    valid_status = str(row.get('valid', '')).lower()
                    
                    # Skip if already processed by affiliate system
                    if (connected_status in ['sent', 'true'] and author == 'affiliate') or author == 'affiliate':
                        print(f"   ⭐ Skipped (already processed): {full_name} - {company_name}")
                        continue
                    
                    # Skip if marked as invalid (excluded company)
                    if valid_status == 'false':
                        print(f"   🚫 Skipped (invalid/excluded): {full_name} - {company_name}")
                        continue

                    # Apply filters
                    # 1. Organization type filter: Affiliate OR Operator - Casino
                    org_type_match = (
                        'affiliate' in organization_type or 
                        ('operator' in organization_type and 'casino' in organization_type)
                    )

                    if not org_type_match:
                        continue

                    # 2. Position filter: exclude anybody who has "payment" word
                    if 'payment' in position:
                        print(f"   🚫 Excluded (payment role): {full_name} - {position}")
                        continue

                    # Extract user ID from source URL
                    user_id = self.data_processor.extract_user_id_from_url(source_url)
                    if not user_id:
                        continue

                    # Create target record
                    target = {
                        'user_id': user_id,
                        'full_name': full_name,
                        'company_name': company_name,
                        'position': position,
                        'organization_type': organization_type,
                        'source_url': source_url,
                        'first_name': full_name.split()[0] if full_name.split() else ''
                    }

                    targets.append(target)

        except Exception as e:
            print(f"❌ Error reading CSV file: {e}")
            return []

        print(f"✅ Filtered targets: {len(targets)} contacts (after removing already processed and excluded)")
        
        # Show organization type breakdown
        org_types = {}
        for target in targets:
            org_type = target['organization_type']
            org_types[org_type] = org_types.get(org_type, 0) + 1

        print(f"\n📊 Organization type breakdown:")
        for org_type, count in org_types.items():
            print(f"   • {org_type}: {count}")

        return targets

    def update_csv_affiliate_status(
        self,
        csv_file: str,
        user_id: str,
        full_name: str,
        status: str = "Sent",
        chat_id: str = "",
        valid: str = ""
    ):
        """Update CSV with affiliate messaging status"""
        try:
            import pandas as pd
            from datetime import datetime
            
            # Load CSV
            df = pd.read_csv(csv_file)
            
            # Find the user row by matching user_id in source_url or by full_name
            user_row_idx = None
            
            # First try to match by user_id in source_url
            for idx, row in df.iterrows():
                source_url = str(row.get('source_url', ''))
                if user_id in source_url:
                    user_row_idx = idx
                    break
            
            # If not found by user_id, try by full_name
            if user_row_idx is None:
                for idx, row in df.iterrows():
                    if str(row.get('full_name', '')).strip() == full_name.strip():
                        user_row_idx = idx
                        break
            
            if user_row_idx is not None:
                # Update the row with proper data type handling
                if status:  # Only update if status is provided
                    df.at[user_row_idx, 'connected'] = status
                    df.at[user_row_idx, 'author'] = 'affiliate'
                    
                    # Convert Date column to string type to avoid dtype warning
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    df['Date'] = df['Date'].astype('object')  # Convert to object type first
                    df.at[user_row_idx, 'Date'] = current_date
                
                if chat_id:
                    df.at[user_row_idx, 'chat_id'] = chat_id
                
                # Update valid column for excluded companies
                if valid:
                    df['valid'] = df['valid'].astype('object')  # Convert to object type first
                    df.at[user_row_idx, 'valid'] = valid
                
                # Save back to CSV
                df.to_csv(csv_file, index=False, encoding='utf-8')
                return True
            else:
                print(f"       ⚠️ User not found in CSV for status update")
                return False
                
        except Exception as e:
            print(f"       ⚠️ Error updating CSV: {e}")
            return False

    def send_single_affiliate_message(
        self,
        target_user_id: str,
        message: str,
        full_name: str = None,
        company_name: str = None,
    ) -> str:
        """Send a single affiliate message (not using main messaging system's double-message logic)"""
        
        csv_file = os.path.join("restricted", "data", "SBC for Aff.csv")
        
        # 0. Check if company is in exclusion list
        if company_name:
            details = self.company_filter.get_exclusion_details(company_name)
            if details["is_excluded"]:
                print(f"       🚫 Company excluded: '{company_name}'")
                print(f"       📋 Matched: '{details['matched_company']}' (similarity: {details['similarity_score']:.2f})")
                # Update CSV to mark as invalid/excluded
                print(f"       📝 Updating CSV to mark company as excluded...")
                self.update_csv_affiliate_status(csv_file, target_user_id, full_name, status="", valid="False")
                return "excluded_company"

        # 1. Check if there's an existing chat
        chat_id = self.messaging.find_chat_with_user(target_user_id)

        if chat_id:
            # 1.1. If chat exists, check if it has messages
            print(f"       🔍 Checking existing chat for messages...")
            if self.messaging.check_chat_has_messages(chat_id):
                print(f"       ⭐ Chat already has messages, skipping")
                # Update CSV to mark as already processed
                print(f"       📝 Updating CSV to mark as already contacted...")
                self.update_csv_affiliate_status(csv_file, target_user_id, full_name, "Sent", chat_id)
                return "already_contacted"
            else:
                print(f"       ✅ Chat is empty, can send message")
        else:
            # 2. Create new chat
            print(f"       🆕 Creating new chat...")
            chat_id = self.messaging.create_chat(target_user_id, self.accounts)
            if not chat_id:
                return "failed"

        # 3. Send ONLY the single affiliate message
        if not self.messaging.send_message(chat_id, message):
            return "failed"

        print(f"       ✅ Affiliate message sent successfully")

        # 4. Update CSV file about sent message
        print(f"       📝 Updating CSV file...")
        self.update_csv_affiliate_status(csv_file, target_user_id, full_name, "Sent", chat_id)

        return "success"

    def send_affiliate_messages(
        self,
        csv_file: str,
        user_limit: int = None
    ):
        """Sends affiliate messages to filtered targets"""
        print(f"\n📬 AFFILIATE MESSAGE CAMPAIGN")
        print(f"📄 CSV file: {csv_file}")
        print("=" * 50)

        # Load existing chats
        print("📥 Loading existing chats...")
        self.messaging.load_chats_list(self.accounts)

        # Filter targets
        targets = self.filter_affiliate_targets(csv_file)
        
        if not targets:
            print("❌ No targets found after filtering")
            return

        # Apply user limit if specified
        if user_limit and user_limit > 0:
            print(f"🔢 Applying limit: {user_limit} contacts")
            targets = targets[:user_limit]
            print(f"📋 Processing {len(targets)} contacts")

        # Initialize counters
        success_count = 0
        failed_count = 0
        skipped_count = 0
        excluded_count = 0

        print(f"\n📨 MESSAGE TEMPLATE:")
        print(f"   → {self.affiliate_message}")

        print(f"\n🚀 Starting affiliate message campaign...")
        print(f"⏱️ Random delays: 1-10 seconds between messages (human-like behavior)")
        print(f"📧 NOTE: Sending ONLY ONE message per contact (not using main system's double-message)")

        for i, target in enumerate(targets, 1):
            user_id = target["user_id"]
            first_name = target["first_name"]
            full_name = target["full_name"]
            company_name = target.get("company_name", "")
            position = target.get("position", "")
            organization_type = target.get("organization_type", "")

            print(f"\n📤 [{i}/{len(targets)}] {full_name}")
            print(f"     🏢 Company: {company_name}")
            print(f"     💼 Position: {position}")
            print(f"     🎯 Org Type: {organization_type}")

            # Use our single-message method instead of main messaging system
            result = self.send_single_affiliate_message(
                user_id,
                self.affiliate_message,
                full_name,
                company_name,
            )

            if result == "success":
                success_count += 1
                print(f"       ✅ Successfully sent")
            elif result == "already_contacted":
                skipped_count += 1
                print(f"       ⭐ Already contacted")
            elif result == "excluded_company":
                excluded_count += 1
                print(f"       🚫 Company excluded")
            else:
                failed_count += 1
                print(f"       ❌ Failed to send")

            # Random delay between messages (1-10 seconds) for human-like behavior
            if i < len(targets):
                import random
                random_delay = random.randint(1, 10)
                print(f"       ⏳ Waiting {random_delay} seconds (random delay for human-like behavior)...")
                time.sleep(random_delay)

        # Print campaign summary
        print(f"\n📊 AFFILIATE CAMPAIGN SUMMARY:")
        print("=" * 50)
        print(f"   ✅ Successful: {success_count}")
        print(f"   ⭐ Skipped (existing chat): {skipped_count}")
        print(f"   🚫 Excluded (company): {excluded_count}")
        print(f"   ❌ Failed: {failed_count}")
        total_processed = success_count + failed_count
        if total_processed > 0:
            success_rate = (success_count / total_processed) * 100
            print(f"   📈 Success rate: {success_rate:.1f}%")
        print(f"   📧 Total messages sent: {success_count}")

    def show_affiliate_menu(self):
        """Shows affiliate-specific menu"""
        while True:
            print("\n" + "=" * 60)
            print("🎯 AFFILIATE MESSAGE SENDER")
            print("=" * 60)
            print(f"👤 Current account: {self.accounts['affiliate']['name']}")
            print("-" * 60)
            print("1. 📨 Send affiliate messages")
            print("2. 📊 Preview targets (dry run)")
            print("3. 🚪 Exit")
            print("=" * 60)

            choice = input("➡️ Choose an action (1-3): ").strip()

            if choice == "1":
                self.handle_send_messages()
            elif choice == "2":
                self.handle_preview_targets()
            elif choice == "3":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please select 1-3.")

    def handle_send_messages(self):
        """Handles the message sending process"""
        print("\n📨 AFFILIATE MESSAGE SENDING")
        print("=" * 40)

        csv_file = os.path.join("restricted", "data", "SBC for Aff.csv")
        
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            print("Please ensure the file exists at the specified location.")
            return

        print(f"📄 Using CSV file: {csv_file}")

        # Get parameters from user
        try:
            limit_input = input("User limit (press Enter for no limit): ").strip()
            user_limit = int(limit_input) if limit_input else None

        except ValueError:
            print("❌ Invalid input. Using defaults.")
            user_limit = None

        # Show preview
        targets = self.filter_affiliate_targets(csv_file)
        actual_targets = user_limit if user_limit and user_limit < len(targets) else len(targets)

        print(f"\n📋 CAMPAIGN PREVIEW:")
        print(f"   📊 Total targets found: {len(targets)}")
        print(f"   🎯 Will process: {actual_targets}")
        print(f"   ⏱️ Random delay: 1-10 seconds between messages (human-like behavior)")
        print(f"   📨 Message: {self.affiliate_message[:80]}...")

        # Confirm sending
        confirm = input(f"\n🤔 Send affiliate messages to {actual_targets} targets? (y/n): ").lower()
        
        if confirm == "y":
            self.send_affiliate_messages(csv_file, user_limit=user_limit)
        else:
            print("❌ Campaign cancelled")

    def handle_preview_targets(self):
        """Handles target preview (dry run)"""
        print("\n👀 PREVIEW TARGETS (DRY RUN)")
        print("=" * 40)

        csv_file = os.path.join("restricted", "data", "SBC for Aff.csv")
        
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return

        targets = self.filter_affiliate_targets(csv_file)
        
        if not targets:
            print("❌ No targets found")
            return

        print(f"\n📋 PREVIEW RESULTS ({len(targets)} targets):")
        print("-" * 60)

        for i, target in enumerate(targets[:10], 1):  # Show first 10
            print(f"{i:2d}. {target['full_name']}")
            print(f"     🏢 {target['company_name']}")
            print(f"     💼 {target['position']}")
            print(f"     🎯 {target['organization_type']}")
            print()

        if len(targets) > 10:
            print(f"... and {len(targets) - 10} more targets")

        input("\nPress Enter to continue...")

    def close(self):
        """Closes browser and cleans up resources"""
        self.base_scraper.close()


def main():
    """Main entry point for affiliate message sender"""
    try:
        sender = AffiliateMessageSender()
        
        print("🚀 Starting affiliate message sender...")
        
        if sender.start():
            print("✅ Successfully logged in to affiliate account")
            sender.show_affiliate_menu()
        else:
            print("❌ Failed to login to affiliate account")
            
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            sender.close()
        except:
            pass


if __name__ == "__main__":
    main()