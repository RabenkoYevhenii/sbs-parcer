#!/usr/bin/env python3
"""
COMPATIBILITY FILE - ORIGINAL api_test.py HAS BEEN REFACTORED

This file has been refactored into multiple modules for better organization:

NEW STRUCTURE:
- api_scraping/base_scraper.py - Browser automation and API requests
- api_scraping/company_filter.py - Company exclusion logic
- api_scraping/data_processor.py - CSV handling and data processing
- api_scraping/messaging.py - Chat and messaging functionality
- api_scraping/main_scraper.py - Main orchestrator class
- sbc_scraper.py - New entry point

USAGE:
Instead of running this file, use the new entry point:
    python sbc_scraper.py

Or import the refactored components:
    from api_scraping import SBCAttendeesScraper
    scraper = SBCAttendeesScraper()
"""

import sys
import os


def main():
    print("⚠️  DEPRECATED: This file has been refactored!")
    print("=" * 50)
    print()
    print("The original api_test.py has been split into multiple modules")
    print("for better organization and maintainability.")
    print()
    print("NEW ENTRY POINT:")
    print("    python sbc_scraper.py")
    print()
    print("Or use the new modular components:")
    print("    from api_scraping import SBCAttendeesScraper")
    print()

    choice = input("Would you like to run the new version? (y/n): ").lower()

    if choice == "y":
        print("\n🚀 Launching new version...")
        try:
            # Import and run the new version
            from api_scraping import SBCAttendeesScraper

            scraper = SBCAttendeesScraper(headless=False)

            try:
                if scraper.start():
                    print("✅ Successfully started and logged in")
                    scraper.show_main_menu()
                else:
                    print("❌ Failed to start or login")

            except KeyboardInterrupt:
                print("\n⚠️ Interrupted by user")
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                import traceback

                traceback.print_exc()
            finally:
                scraper.close()
                print("🔒 Application closed")

        except ImportError as e:
            print(f"❌ Import error: {e}")
            print("Make sure all new modules are in place.")
    else:
        print("👋 Goodbye!")


if __name__ == "__main__":
    main()
