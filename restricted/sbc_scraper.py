#!/usr/bin/env python3
"""
New entry point for the refactored SBC Attendees Scraper

This file replaces the original api_test.py with a cleaner interface
using the new modular structure.
"""

import sys
import os

# Add the restricted directory to Python path so we can import api_scraping
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api_scraping.main_scraper import SBCAttendeesScraper
from config import settings


def main():
    """Main entry point for the application"""
    print("🚀 Starting SBC Attendees Scraper (Refactored Version)")
    print("=" * 60)

    proxy_config = settings.get_proxy_config()
    scraper = SBCAttendeesScraper(headless=False, proxy_config=proxy_config)

    try:
        # Start browser and login
        if scraper.start():
            print("✅ Successfully started and logged in")
            # Show main menu
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
        # Clean up
        scraper.close()
        print("🔒 Application closed")


if __name__ == "__main__":
    main()
