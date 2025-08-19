"""Main script for SBC website scraper."""

import asyncio
import argparse
import logging
import os
import sys
import warnings
from typing import Dict, Any

from config import settings
from tools import scrape_attendees

# Suppress the specific asyncio subprocess transport warning
warnings.filterwarnings(
    "ignore", message="Exception ignored in.*BaseSubprocessTransport.__del__"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sbc_scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class SBCScraperMain:
    """Main class for SBC scraper application."""

    def __init__(self):
        self.supported_pages = {
            "attendees": {
                "url": settings.sbc_attendees_url,
                "csv_path": settings.attendees_csv_path,
                "scraper_func": scrape_attendees,
                "description": "Scrape attendees with profile information",
            },
            "companies": {
                "url": settings.sbc_companies_url,
                "csv_path": settings.companies_csv_path,
                "scraper_func": None,  # To be implemented
                "description": "Scrape companies information (not implemented yet)",
            },
            "exhibitors": {
                "url": settings.sbc_exhibitors_url,
                "csv_path": settings.exhibitors_csv_path,
                "scraper_func": None,  # To be implemented
                "description": "Scrape exhibitors information (not implemented yet)",
            },
        }

    def validate_credentials(self) -> bool:
        """Validate that required credentials are provided."""
        if (
            not settings.sbc_username
            or settings.sbc_username == "your_username_here"
        ):
            logger.error("Please set SBC_USERNAME in .env file")
            return False

        if (
            not settings.sbc_password
            or settings.sbc_password == "your_password_here"
        ):
            logger.error("Please set SBC_PASSWORD in .env file")
            return False

        return True

    def print_supported_pages(self) -> None:
        """Print information about supported pages."""
        print("\nSupported pages for scraping:")
        print("-" * 50)
        for page_name, page_info in self.supported_pages.items():
            status = (
                "✓ Available"
                if page_info["scraper_func"]
                else "✗ Not implemented"
            )
            print(f"{page_name:<12} - {page_info['description']} [{status}]")
        print("-" * 50)

    async def scrape_page(self, page_name: str) -> int:
        """Scrape a specific page."""
        if page_name not in self.supported_pages:
            logger.error(f"Unsupported page: {page_name}")
            self.print_supported_pages()
            return 0

        page_info = self.supported_pages[page_name]

        if not page_info["scraper_func"]:
            logger.error(f"Scraper for '{page_name}' is not implemented yet")
            return 0

        logger.info(f"Starting to scrape {page_name} page")
        logger.info(f"URL: {page_info['url']}")
        logger.info(f"Output CSV: {page_info['csv_path']}")

        try:
            # Call the appropriate scraper function
            total_scraped = await page_info["scraper_func"](
                page_info["url"], page_info["csv_path"]
            )

            logger.info(f"Scraping completed successfully!")
            logger.info(f"Total new records scraped: {total_scraped}")

            return total_scraped

        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            return 0

    def run_interactive_mode(self) -> None:
        """Run scraper in interactive mode."""
        print("\n🎯 SBC Website Scraper")
        print("=" * 40)

        if not self.validate_credentials():
            print("\n❌ Please configure your credentials in .env file first!")
            return

        self.print_supported_pages()

        while True:
            try:
                print("\nOptions:")
                print("1. Scrape a specific page")
                print("2. View supported pages")
                print("3. Exit")

                choice = input("\nEnter your choice (1-3): ").strip()

                if choice == "1":
                    page_name = (
                        input("\nEnter page name (e.g., 'attendees'): ")
                        .strip()
                        .lower()
                    )
                    if page_name:
                        print(f"\n🚀 Starting to scrape {page_name} page...")
                        print(
                            "📜 Note: This page uses infinite scroll - the scraper will"
                        )
                        print(
                            "   automatically scroll and load all available data."
                        )
                        print(
                            "   This may take several minutes depending on the amount of data."
                        )

                        result = asyncio.run(self.scrape_page(page_name))
                        if result > 0:
                            print(
                                f"\n✅ Successfully scraped {result} new records!"
                            )
                        else:
                            print(
                                "\n❌ Scraping failed or no new records found."
                            )

                elif choice == "2":
                    self.print_supported_pages()

                elif choice == "3":
                    print("\n👋 Goodbye!")
                    break

                else:
                    print("❌ Invalid choice. Please enter 1, 2, or 3.")

            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                logger.error(f"Error in interactive mode: {str(e)}")
                print(f"❌ Error: {str(e)}")


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description="SBC Website Scraper - Extract data from SBC events website",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --page attendees                    # Scrape attendees page
  python main.py --interactive                       # Run in interactive mode
  python main.py --list-pages                        # Show supported pages
        """,
    )

    parser.add_argument(
        "--page",
        type=str,
        help="Page to scrape (attendees, companies, exhibitors)",
    )

    parser.add_argument(
        "--interactive", action="store_true", help="Run in interactive mode"
    )

    parser.add_argument(
        "--list-pages", action="store_true", help="List all supported pages"
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Custom output CSV file path (overrides default)",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        default=None,
        help="Run browser in headless mode",
    )

    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser with GUI (for debugging)",
    )

    return parser


async def main() -> None:
    """Main function."""
    parser = create_argument_parser()
    args = parser.parse_args()

    scraper_app = SBCScraperMain()

    # Override headless setting if specified
    if args.headless:
        settings.headless = True
    elif args.no_headless:
        settings.headless = False

    # Handle different modes
    if args.list_pages:
        scraper_app.print_supported_pages()
        return

    if args.interactive:
        scraper_app.run_interactive_mode()
        return

    if args.page:
        if not scraper_app.validate_credentials():
            logger.error(
                "Please configure your credentials in .env file first!"
            )
            return

        # Override output path if specified
        if args.output:
            if args.page in scraper_app.supported_pages:
                scraper_app.supported_pages[args.page][
                    "csv_path"
                ] = args.output

        result = await scraper_app.scrape_page(args.page)

        if result > 0:
            print(f"\n✅ Successfully scraped {result} new records!")
        else:
            print("\n❌ Scraping failed or no new records found.")
        return

    # If no specific arguments, run interactive mode
    scraper_app.run_interactive_mode()


if __name__ == "__main__":
    try:
        # Set up asyncio policy for better compatibility
        if sys.platform.startswith("linux"):
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

        # Run the main function
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user. Goodbye!")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"❌ Fatal error: {str(e)}")
        sys.exit(1)
    finally:
        # Simple cleanup
        logger.info("Application shutdown completed")
