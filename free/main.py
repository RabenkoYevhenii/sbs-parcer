#!/usr/bin/env python3
"""
Universal Company Parser
A Playwright-based web scraper for extracting company information from various websites.

Usage:
    python main.py <website_url> [options]

Example:
    python main.py "https://example.com/companies" --max-pages 5 --delay 1.5
"""

import argparse
import asyncio
import sys
import logging
from urllib.parse import urlparse

from tools import parse_website

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def validate_url(url: str) -> bool:
    """Validate if the provided URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Universal Company Parser - Extract company data from websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py "https://example.com/companies"
  python main.py "https://directory.com/businesses" --max-pages 10 --delay 2.0
  python main.py "https://listings.com" --max-pages 5 --delay 1.5 --visible
        """,
    )

    parser.add_argument(
        "website_url",
        help="URL of the website to parse for company information",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum number of pages to process (default: 10)",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between requests in seconds (default: 1.0)",
    )

    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (not headless)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate URL
    if not validate_url(args.website_url):
        logger.error(f"Invalid URL provided: {args.website_url}")
        sys.exit(1)

    # Validate arguments
    if args.max_pages <= 0:
        logger.error("max_pages must be greater than 0")
        sys.exit(1)

    if args.delay < 0:
        logger.error("delay must be non-negative")
        sys.exit(1)

    return asyncio.run(async_main(args))


async def async_main(args):
    """Async main function"""
    logger.info("=" * 60)
    logger.info("Universal Company Parser Started")
    logger.info("=" * 60)
    logger.info(f"Target URL: {args.website_url}")
    logger.info(f"Max pages: {args.max_pages}")
    logger.info(f"Delay: {args.delay}s")
    logger.info(f"Headless: {not args.visible}")
    logger.info("=" * 60)

    try:
        # Start parsing
        companies_found = await parse_website(
            website_url=args.website_url,
            max_pages=args.max_pages,
            delay=args.delay,
            headless=not args.visible,
        )

        logger.info("=" * 60)
        logger.info("Parsing Completed Successfully!")
        logger.info(f"Total companies found: {companies_found}")

        # Generate output filename
        domain = urlparse(args.website_url).netloc
        csv_filename = f"{domain.replace('.', '_')}_companies.csv"
        logger.info(f"Results saved to: {csv_filename}")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.info("\nParsing interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error during parsing: {e}")
        if args.verbose:
            import traceback

            logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
