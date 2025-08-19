import csv
import os
import asyncio
import logging
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse, urljoin
from playwright.async_api import (
    async_playwright,
    Page,
    Browser,
    BrowserContext,
)

from helpers import (
    CompanySelectors,
    SBCSelectors,
    clean_text,
    clean_company_name,
    extract_phone,
    extract_email,
    extract_url,
    get_element_text_or_attribute_async,
    try_selectors_async,
    find_company_containers_async,
    extract_social_links_async,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UniversalCompanyParser:
    """Universal company parser using Playwright"""

    def __init__(
        self, website_url: str, delay: float = 1.0, headless: bool = True
    ):
        self.website_url = website_url
        self.delay = delay
        self.headless = headless
        self.domain = urlparse(website_url).netloc
        self.csv_filename = f"{self.domain.replace('.', '_')}_companies.csv"
        self.company_index = 0
        self.existing_companies: Set[str] = set()

        # CSV fieldnames
        self.fieldnames = [
            "company_index",
            "name",
            "description",
            "company_category",
            "phone",
            "email",
            "logo_url",
            "facebook",
            "instagram",
            "linkedin",
            "twitter",
            "other_socials",
        ]

        # Initialize CSV file
        self._init_csv_file()

    def _init_csv_file(self):
        """Initialize CSV file and load existing companies"""
        if os.path.exists(self.csv_filename):
            # Load existing companies to avoid duplicates
            with open(
                self.csv_filename, "r", encoding="utf-8", newline=""
            ) as file:
                reader = csv.DictReader(file)
                for row in reader:
                    company_key = self._generate_company_key(
                        row.get("name", "")
                    )
                    if company_key:
                        self.existing_companies.add(company_key)
                    # Update company index
                    try:
                        self.company_index = max(
                            self.company_index,
                            int(row.get("company_index", 0)),
                        )
                    except (ValueError, TypeError):
                        pass
        else:
            # Create new CSV file with headers
            with open(
                self.csv_filename, "w", encoding="utf-8", newline=""
            ) as file:
                writer = csv.DictWriter(file, fieldnames=self.fieldnames)
                writer.writeheader()

        logger.info(
            f"CSV file: {self.csv_filename}, Existing companies: {len(self.existing_companies)}"
        )

    def _generate_company_key(self, name: str) -> str:
        """Generate unique key for company identification"""
        name_clean = clean_text(name).lower()

        if name_clean:
            return f"{name_clean}"
        return ""

    def _is_company_duplicate(self, company_data: Dict[str, str]) -> bool:
        """Check if company already exists"""
        company_key = self._generate_company_key(company_data.get("name", ""))

        if not company_key:
            logging.debug(
                f"Empty company key generated for: {company_data.get('name', 'Unknown')}"
            )
            return False

        is_duplicate = company_key in self.existing_companies

        if is_duplicate:
            logging.debug(
                f"Duplicate detected - Key: '{company_key}' already exists"
            )
        else:
            logging.debug(f"New company - Key: '{company_key}'")

        return is_duplicate

    def _save_company(self, company_data: Dict[str, str]):
        """Save company data to CSV immediately"""
        if self._is_company_duplicate(company_data):
            logger.info(
                f"Skipping duplicate company: {company_data.get('name', 'Unknown')}"
            )
            return False

        # Increment company index
        self.company_index += 1
        company_data["company_index"] = str(self.company_index)

        # Add to existing companies set
        company_key = self._generate_company_key(company_data.get("name", ""))
        if company_key:
            self.existing_companies.add(company_key)

        # Save to CSV
        try:
            with open(
                self.csv_filename, "a", encoding="utf-8", newline=""
            ) as file:
                writer = csv.DictWriter(file, fieldnames=self.fieldnames)
                writer.writerow(company_data)

            logger.info(
                f"Saved company #{self.company_index}: {company_data.get('name', 'Unknown')}"
            )
            return True
        except Exception as e:
            logger.error(f"Error saving company data: {e}")
            return False

    async def _extract_company_data(
        self, container, page: Page, source_url: str
    ) -> Dict[str, str]:
        """Extract all company data from a container element"""
        company_data = {
            field: "" for field in self.fieldnames if field != "company_index"
        }

        # Extract name with improved filtering
        name_elements = await try_selectors_async(
            page, CompanySelectors.NAME_SELECTORS, container
        )
        if name_elements:
            raw_name = clean_text(
                await get_element_text_or_attribute_async(name_elements[0])
            )
            cleaned_name = clean_company_name(raw_name)

            if cleaned_name:
                company_data["name"] = cleaned_name
                logging.info(
                    f"[ITEM {self.company_index + 1}] Found valid company name: '{cleaned_name}' (from: '{raw_name}')"
                )
            else:
                logging.warning(
                    f"[ITEM {self.company_index + 1}] Filtered out company name: '{raw_name}' (likely stand number)"
                )
                company_data["name"] = ""

        # Extract description
        desc_elements = await try_selectors_async(
            page, CompanySelectors.DESCRIPTION_SELECTORS, container
        )
        if desc_elements:
            company_data["description"] = clean_text(
                await get_element_text_or_attribute_async(desc_elements[0])
            )

        # Extract company category
        category_elements = await try_selectors_async(
            page, SBCSelectors.CATEGORY_SELECTORS, container
        )
        if category_elements:
            category_text = clean_text(
                await get_element_text_or_attribute_async(category_elements[0])
            )
            # Clean up category text (remove # if present)
            if category_text.startswith("#"):
                category_text = category_text[1:].strip()
            company_data["company_category"] = category_text

        # Extract phone
        phone_elements = await try_selectors_async(
            page, CompanySelectors.PHONE_SELECTORS, container
        )
        if phone_elements:
            phone_text = await get_element_text_or_attribute_async(
                phone_elements[0]
            )
            phone_href = await get_element_text_or_attribute_async(
                phone_elements[0], "href"
            )
            if phone_href and phone_href.startswith("tel:"):
                company_data["phone"] = extract_phone(
                    phone_href.replace("tel:", "")
                )
            else:
                company_data["phone"] = extract_phone(phone_text)

        # Extract email
        email_elements = await try_selectors_async(
            page, CompanySelectors.EMAIL_SELECTORS, container
        )
        if email_elements:
            email_text = await get_element_text_or_attribute_async(
                email_elements[0]
            )
            email_href = await get_element_text_or_attribute_async(
                email_elements[0], "href"
            )
            if email_href and email_href.startswith("mailto:"):
                company_data["email"] = extract_email(
                    email_href.replace("mailto:", "")
                )
            else:
                company_data["email"] = extract_email(email_text)

        # Extract logo
        logo_elements = await try_selectors_async(
            page, CompanySelectors.LOGO_SELECTORS, container
        )
        if logo_elements:
            logo_src = await get_element_text_or_attribute_async(
                logo_elements[0], "src"
            )
            if logo_src:
                # Convert relative URLs to absolute
                if logo_src.startswith("//"):
                    logo_src = "https:" + logo_src
                elif logo_src.startswith("/"):
                    logo_src = urljoin(self.website_url, logo_src)
                company_data["logo_url"] = logo_src

        # Extract social media links
        social_data = await extract_social_links_async(container, page)
        company_data.update(social_data)

        return company_data

    async def _click_load_more(self, page: Page) -> bool:
        """Try to click load more/show more buttons and wait for new content"""
        # Get current container count before clicking
        current_containers = await find_company_containers_async(page)
        current_count = len(current_containers) if current_containers else 0

        for selector in CompanySelectors.PAGINATION_SELECTORS:
            try:
                elements = await page.locator(selector).all()
                for element in elements:
                    if (
                        await element.is_visible()
                        and await element.is_enabled()
                    ):
                        logger.info(f"Clicking load more button: {selector}")
                        logger.info(
                            f"Current container count: {current_count}"
                        )

                        await element.click()

                        # Wait for new content to load with longer delay
                        await asyncio.sleep(self.delay * 3)

                        # Check if new content was loaded by comparing container count
                        max_wait_time = 15  # seconds - increased wait time
                        wait_time = 0

                        while wait_time < max_wait_time:
                            new_containers = (
                                await find_company_containers_async(page)
                            )
                            new_count = (
                                len(new_containers) if new_containers else 0
                            )

                            if new_count > current_count:
                                logger.info(
                                    f"New content loaded! Container count increased from {current_count} to {new_count}"
                                )
                                # Add extra delay to ensure content is fully loaded
                                await asyncio.sleep(self.delay)
                                return True

                            await asyncio.sleep(
                                1.0
                            )  # Increased check interval
                            wait_time += 1.0

                        logger.warning(
                            f"Load more button clicked but no new content detected after {max_wait_time}s"
                        )
                        return False

            except Exception as e:
                logger.debug(f"Could not click selector {selector}: {e}")
                continue

        return False

    async def _wait_for_page_load(self, page: Page):
        """Wait for page to fully load"""
        try:
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass
        await asyncio.sleep(self.delay)

    async def parse_companies(self, max_pages: int = 10) -> int:
        """Main parsing method"""
        companies_found = 0

        async with async_playwright() as p:
            # Launch browser
            browser: Browser = await p.chromium.launch(headless=self.headless)
            context: BrowserContext = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page: Page = await context.new_page()

            try:
                logger.info(f"Starting to parse: {self.website_url}")

                # Navigate to the website
                await page.goto(
                    self.website_url,
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await self._wait_for_page_load(page)

                pages_processed = 0
                total_containers_processed = 0

                while pages_processed < max_pages:
                    logger.info(f"Processing page {pages_processed + 1}")

                    # Find company containers
                    containers = await find_company_containers_async(page)

                    if not containers:
                        logger.warning(
                            "No company containers found on this page"
                        )
                        break

                    current_container_count = len(containers)
                    logger.info(
                        f"Found {current_container_count} total company containers"
                    )

                    # Process containers starting from where we left off
                    start_index = total_containers_processed
                    end_index = current_container_count

                    logger.info(
                        f"DEBUG: start_index={start_index}, end_index={end_index}, total_containers_processed={total_containers_processed}"
                    )

                    if start_index >= end_index:
                        logger.info("No new containers to process")
                        # Try to load more content before breaking
                        if not await self._click_load_more(page):
                            logger.info(
                                "No more content to load or load more button not found"
                            )
                            break
                        else:
                            # Successfully loaded more content, continue to next iteration to refresh containers
                            logger.info(
                                "Successfully loaded more content, refreshing container list..."
                            )
                            pages_processed += 1
                            continue

                    logging.info(
                        f"Processing containers {start_index} to {end_index - 1} on page {pages_processed + 1}"
                    )

                    # Process each new container
                    for i in range(start_index, end_index):
                        container = containers[i]
                        try:
                            logging.info(
                                f"[ITEM {self.company_index + 1}] Processing container {i + 1}/{current_container_count}"
                            )

                            company_data = await self._extract_company_data(
                                container, page, page.url
                            )

                            # Only save if we have at least a name
                            if company_data.get("name"):
                                if self._save_company(company_data):
                                    companies_found += 1
                                    logging.info(
                                        f"[ITEM {self.company_index}] Saved company: {company_data['name']}"
                                    )
                                else:
                                    logging.info(
                                        f"[ITEM {self.company_index + 1}] Skipped duplicate: {company_data['name']}"
                                    )
                            else:
                                logging.warning(
                                    f"[ITEM {self.company_index + 1}] Skipped item with no valid company name"
                                )

                            # Add small delay between companies
                            await asyncio.sleep(self.delay * 0.5)

                        except Exception as e:
                            logger.error(
                                f"[ITEM {self.company_index + 1}] Error processing container {i + 1}: {e}"
                            )
                            continue

                    # Update total processed containers count
                    total_containers_processed = current_container_count

                    pages_processed += 1

                    # Wait between page loads
                    await asyncio.sleep(self.delay)

                logger.info(
                    f"Parsing completed. Found {companies_found} new companies."
                )

            except Exception as e:
                logger.error(f"Error during parsing: {e}")

            finally:
                await browser.close()

        return companies_found


async def parse_website(
    website_url: str,
    max_pages: int = 10,
    delay: float = 1.0,
    headless: bool = True,
) -> int:
    """Main function to parse a website for companies"""
    parser = UniversalCompanyParser(website_url, delay, headless)
    return await parser.parse_companies(max_pages)


if __name__ == "__main__":
    # Example usage
    async def main():
        url = "https://example.com/companies"
        companies_found = await parse_website(
            url, max_pages=5, delay=1.0, headless=True
        )
        print(f"Found {companies_found} companies")

    asyncio.run(main())
