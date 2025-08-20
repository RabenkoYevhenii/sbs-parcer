"""Scraping tools and logic for SBC website."""

import asyncio
import logging
import time
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)

from config import settings
from helpers import (
    AttendeeData,
    SBCSelectors,
    CSVManager,
    URLHelper,
    clean_text,
    get_attendee_csv_fieldnames,
)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SBCScraper:
    """Main scraper class for SBC website."""

    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.csv_manager = CSVManager()
        self.url_helper = URLHelper()

        # Realistic Chrome user agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        try:
            await self.close_browser()
        except Exception as e:
            logger.warning(f"Error during context manager exit: {e}")
        # Don't suppress any exceptions
        return False

    async def start_browser(self) -> None:
        """Start browser with maximum stealth and anti-detection measures."""
        self.playwright = await async_playwright().start()

        # Enhanced browser setup with headless-specific anti-detection
        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--exclude-switches=enable-automation",
            "--disable-extensions",
            "--no-first-run",
            "--disable-default-apps",
        ]

        # Add headless-specific args if running in headless mode
        if settings.headless:
            launch_args.extend(
                [
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-software-rasterizer",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--disable-features=TranslateUI",
                    "--disable-ipc-flooding-protection",
                    "--window-size=1366,768",
                ]
            )

        self.browser = await self.playwright.chromium.launch(
            headless=settings.headless,
            args=launch_args,
        )

        # Enhanced context with better stealth settings
        selected_ua = self.user_agents[
            hash(str(time.time())) % len(self.user_agents)
        ]
        self.context = await self.browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent=selected_ua,
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
        )

        # Comprehensive anti-detection script for headless mode
        await self.context.add_init_script(
            """
            // Remove webdriver traces
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            delete navigator.webdriver;
            
            // Override headless detection
            Object.defineProperty(navigator, 'headless', {
                get: () => false,
            });
            
            // Override language and platform
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32',
            });
            
            // Mock hardware properties
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 4,
            });
            
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8,
            });
            
            // Override permissions API
            if (navigator.permissions && navigator.permissions.query) {
                const originalQuery = navigator.permissions.query;
                navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                    Promise.resolve({ state: 'granted' }) :
                    originalQuery(parameters)
                );
            }
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => ({
                    length: 1,
                    item: () => ({ name: 'Chrome PDF Plugin' }),
                    namedItem: () => null,
                    refresh: () => undefined,
                }),
            });
        """
        )

        self.page = await self.context.new_page()
        self.page.set_default_timeout(settings.browser_timeout)

    async def wait_for_cloudflare_challenge(self, timeout: int = 30) -> bool:
        """Wait for Cloudflare challenge to complete."""
        if not self.page:
            return False

        try:
            logger.info("Checking for Cloudflare challenge...")

            # Check for various Cloudflare indicators
            cloudflare_selectors = [
                "text=Checking your browser",
                "text=Please wait",
                "text=DDoS protection",
                "text=Just a moment",
                "text=Sorry, you have been blocked",
                ".cf-browser-verification",
                "#cf-challenge-running",
                ".cf-checking-browser",
            ]

            start_time = time.time()
            while time.time() - start_time < timeout:
                # Check if we're on a Cloudflare challenge page
                for selector in cloudflare_selectors:
                    try:
                        element = await self.page.query_selector(selector)
                        if element:
                            logger.info(
                                f"Cloudflare challenge detected: {selector}"
                            )
                            logger.info("Waiting for challenge to complete...")
                            await asyncio.sleep(2)
                            break
                    except:
                        continue
                else:
                    # No challenge detected, check if page loaded normally
                    try:
                        await self.page.wait_for_load_state(
                            "networkidle", timeout=3000
                        )
                        current_url = self.page.url
                        if (
                            "cloudflare" not in current_url.lower()
                            and "blocked" not in current_url.lower()
                        ):
                            logger.info(
                                "Page loaded successfully, no Cloudflare challenge"
                            )
                            return True
                    except:
                        pass

                await asyncio.sleep(1)

            logger.warning("Cloudflare challenge timeout")
            return False

        except Exception as e:
            logger.error(f"Error waiting for Cloudflare challenge: {e}")
            return False

    async def simulate_human_behavior(self) -> None:
        """Simulate human-like behavior with random mouse movements."""
        if not self.page:
            return

        try:
            # Random mouse movements
            for _ in range(3):
                x = 100 + (hash(str(time.time())) % 600)
                y = 100 + (hash(str(time.time() * 2)) % 400)
                await self.page.mouse.move(x, y)
                await asyncio.sleep(
                    0.1 + (hash(str(time.time())) % 100) / 1000
                )

        except Exception:
            pass  # Ignore errors in behavior simulation

    async def close_browser(self) -> None:
        """Close browser and clean up properly."""
        logger.info("Starting browser cleanup...")

        # Close page first
        if self.page:
            try:
                await self.page.close()
                logger.info("Page closed successfully")
            except Exception as e:
                logger.warning(f"Error closing page: {e}")
            finally:
                self.page = None

        # Close context second
        if self.context:
            try:
                await self.context.close()
                logger.info("Context closed successfully")
            except Exception as e:
                logger.warning(f"Error closing context: {e}")
            finally:
                self.context = None

        # Close browser third
        if self.browser:
            try:
                await self.browser.close()
                logger.info("Browser closed successfully")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            finally:
                self.browser = None

        # Stop playwright last
        if self.playwright:
            try:
                await self.playwright.stop()
                logger.info("Playwright stopped successfully")
            except Exception as e:
                logger.warning(f"Error stopping playwright: {e}")
            finally:
                self.playwright = None

        # Give more time for cleanup
        await asyncio.sleep(1.0)
        logger.info("Browser cleanup completed")

    async def login(self) -> bool:
        """Login to SBC website with two-step process."""
        if not self.page:
            logger.error("Browser page not initialized")
            return False

        try:
            logger.info("Navigating to login page...")
            logger.info(f"Login URL: {settings.sbc_login_url}")

            # Navigate directly to the target site with additional options
            await self.page.goto(
                settings.sbc_login_url, wait_until="networkidle", timeout=30000
            )

            # Wait for potential Cloudflare challenge
            if not await self.wait_for_cloudflare_challenge(timeout=60):
                logger.error("Failed to pass Cloudflare challenge")
                return False

            # Wait for page to load completely
            await self.page.wait_for_load_state("networkidle")

            # Simulate human behavior - random mouse movements
            await self.simulate_human_behavior()  # Handle cookies consent popup first
            logger.info("Checking for cookies consent popup...")
            try:
                cookies_button = await self.page.wait_for_selector(
                    "#c-p-bn", timeout=5000
                )
                if cookies_button:
                    logger.info("Cookies consent popup found, accepting...")
                    await cookies_button.click()
                    await asyncio.sleep(1)  # Wait for popup to disappear
                    logger.info("Cookies accepted successfully")
            except Exception as e:
                logger.info("No cookies popup found or already accepted")

            # Step 1: Find and fill email with human-like typing
            logger.info("Step 1: Filling email...")
            username_input = await self.page.wait_for_selector(
                SBCSelectors.LOGIN_USERNAME_INPUT, timeout=10000
            )
            if username_input:
                # Clear field first and type with human-like delays
                await username_input.click()
                await asyncio.sleep(
                    0.3 + (hash(str(time.time())) % 200) / 1000
                )  # Random delay
                await username_input.fill("")
                await asyncio.sleep(
                    0.2 + (hash(str(time.time())) % 100) / 1000
                )  # Random delay
                # Type each character with variable delays
                for char in settings.sbc_username:
                    await username_input.type(
                        char, delay=30 + (hash(char) % 40)
                    )
                await asyncio.sleep(
                    0.5 + (hash(str(time.time())) % 300) / 1000
                )  # Random delay
                logger.info("Email filled successfully")
            else:
                logger.error("Could not find email input field")
                return False

            # Step 2: Click "Next" button to proceed to password page
            logger.info("Step 2: Clicking Next button...")
            next_button = await self.page.wait_for_selector(
                SBCSelectors.LOGIN_NEXT_BUTTON, timeout=10000
            )
            if next_button:
                # Small delay before clicking with randomization
                await asyncio.sleep(
                    0.8 + (hash(str(time.time())) % 400) / 1000
                )

                # Move mouse to button before clicking
                box = await next_button.bounding_box()
                if box:
                    await self.page.mouse.move(
                        box["x"] + box["width"] / 2,
                        box["y"] + box["height"] / 2,
                    )
                    await asyncio.sleep(0.1)

                await next_button.click()
                logger.info(
                    "Next button clicked, waiting for password page..."
                )
                # Wait for navigation to password page with longer timeout
                try:
                    await self.page.wait_for_load_state(
                        "networkidle", timeout=60000
                    )
                    logger.info("Password page loaded successfully")
                except Exception as e:
                    logger.warning(
                        f"Networkidle timeout, trying domcontentloaded: {e}"
                    )
                    await self.page.wait_for_load_state(
                        "domcontentloaded", timeout=30000
                    )

                await asyncio.sleep(
                    3.0 + (hash(str(time.time())) % 1000) / 1000
                )  # Longer delay for headless mode
            else:
                logger.error("Could not find Next button")
                return False

            # Step 3: Find and fill password on the new page with human-like typing
            logger.info("Step 3: Filling password...")
            password_input = await self.page.wait_for_selector(
                SBCSelectors.LOGIN_PASSWORD_INPUT, timeout=10000
            )
            if password_input:
                # Click and clear field, then type with delays
                await password_input.click()
                await asyncio.sleep(
                    0.3 + (hash(str(time.time())) % 200) / 1000
                )  # Random delay
                await password_input.fill("")
                await asyncio.sleep(
                    0.2 + (hash(str(time.time())) % 100) / 1000
                )  # Random delay
                # Type each character with variable delays
                for char in settings.sbc_password:
                    await password_input.type(
                        char, delay=50 + (hash(char) % 50)
                    )
                await asyncio.sleep(
                    0.7 + (hash(str(time.time())) % 300) / 1000
                )  # Random delay
                logger.info("Password filled successfully")
            else:
                logger.error("Could not find password input field")
                return False

            # Step 4: Submit login form
            logger.info("Step 4: Submitting login form...")
            submit_button = await self.page.wait_for_selector(
                SBCSelectors.LOGIN_SUBMIT_BUTTON, timeout=10000
            )

            if submit_button:
                # Human-like delay before submission with randomization
                await asyncio.sleep(
                    1.2 + (hash(str(time.time())) % 800) / 1000
                )

                # Move mouse to submit button
                box = await submit_button.bounding_box()
                if box:
                    await self.page.mouse.move(
                        box["x"] + box["width"] / 2,
                        box["y"] + box["height"] / 2,
                    )
                    await asyncio.sleep(0.1)

                # Click submit and wait for navigation
                async with self.page.expect_navigation():
                    await submit_button.click()
                logger.info("Login form submitted")
            else:
                logger.error("Could not find submit button")
                return False

            # Check if login was successful
            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(
                2 + (hash(str(time.time())) % 1000) / 1000
            )  # Random wait for redirects

            # Verify login by checking if we're redirected away from login page
            current_url = self.page.url
            if "login" not in current_url.lower():
                logger.info("Login successful!")
                return True
            else:
                logger.error("Login failed - still on login page")
                return False

        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            return False

    async def extract_attendee_from_profile_element(
        self, profile_element, base_url: str
    ) -> Optional[AttendeeData]:
        """Extract attendee data directly from profile element without navigation."""
        try:
            # Helper function to safely get text from element
            async def get_text_from_element(element, selector: str) -> str:
                try:
                    sub_element = await element.query_selector(selector)
                    if sub_element:
                        return clean_text(await sub_element.inner_text())
                    return ""
                except:
                    return ""

            # Helper function to safely get attribute from element
            async def get_attribute_from_element(
                element, selector: str, attribute: str
            ) -> str:
                try:
                    sub_element = await element.query_selector(selector)
                    if sub_element:
                        return await sub_element.get_attribute(attribute) or ""
                    return ""
                except:
                    return ""

            # Extract name
            name = await get_text_from_element(
                profile_element, SBCSelectors.ATTENDEE_NAME
            )
            if not name:
                return None

            # Click on profile to get URL (but don't navigate away)
            current_url = ""
            try:
                # Store current page URL before clicking
                original_url = self.page.url if self.page else ""

                # Check if element is clickable
                clickable_element = await profile_element.query_selector(
                    "app-person-profile, .profile-item, .attendee-item"
                )
                if clickable_element:
                    # Get the current URL after clicking (this might trigger a modal or expand the profile)
                    await clickable_element.click()
                    await asyncio.sleep(1)  # Wait for any URL change or modal
                    current_url = self.page.url if self.page else original_url

                    # If URL changed, we can use it as source URL
                    if current_url != original_url:
                        logger.info(f"Profile URL changed to: {current_url}")
                    else:
                        # If no URL change, construct a generic profile URL
                        current_url = f"{base_url}#profile-{name.replace(' ', '-').lower()}"
                else:
                    current_url = (
                        f"{base_url}#profile-{name.replace(' ', '-').lower()}"
                    )

            except Exception as e:
                logger.warning(f"Could not get profile URL for {name}: {e}")
                current_url = (
                    f"{base_url}#profile-{name.replace(' ', '-').lower()}"
                )

            # Initialize attendee data
            attendee = AttendeeData(full_name=name, source_url=current_url)

            # Extract company name - look for the fw-600 class that's not the name
            company_selectors = [
                ".fw-600.overflow-hidden.text-overflow-ellipsis.text-nowrap",
                ".company",
                ".company-name",
                ".organization",
            ]

            for selector in company_selectors:
                company_elements = await profile_element.query_selector_all(
                    selector
                )
                for elem in company_elements:
                    text = clean_text(await elem.inner_text())
                    # Skip if it's the person's name or position
                    if (
                        text
                        and text != name
                        and not any(
                            word in text.lower()
                            for word in [
                                "head",
                                "manager",
                                "director",
                                "officer",
                                "lead",
                                "specialist",
                            ]
                        )
                    ):
                        attendee.company_name = text
                        break
                if attendee.company_name:
                    break

            # Extract position - look for text that's not name or company
            position_selectors = [
                ".overflow-hidden.text-overflow-ellipsis.text-nowrap:not(.name):not(.fw-600)",
                ".position",
                ".title",
                ".job-title",
            ]

            for selector in position_selectors:
                position_elem = await profile_element.query_selector(selector)
                if position_elem:
                    text = clean_text(await position_elem.inner_text())
                    if text and text != name and text != attendee.company_name:
                        attendee.position = text
                        break

            # Extract detailed information from details items
            details_items = await profile_element.query_selector_all(
                ".details-item"
            )

            for item in details_items:
                try:
                    # Get the header text
                    header = await item.query_selector("h5")
                    if not header:
                        continue

                    header_text = clean_text(await header.inner_text()).lower()

                    # Get the field value
                    value_elem = await item.query_selector(".field-value")
                    if not value_elem:
                        continue

                    value = clean_text(await value_elem.inner_text())

                    # Map header text to attendee fields
                    if "country" in header_text:
                        attendee.country = value
                    elif "responsibility" in header_text:
                        attendee.responsibility = value
                    elif "gaming vertical" in header_text:
                        attendee.gaming_vertical = value
                    elif (
                        "organisation type" in header_text
                        or "organization type" in header_text
                    ):
                        attendee.organization_type = value
                    elif "introduction" in header_text:
                        attendee.introduction = value

                except Exception as e:
                    logger.warning(f"Error processing details item: {e}")
                    continue

            # Extract profile image
            try:
                img_element = await profile_element.query_selector(
                    SBCSelectors.PROFILE_IMAGE
                )
                if img_element:
                    img_src = await img_element.get_attribute("src")
                    if img_src:
                        attendee.profile_image_url = (
                            self.url_helper.normalize_url(img_src, base_url)
                        )
            except Exception as e:
                logger.warning(f"Error extracting profile image: {e}")

            # Note: Social media links extraction skipped for profile elements
            # as they require clicking and navigation which is handled in the full profile extraction
            logger.info(
                "Social media extraction skipped for profile element - will be done in full profile extraction"
            )

            logger.info(f"Successfully extracted attendee data: {name}")
            return attendee

        except Exception as e:
            logger.error(
                f"Error extracting attendee from profile element: {str(e)}"
            )
            return None

    async def extract_attendee_from_current_page(
        self, name: str, profile_url: str
    ) -> Optional[AttendeeData]:
        """Extract attendee data from the current profile page or modal."""
        if not self.page:
            return None

        try:
            # Initialize attendee data with basic info
            attendee = AttendeeData(full_name=name, source_url=profile_url)

            # Wait for page to fully load
            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)

            # First, check if we're in a modal or profile details section
            modal_selector = "div[role='dialog'], .modal, .sidebar, .profile-panel, .profile-details"
            modal = await self.page.query_selector(modal_selector)

            # Set the search context - modal if exists, otherwise page
            search_context = modal if modal else self.page

            # Extract name (verify it matches)
            try:
                name_selectors = [
                    SBCSelectors.PROFILE_NAME,
                    ".name",
                    "h1, h2, h3",
                    ".attendee-name",
                    ".profile-name",
                ]

                page_name = None
                for selector in name_selectors:
                    if hasattr(search_context, "query_selector"):
                        name_element = await search_context.query_selector(
                            selector
                        )
                    else:
                        name_element = await self.page.query_selector(selector)

                    if name_element:
                        page_name = clean_text(await name_element.inner_text())
                        if page_name and len(page_name) > 2:
                            attendee.full_name = page_name
                            break

                if not page_name:
                    attendee.full_name = name  # Keep original name

            except Exception as e:
                logger.warning(f"Could not extract name from profile: {e}")
                attendee.full_name = name

            # Extract company name
            try:
                company_selectors = [
                    SBCSelectors.PROFILE_COMPANY,
                    ".company",
                    ".company-name",
                    ".organization",
                ]

                for selector in company_selectors:
                    if hasattr(search_context, "query_selector"):
                        company_element = await search_context.query_selector(
                            selector
                        )
                    else:
                        company_element = await self.page.query_selector(
                            selector
                        )

                    if company_element:
                        company_text = clean_text(
                            await company_element.inner_text()
                        )
                        if company_text and len(company_text) > 1:
                            attendee.company_name = company_text
                            break

            except Exception as e:
                logger.warning(f"Could not extract company: {e}")

            # Extract position/title
            try:
                position_selectors = [
                    SBCSelectors.PROFILE_POSITION,
                    ".title",
                    ".position",
                    ".job-title",
                ]

                for selector in position_selectors:
                    if hasattr(search_context, "query_selector"):
                        position_element = await search_context.query_selector(
                            selector
                        )
                    else:
                        position_element = await self.page.query_selector(
                            selector
                        )

                    if position_element:
                        position_text = clean_text(
                            await position_element.inner_text()
                        )
                        if position_text and len(position_text) > 1:
                            attendee.position = position_text
                            break

            except Exception as e:
                logger.warning(f"Could not extract position: {e}")

            # Extract profile image
            try:
                img_element = await self.page.query_selector(
                    SBCSelectors.PROFILE_IMAGE
                )

                if img_element:
                    logger.info("Found image element, getting src attribute")
                    img_src = await img_element.get_attribute("src")
                    if img_src:
                        # Take any valid image URL, not just profile-photo ones
                        attendee.profile_image_url = (
                            self.url_helper.normalize_url(img_src, profile_url)
                        )
                        logger.info(f"✅ Extracted profile image: {img_src}")
                        logger.info(
                            f"✅ Normalized profile image URL: {attendee.profile_image_url}"
                        )
                    else:
                        logger.warning(
                            "Image element found but no src attribute"
                        )
                else:
                    logger.warning("No profile image element found")
            except Exception as e:
                logger.warning(f"Could not extract profile image: {e}")

            # Extract detailed information from details items
            try:
                details_items = await self.page.query_selector_all(
                    ".details-item"
                )

                for item in details_items:
                    try:
                        # Get the header text
                        header = await item.query_selector("h5")
                        if not header:
                            continue

                        header_text = clean_text(
                            await header.inner_text()
                        ).lower()

                        # Get the field value
                        value_elem = await item.query_selector(".field-value")
                        if not value_elem:
                            continue

                        value = clean_text(await value_elem.inner_text())

                        # Map header text to attendee fields
                        if "country" in header_text:
                            attendee.country = value
                        elif "responsibility" in header_text:
                            attendee.responsibility = value
                        elif "gaming vertical" in header_text:
                            attendee.gaming_vertical = value
                        elif (
                            "organisation type" in header_text
                            or "organization type" in header_text
                        ):
                            attendee.organization_type = value
                        elif "introduction" in header_text:
                            attendee.introduction = value

                    except Exception as e:
                        logger.warning(f"Error processing details item: {e}")
                        continue

            except Exception as e:
                logger.warning(f"Could not extract details: {e}")

            # Extract social media links by clicking each button
            try:
                # Find the social media container
                social_container = await self.page.query_selector(
                    SBCSelectors.SOCIAL_MEDIA_CONTAINER
                )

                linkedin_url = ""
                facebook_url = ""
                x_twitter_url = ""
                other_socials = []

                if social_container:
                    logger.info(
                        "Found social media container, extracting links..."
                    )

                    # Get all social media buttons in the container
                    social_buttons = await social_container.query_selector_all(
                        SBCSelectors.SOCIAL_MEDIA_BUTTONS
                    )

                    logger.info(
                        f"Found {len(social_buttons)} social media buttons"
                    )

                    for i, button in enumerate(social_buttons):
                        try:
                            logger.info(
                                f"Clicking social media button {i + 1}..."
                            )

                            # Listen for new page/tab creation
                            new_page = None
                            try:
                                if self.context:
                                    async with self.context.expect_page() as page_info:
                                        # Click the social media button (opens new tab)
                                        await button.click()
                                        # Get the new page
                                        new_page = await page_info.value
                                else:
                                    logger.warning(
                                        "Browser context not available for tab handling"
                                    )
                                    continue

                                if new_page:
                                    # Get the URL from the new tab immediately
                                    social_url = new_page.url
                                    logger.info(
                                        f"Got social URL from new tab: {social_url}"
                                    )

                                    # Close the new tab immediately
                                    await new_page.close()

                                    # Categorize the social media link
                                    if "linkedin" in social_url.lower():
                                        linkedin_url = social_url
                                        logger.info(
                                            f"Found LinkedIn: {social_url}"
                                        )
                                    elif "facebook" in social_url.lower():
                                        facebook_url = social_url
                                        logger.info(
                                            f"Found Facebook: {social_url}"
                                        )
                                    elif (
                                        "twitter" in social_url.lower()
                                        or "x.com" in social_url.lower()
                                    ):
                                        x_twitter_url = social_url
                                        logger.info(
                                            f"Found Twitter/X: {social_url}"
                                        )
                                    else:
                                        # Add to other socials if it's a valid URL
                                        if social_url.startswith(
                                            ("http://", "https://")
                                        ):
                                            other_socials.append(social_url)
                                            logger.info(
                                                f"Found other social: {social_url}"
                                            )

                            except Exception as tab_error:
                                logger.info(
                                    f"No new tab opened for button {i + 1}, might be a different action"
                                )

                            # Small delay between clicks
                            await asyncio.sleep(0.5)

                        except Exception as button_error:
                            logger.warning(
                                f"Error processing social button {i + 1}: {button_error}"
                            )
                            continue
                else:
                    logger.info("No social media container found")

                # Assign the extracted social media URLs
                attendee.linkedin_url = linkedin_url
                attendee.facebook_url = facebook_url
                attendee.x_twitter_url = x_twitter_url
                attendee.other_socials = (
                    "; ".join(other_socials) if other_socials else ""
                )

            except Exception as e:
                logger.warning(f"Could not extract social media links: {e}")

            logger.info(
                f"Successfully extracted attendee data from profile page: {attendee.full_name}"
            )
            return attendee

        except Exception as e:
            logger.error(f"Error extracting attendee data: {e}")
            # Return basic attendee data even if extraction fails
            return AttendeeData(full_name=name, source_url=profile_url)

    async def apply_filters(self) -> bool:
        """Apply filters to attendees list before scraping."""
        if not self.page:
            logger.error("Browser page not initialized")
            return False

        try:
            logger.info("Applying filters to attendees list...")

            # Wait a bit for the page to fully load
            await asyncio.sleep(2)

            # Click on the filters button
            filters_button_selector = "#page-top > app-root > app-event > div > div > div > app-attendees-list > div > app-advanced-search > div > button"

            try:
                logger.info("Looking for filters button...")
                filters_button = await self.page.wait_for_selector(
                    filters_button_selector, timeout=15000
                )
                if filters_button:
                    logger.info("Clicking filters button...")
                    await filters_button.click()
                    await asyncio.sleep(3)  # Wait for filters panel to open
                else:
                    logger.warning("Filters button not found")
                    return False
            except Exception as e:
                logger.warning(f"Could not click filters button: {e}")
                return False

            # Wait for gaming verticals filter to be available
            gaming_verticals_selector = "#gaming-verticals"

            try:
                logger.info("Looking for gaming verticals filter...")
                await self.page.wait_for_selector(
                    gaming_verticals_selector, timeout=15000
                )

                # Click on gaming verticals to open the dropdown
                gaming_verticals_element = await self.page.query_selector(
                    gaming_verticals_selector
                )
                if gaming_verticals_element:
                    logger.info("Clicking gaming verticals filter...")
                    await gaming_verticals_element.click()
                    await asyncio.sleep(3)  # Wait for dropdown to open
                else:
                    logger.warning("Gaming verticals element not found")
                    return False

            except Exception as e:
                logger.warning(
                    f"Could not access gaming verticals filter: {e}"
                )
                return False

            # Now find and select all checkboxes with "Online:" in their labels
            try:
                logger.info("Looking for 'Online:' gaming vertical options...")

                # Find all checkboxes within the gaming verticals section
                checkbox_elements = await self.page.query_selector_all(
                    "mat-checkbox"
                )
                online_options_found = 0
                online_options_selected = 0

                for checkbox in checkbox_elements:
                    try:
                        # Get the label text for this checkbox
                        label_element = await checkbox.query_selector(
                            "label.mdc-label"
                        )
                        if label_element:
                            label_text = await label_element.inner_text()
                            label_text = label_text.strip()

                            # Check if this is an "Online:" option
                            if label_text.startswith("Online:"):
                                online_options_found += 1
                                logger.info(
                                    f"Found Online option: {label_text}"
                                )

                                # Find the input element within this checkbox
                                input_element = await checkbox.query_selector(
                                    "input[type='checkbox']"
                                )
                                if input_element:
                                    # Check if it's already checked
                                    is_checked = (
                                        await input_element.is_checked()
                                    )
                                    if not is_checked:
                                        logger.info(f"Selecting: {label_text}")
                                        await input_element.click()
                                        online_options_selected += 1
                                        await asyncio.sleep(
                                            0.8
                                        )  # Small delay between selections
                                    else:
                                        logger.info(
                                            f"Already selected: {label_text}"
                                        )
                                        online_options_selected += 1
                                else:
                                    logger.warning(
                                        f"Input element not found for: {label_text}"
                                    )

                    except Exception as e:
                        logger.warning(f"Error processing checkbox: {e}")
                        continue

                logger.info(
                    f"Found {online_options_found} Online gaming vertical options"
                )
                logger.info(
                    f"Selected {online_options_selected} Online gaming vertical options"
                )

                if online_options_found == 0:
                    logger.warning("No Online gaming vertical options found")
                    return False

            except Exception as e:
                logger.error(f"Error selecting Online gaming verticals: {e}")
                return False

            # Wait a moment for selections to register
            await asyncio.sleep(2)

            # Apply the gaming verticals filter by clicking its apply button
            try:
                logger.info("Looking for gaming verticals apply button...")
                gaming_apply_button_selectors = [
                    "button:has-text('Apply')",
                    "button[type='submit']",
                    ".apply-filters",
                    ".search-button",
                    "button.mat-raised-button",
                    "button.mat-button",
                ]

                gaming_button_found = False
                for selector in gaming_apply_button_selectors:
                    try:
                        gaming_apply_button = (
                            await self.page.wait_for_selector(
                                selector, timeout=5000
                            )
                        )
                        if gaming_apply_button:
                            logger.info(
                                f"Clicking gaming verticals apply button with selector: {selector}"
                            )
                            await gaming_apply_button.click()
                            gaming_button_found = True
                            await asyncio.sleep(3)  # Wait for application
                            break
                    except:
                        continue

                if not gaming_button_found:
                    logger.warning(
                        "Gaming verticals apply button not found, proceeding anyway"
                    )

            except Exception as e:
                logger.warning(
                    f"Could not click gaming verticals apply button: {e}"
                )

            # Now apply the second filter: Organization Types
            try:
                logger.info("Applying organization type filter...")

                # Wait for organization type filter elements to be available after gaming verticals selection
                await asyncio.sleep(3)

                # Click on organization type filter to open it
                organization_type_selector = "#organization-type"
                try:
                    logger.info("Looking for organization type filter...")
                    await self.page.wait_for_selector(
                        organization_type_selector, timeout=15000
                    )

                    organization_type_element = await self.page.query_selector(
                        organization_type_selector
                    )
                    if organization_type_element:
                        logger.info("Clicking organization type filter...")
                        await organization_type_element.click()
                        await asyncio.sleep(3)  # Wait for dropdown to open
                    else:
                        logger.warning("Organization type element not found")
                        # Continue anyway, maybe it's already open

                except Exception as e:
                    logger.warning(
                        f"Could not click organization type filter: {e}"
                    )

                # Find all checkboxes in the organization type filter (they appear in overlay)
                organization_checkbox_elements = (
                    await self.page.query_selector_all("mat-checkbox")
                )
                organization_options_found = 0
                organization_options_selected = 0

                # Define the organization types we want to select
                target_organization_types = [
                    "Operator - Casino/Bookmaker/Sportsbook",
                    "Supplier/Service Provider",
                    "Affiliate",
                    "Sports Organisation",
                ]

                logger.info(
                    f"Looking for organization type options: {target_organization_types}"
                )

                for checkbox in organization_checkbox_elements:
                    try:
                        # Get the label text for this checkbox
                        label_element = await checkbox.query_selector(
                            "label.mdc-label"
                        )
                        if label_element:
                            label_text = await label_element.inner_text()
                            label_text = label_text.strip()

                            # Check if this is one of our target organization types
                            if label_text in target_organization_types:
                                organization_options_found += 1
                                logger.info(
                                    f"Found target organization type: {label_text}"
                                )

                                # Find the input element within this checkbox
                                input_element = await checkbox.query_selector(
                                    "input[type='checkbox']"
                                )
                                if input_element:
                                    # Check if it's already checked
                                    is_checked = (
                                        await input_element.is_checked()
                                    )
                                    if not is_checked:
                                        logger.info(
                                            f"Selecting organization type: {label_text}"
                                        )
                                        await input_element.click()
                                        organization_options_selected += 1
                                        await asyncio.sleep(
                                            0.8
                                        )  # Small delay between selections
                                    else:
                                        logger.info(
                                            f"Already selected organization type: {label_text}"
                                        )
                                        organization_options_selected += 1
                                else:
                                    logger.warning(
                                        f"Input element not found for organization type: {label_text}"
                                    )

                    except Exception as e:
                        logger.warning(
                            f"Error processing organization checkbox: {e}"
                        )
                        continue

                logger.info(
                    f"Found {organization_options_found} target organization type options"
                )
                logger.info(
                    f"Selected {organization_options_selected} target organization type options"
                )

                if organization_options_found > 0:
                    # Wait a moment and then click the organization type apply button
                    await asyncio.sleep(2)

                    try:
                        logger.info(
                            "Looking for organization type apply button..."
                        )
                        selectors = [
                            "button:has-text('Apply')",
                            "button[type='submit']",
                            ".apply-filters",
                            ".search-button",
                            "button.multi-select-actions-apply",
                            "button.mat-raised-button",
                            "button.mat-button",
                        ]
                        for selector in selectors:
                            btn = await self.page.query_selector(selector)
                            if btn:
                                logger.info(
                                    f"Clicking organization type apply button: {selector}"
                                )
                                await btn.click()
                                await asyncio.sleep(3)
                                break
                        else:
                            logger.warning(
                                "Organization type apply button not found, proceeding anyway"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Could not click organization type apply button: {e}"
                        )
                else:
                    logger.warning("No target organization type options found")
            except Exception as e:
                logger.error(f"Error applying organization type filter: {e}")

            # Wait a moment for all selections to register
            await asyncio.sleep(3)

            # Apply the final search with the specific search button
            try:
                logger.info("Looking for main search button...")

                # Try the specific class selectors first
                search_button_selectors = [
                    "button.action-btn.search",
                    ".action-btn.search",
                    "button:has-text('Search')",
                    "button[type='submit']",
                    ".search-button",
                ]

                search_button_found = False
                for search_selector in search_button_selectors:
                    try:
                        search_button = await self.page.wait_for_selector(
                            search_selector, timeout=5000
                        )
                        if search_button:
                            logger.info(
                                f"Clicking main search button with selector: {search_selector}"
                            )
                            await search_button.click()
                            logger.info(
                                "Main search button clicked successfully"
                            )
                            search_button_found = True
                            await asyncio.sleep(
                                5
                            )  # Wait for search results to load
                            break
                    except Exception as e:
                        logger.debug(
                            f"Search selector {search_selector} failed: {e}"
                        )
                        continue

                if not search_button_found:
                    logger.warning("No search button found with any selector")

                # Wait for results to update
                await self.page.wait_for_load_state("networkidle")

            except Exception as e:
                logger.warning(f"Could not click search button: {e}")

            # Close the filter panel if it's still open (optional)
            try:
                # Look for a close button or click outside the filter area
                close_selectors = [
                    "button:has-text('✕')",
                    "button:has-text('Close')",
                    ".close-button",
                    "[aria-label='Close']",
                ]

                for selector in close_selectors:
                    try:
                        close_button = await self.page.wait_for_selector(
                            selector, timeout=2000
                        )
                        if close_button:
                            await close_button.click()
                            logger.info("Closed filter panel")
                            break
                    except:
                        continue

            except Exception as e:
                logger.debug(f"Could not close filter panel: {e}")

            await asyncio.sleep(2)
            logger.info("Filters applied successfully")
            return True

        except Exception as e:
            logger.error(f"Error applying filters: {e}")
            return False

    async def scroll_and_scrape_attendees(
        self, page_url: str, csv_filepath: str
    ) -> int:
        """Scrape attendees with scrolling in the ng-scroll-layer container."""
        if not self.page:
            logger.error("Browser page not initialized")
            return 0

        logger.info(f"Starting scrolling scrape for: {page_url}")

        # Navigate to the page first
        await self.page.goto(page_url)
        await self.page.wait_for_load_state("networkidle")

        # Apply filters before starting to scrape
        logger.info("Applying filters to narrow down attendees...")
        filter_success = await self.apply_filters()
        if not filter_success:
            logger.warning(
                "Failed to apply filters, proceeding with unfiltered results"
            )
        else:
            logger.info(
                "Filters applied successfully, proceeding with filtered results"
            )

        # Ensure CSV file exists with proper headers
        fieldnames = get_attendee_csv_fieldnames()
        self.csv_manager.create_csv_if_not_exists(csv_filepath, fieldnames)

        # Read existing names to avoid duplicates
        existing_names = self.csv_manager.read_existing_names(
            csv_filepath, "full_name"
        )
        logger.info(f"Found {len(existing_names)} existing attendees in CSV")

        total_scraped = 0
        scroll_attempts = 0
        max_scroll_attempts = 800000000  # Prevent infinite loops
        no_new_data_attempts = 0
        max_no_data_attempts = 800000000

        try:
            # Wait for attendees to load
            try:
                await self.page.wait_for_selector(
                    SBCSelectors.ATTENDEE_ITEM, timeout=10000
                )
            except Exception as e:
                logger.warning(f"Attendee items not found: {e}")
                return 0

            # Wait for scroll container to be available
            scroll_container = None
            try:
                await self.page.wait_for_selector(
                    ".ng-scroll-layer", timeout=10000
                )
                scroll_container = await self.page.query_selector(
                    ".ng-scroll-layer"
                )
                if scroll_container:
                    logger.info("Found ng-scroll-layer container")
                else:
                    logger.warning(
                        "ng-scroll-layer container not found, will use fallback scrolling"
                    )
            except Exception as e:
                logger.warning(f"Error finding scroll container: {e}")

            last_attendee_count = 0

            while scroll_attempts < max_scroll_attempts:
                scroll_attempts += 1
                logger.info(f"Scroll attempt {scroll_attempts}")

                # Get all currently visible attendee elements
                attendee_items = await self.page.query_selector_all(
                    SBCSelectors.ATTENDEE_ITEM
                )
                current_attendee_count = len(attendee_items)

                logger.info(
                    f"Found {current_attendee_count} attendees on page (attempt {scroll_attempts})"
                )

                # Check if we got new attendees since last scroll
                if scroll_attempts > 1:
                    if current_attendee_count <= last_attendee_count:
                        no_new_data_attempts += 1
                        logger.info(
                            f"No new attendees loaded (attempt {no_new_data_attempts}/{max_no_data_attempts})"
                        )

                        if no_new_data_attempts >= max_no_data_attempts:
                            logger.info(
                                "No new attendees after multiple scroll attempts, ending"
                            )
                            break
                    else:
                        no_new_data_attempts = (
                            0  # Reset counter when new attendees are found
                        )
                        logger.info(
                            f"Found {current_attendee_count - last_attendee_count} new attendees"
                        )

                last_attendee_count = current_attendee_count

                # Process all visible attendees using index-based iteration to handle stale elements
                attendee_count = 0
                processed_attendees = 0
                batch_scraped = 0

                while processed_attendees < current_attendee_count:
                    try:
                        # Re-find attendee items to avoid stale element issues
                        attendee_items = await self.page.query_selector_all(
                            SBCSelectors.ATTENDEE_ITEM
                        )

                        if processed_attendees >= len(attendee_items):
                            logger.info(
                                "No more attendees to process in current batch"
                            )
                            break

                        item = attendee_items[processed_attendees]
                        attendee_count += 1

                        # Get the current page URL for navigation
                        current_page_url = self.page.url

                        # Get attendee name for duplicate checking
                        try:
                            name_element = await item.query_selector(
                                SBCSelectors.ATTENDEE_NAME
                            )
                            if not name_element:
                                processed_attendees += 1
                                continue
                        except Exception as element_error:
                            logger.warning(
                                f"Stale element encountered: {element_error}"
                            )
                            processed_attendees += 1
                            continue

                        name = await name_element.inner_text()
                        name = clean_text(name)

                        if not name or name.lower().strip() in existing_names:
                            logger.info(f"Skipping duplicate attendee: {name}")
                            processed_attendees += 1
                            continue

                        logger.info(
                            f"Processing new attendee ({total_scraped + 1}): {name}"
                        )

                        # Extract data directly from the attendee card
                        attendee_data = None

                        try:
                            # Extract name
                            name_elem = await item.query_selector(".name")
                            if name_elem:
                                extracted_name = clean_text(
                                    await name_elem.inner_text()
                                )
                                if extracted_name:
                                    name = extracted_name

                            # Extract position/title
                            position = ""
                            title_elem = await item.query_selector(".title")
                            if title_elem:
                                position = clean_text(
                                    await title_elem.inner_text()
                                )

                            # Extract company
                            company = ""
                            company_elem = await item.query_selector(
                                ".company"
                            )
                            if company_elem:
                                company = clean_text(
                                    await company_elem.inner_text()
                                )

                            # Create basic attendee data from card information
                            attendee_data = AttendeeData(
                                full_name=name,
                                company_name=company,
                                position=position,
                                source_url=current_page_url,  # Will be updated with profile URL
                            )

                            logger.info(
                                f"✅ Extracted basic data from card for: {name}"
                            )

                        except Exception as card_error:
                            logger.warning(
                                f"Failed to extract from card for {name}: {card_error}"
                            )
                            # Create minimal attendee data
                            attendee_data = AttendeeData(
                                full_name=name,
                                source_url=current_page_url,
                            )

                        # Now try to enhance the data by clicking on the profile to get detailed information
                        if attendee_data:
                            try:
                                logger.info(
                                    f"🔍 Attempting to get detailed profile data for: {name}"
                                )

                                # Get current page URL to check if navigation happens
                                initial_url = self.page.url
                                navigation_successful = False

                                # Try clicking on the profile to navigate
                                clickable_element = await item.query_selector(
                                    ".name"
                                )
                                if not clickable_element:
                                    # Try clicking on the entire attendee card
                                    clickable_element = item

                                try:
                                    logger.info(
                                        f"🖱️ Clicking on profile for: {name}"
                                    )
                                    await clickable_element.click()

                                    # Wait a moment for potential navigation
                                    await asyncio.sleep(1)

                                    # Check if page URL changed (indicating successful navigation)
                                    current_url = self.page.url
                                    if current_url != initial_url:
                                        logger.info(
                                            f"✅ Successfully navigated to profile page for: {name}"
                                        )
                                        navigation_successful = True
                                    else:
                                        logger.warning(
                                            f"⚠️ Page didn't change after clicking for: {name}, waiting 5 seconds..."
                                        )
                                        await asyncio.sleep(5)

                                        # Try one more time
                                        await clickable_element.click()
                                        await asyncio.sleep(2)

                                        current_url = self.page.url
                                        if current_url != initial_url:
                                            logger.info(
                                                f"✅ Navigation successful on second try for: {name}"
                                            )
                                            navigation_successful = True
                                        else:
                                            logger.warning(
                                                f"🔄 Still no navigation, refreshing page for: {name}"
                                            )
                                            await self.page.reload(
                                                wait_until="networkidle"
                                            )
                                            await asyncio.sleep(2)

                                            # Wait for attendees to load after page refresh
                                            await self.page.wait_for_selector(
                                                SBCSelectors.ATTENDEE_ITEM,
                                                timeout=10000,
                                            )

                                            # Update initial URL after refresh
                                            initial_url = self.page.url
                                            logger.info(
                                                f"✅ Page refreshed, updated initial URL: {initial_url}"
                                            )

                                except Exception as e:
                                    logger.error(
                                        f"❌ Error clicking on profile for {name}: {e}"
                                    )
                                    await asyncio.sleep(2)
                                    continue

                                if navigation_successful:
                                    # Wait for profile page to load
                                    await self.page.wait_for_load_state(
                                        "networkidle"
                                    )
                                    await asyncio.sleep(1)

                                    # Get the profile URL
                                    profile_url = self.page.url

                                    # Extract enhanced attendee data from the profile page
                                    enhanced_data = await self.extract_attendee_from_current_page(
                                        name, profile_url
                                    )

                                    if enhanced_data:
                                        # Merge card data with profile data, keeping profile data priority for most fields
                                        attendee_data.source_url = profile_url
                                        attendee_data.full_name = (
                                            enhanced_data.full_name
                                            or attendee_data.full_name
                                        )
                                        attendee_data.company_name = (
                                            enhanced_data.company_name
                                            or attendee_data.company_name
                                        )
                                        attendee_data.position = (
                                            enhanced_data.position
                                            or attendee_data.position
                                        )

                                        # Profile-only data
                                        attendee_data.linkedin_url = (
                                            enhanced_data.linkedin_url
                                        )
                                        attendee_data.facebook_url = (
                                            enhanced_data.facebook_url
                                        )
                                        attendee_data.x_twitter_url = (
                                            enhanced_data.x_twitter_url
                                        )
                                        attendee_data.other_socials = (
                                            enhanced_data.other_socials
                                        )
                                        attendee_data.country = (
                                            enhanced_data.country
                                        )
                                        attendee_data.responsibility = (
                                            enhanced_data.responsibility
                                        )
                                        attendee_data.gaming_vertical = (
                                            enhanced_data.gaming_vertical
                                        )
                                        attendee_data.organization_type = (
                                            enhanced_data.organization_type
                                        )
                                        attendee_data.introduction = (
                                            enhanced_data.introduction
                                        )
                                        attendee_data.profile_image_url = (
                                            enhanced_data.profile_image_url
                                        )

                                        logger.info(
                                            f"✅ Enhanced data from profile for: {name}"
                                        )
                                    else:
                                        # Keep the profile URL even if extraction failed
                                        attendee_data.source_url = profile_url
                                        logger.warning(
                                            f"⚠️ Failed to extract enhanced data from profile for: {name}"
                                        )

                                    # Navigate back to the attendees list
                                    try:
                                        back_button = (
                                            await self.page.wait_for_selector(
                                                ".back-btn.ng-star-inserted",
                                                timeout=5000,
                                            )
                                        )
                                        if back_button:
                                            await back_button.click()
                                            await self.page.wait_for_load_state(
                                                "networkidle"
                                            )
                                            await asyncio.sleep(1)
                                            logger.info(
                                                f"⬅️ Navigated back to attendees list using back button"
                                            )
                                        else:
                                            # Fallback to goto if back button not found
                                            await self.page.goto(
                                                current_page_url
                                            )
                                            await self.page.wait_for_load_state(
                                                "networkidle"
                                            )
                                            await asyncio.sleep(1)
                                            logger.info(
                                                f"⬅️ Navigated back to attendees list using goto (fallback)"
                                            )

                                    except Exception as nav_error:
                                        logger.warning(
                                            f"Failed to navigate back: {nav_error}"
                                        )

                                else:
                                    logger.warning(
                                        f"⚠️ Could not navigate to profile for {name}, using card data only"
                                    )

                            except Exception as profile_error:
                                logger.warning(
                                    f"Failed to get profile data for {name}: {profile_error}"
                                )

                        # Save attendee data if we have it
                        if attendee_data:
                            # Save to CSV immediately after extraction
                            try:
                                self.csv_manager.append_to_csv(
                                    csv_filepath, attendee_data.to_dict()
                                )
                                total_scraped += 1
                                batch_scraped += 1
                                existing_names.add(name.lower().strip())
                                logger.info(
                                    f"✅ Successfully saved attendee: {name} (Total: {total_scraped})"
                                )
                            except Exception as save_error:
                                logger.error(
                                    f"❌ Error saving attendee {name} to CSV: {save_error}"
                                )
                                # Continue processing even if save fails
                        else:
                            logger.warning(
                                f"⚠️ Failed to extract data for attendee: {name}"
                            )

                        # Move to next attendee
                        processed_attendees += 1

                        # Delay between requests to be respectful and human-like
                        delay = settings.delay_between_requests + (
                            (attendee_count % 3) * 0.5
                        )
                        await asyncio.sleep(delay)

                    except Exception as e:
                        logger.error(
                            f"Error processing attendee #{attendee_count}: {str(e)}"
                        )
                        processed_attendees += 1
                        continue

                logger.info(
                    f"Batch {scroll_attempts} completed: {batch_scraped} new attendees scraped"
                )

                # Perform scrolling in the ng-scroll-layer container to load more content
                logger.info("🔄 Scrolling to load more attendees...")

                try:
                    # Enhanced scrolling logic with multiple container detection
                    scroll_result = await self.page.evaluate(
                        """
                        (function() {
                            // Try multiple container selectors in order of preference
                            const containerSelectors = [
                                '.ng-scroll-layer',
                                '.attendees-wrapper',
                                '.attendees-grid',
                                '.attendees-inner',
                                '.ng-scroll-content',
                                '[class*="attendees"]',
                                '[class*="scroll"]'
                            ];
                            
                            let scrolledContainer = null;
                            let scrollInfo = {};
                            
                            // First, try to find a scrollable container
                            for (const selector of containerSelectors) {
                                const containers = document.querySelectorAll(selector);
                                for (const container of containers) {
                                    if (container && container.scrollHeight > container.clientHeight) {
                                        const beforeScrollTop = container.scrollTop;
                                        const clientHeight = container.clientHeight;
                                        const scrollDistance = Math.max(clientHeight * 1.5, 800); // At least 800px
                                        
                                        // Focus the container
                                        container.focus();
                                        
                                        // Method 1: Direct scrollTop change
                                        container.scrollTop += scrollDistance;
                                        
                                        // Method 2: scrollBy if available
                                        if (container.scrollBy) {
                                            container.scrollBy(0, scrollDistance);
                                        }
                                        
                                        // Method 3: Dispatch wheel events
                                        const wheelEvent = new WheelEvent('wheel', {
                                            deltaY: scrollDistance,
                                            deltaMode: 0,
                                            bubbles: true,
                                            cancelable: true
                                        });
                                        container.dispatchEvent(wheelEvent);
                                        
                                        // Method 4: Dispatch scroll event
                                        const scrollEvent = new Event('scroll', { bubbles: true });
                                        container.dispatchEvent(scrollEvent);
                                        
                                        const afterScrollTop = container.scrollTop;
                                        
                                        scrollInfo = {
                                            selector: selector,
                                            scrolled: afterScrollTop > beforeScrollTop,
                                            beforeScrollTop: beforeScrollTop,
                                            afterScrollTop: afterScrollTop,
                                            scrollHeight: container.scrollHeight,
                                            clientHeight: clientHeight,
                                            scrollDistance: scrollDistance
                                        };
                                        
                                        if (afterScrollTop > beforeScrollTop) {
                                            scrolledContainer = selector;
                                            break;
                                        }
                                    }
                                }
                                if (scrolledContainer) break;
                            }
                            
                            // If no container scrolled, try window scrolling
                            if (!scrolledContainer) {
                                const beforeY = window.pageYOffset;
                                const scrollDistance = Math.max(window.innerHeight * 1.5, 800);
                                
                                window.scrollBy(0, scrollDistance);
                                
                                const afterY = window.pageYOffset;
                                
                                scrollInfo = {
                                    selector: 'window',
                                    scrolled: afterY > beforeY,
                                    beforeScrollTop: beforeY,
                                    afterScrollTop: afterY,
                                    scrollDistance: scrollDistance
                                };
                                
                                if (afterY > beforeY) {
                                    scrolledContainer = 'window';
                                }
                            }
                            
                            return {
                                success: !!scrolledContainer,
                                container: scrolledContainer,
                                info: scrollInfo
                            };
                        })();
                    """
                    )

                    if scroll_result["success"]:
                        container = scroll_result["container"]
                        info = scroll_result["info"]
                        logger.info(f"✅ Successfully scrolled in {container}")
                        logger.info(
                            f"   Scroll: {info['beforeScrollTop']} → {info['afterScrollTop']} (+{info['afterScrollTop'] - info['beforeScrollTop']}px)"
                        )
                    else:
                        logger.warning(
                            "⚠️ No scrollable container found or scroll failed"
                        )
                        # Force scroll attempt on the page
                        await self.page.keyboard.press("PageDown")
                        await asyncio.sleep(0.5)
                        await self.page.keyboard.press("PageDown")
                        logger.info("📄 Attempted PageDown as fallback")

                except Exception as scroll_error:
                    logger.warning(f"Error during scrolling: {scroll_error}")

                # Wait for new content to load
                await asyncio.sleep(3)

                # Check for loading indicators and wait for them to disappear
                try:
                    loading_selectors = [
                        SBCSelectors.LOADING_SPINNER,
                        ".loading",
                        ".spinner",
                        ".loader",
                        "[class*='loading']",
                        "[class*='spinner']",
                    ]

                    for selector in loading_selectors:
                        try:
                            await self.page.wait_for_selector(
                                selector, state="hidden", timeout=5000
                            )
                            logger.info(
                                f"Loading indicator {selector} disappeared"
                            )
                        except Exception:
                            continue

                except Exception as loading_error:
                    logger.debug(f"Loading check completed: {loading_error}")

                # Additional wait for content to stabilize
                await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Error in scrolling scrape: {str(e)}")

        logger.info(
            f"Scrolling scrape completed after {scroll_attempts} attempts"
        )
        logger.info(f"Total new attendees scraped: {total_scraped}")
        return total_scraped

    async def scrape_visible_attendees(
        self, page_url: str, csv_filepath: str
    ) -> int:
        """Scrape attendees from currently visible batch without scrolling."""
        return await self.scroll_and_scrape_attendees(page_url, csv_filepath)

    async def handle_pagination(self, base_url: str, csv_filepath: str) -> int:
        """Handle pagination and scrape all pages."""
        if not self.page:
            logger.error("Browser page not initialized")
            return 0

        # Use scrolling method
        return await self.scroll_and_scrape_attendees(base_url, csv_filepath)


async def scrape_attendees(page_url: str, csv_filepath: str) -> int:
    """
    Main function to scrape attendees from SBC website with scrolling support.

    Args:
        page_url: URL of the SBC attendees page
        csv_filepath: Path to save the CSV file

    Returns:
        int: Number of new attendees scraped
    """
    logger.info("Starting SBC attendees scraper with scrolling support")

    # Validate URL format
    if not page_url.startswith(("http://", "https://")):
        logger.error(f"Invalid URL format: {page_url}")
        return 0

    scraper = None
    try:
        scraper = SBCScraper()
        async with scraper:
            # Login to the website
            if not await scraper.login():
                logger.error("Failed to login to SBC website")
                return 0

            logger.info(
                "Login successful, starting scrolling scraping process"
            )

            # Scrape attendees with scrolling
            total_scraped = await scraper.scroll_and_scrape_attendees(
                page_url, csv_filepath
            )

            logger.info(
                f"Scrolling scraping completed. Total new attendees scraped: {total_scraped}"
            )
            return total_scraped

    except Exception as e:
        logger.error(f"Error in scrape_attendees: {str(e)}")
        return 0
    finally:
        # Ensure clean shutdown with extra time
        if scraper:
            try:
                await scraper.close_browser()
            except Exception as e:
                logger.warning(f"Error in final cleanup: {e}")

        # Give extra time for all async operations to complete
        try:
            await asyncio.sleep(2)
        except Exception:
            pass

        logger.info("Scraper shutdown completed")
