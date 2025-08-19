import re
import logging
from typing import List, Dict, Optional, Union, Any

# Setup logger
logger = logging.getLogger(__name__)


class SBCSelectors:
    """Specific selectors for SBC (Smart Business Connect) websites"""

    # SBC-specific company container
    COMPANY_CONTAINER_SELECTORS = [
        "sbc-exhibitor-entry",  # Individual company entries
        ".sbc-exhibitor-entry",
        ".exhibitor-entry",
        ".exhibitor-card",
        "div[class*='exhibitor']",
    ]

    # SBC-specific name selectors
    NAME_SELECTORS = [
        ".sbc-exhibitor-entry h3",
        ".exhibitor-name",
        ".exhibitor-title",
        "h3",
        "h2",
    ]

    # SBC-specific description selectors
    DESCRIPTION_SELECTORS = [
        ".sbc-exhibitor-entry .description",
        ".sbc-exhibitor-entry p",
        ".exhibitor-description",
        "p:not(.exhibitor-name):not(.exhibitor-title)",
    ]

    # SBC-specific logo selectors
    LOGO_SELECTORS = [
        ".sbc-exhibitor-entry img",
        ".exhibitor-logo img",
        ".exhibitor-image img",
        "img[src*='exhibitor']",
        "img[alt*='logo']",
    ]

    # SBC-specific additional info selectors
    STAND_NUMBER_SELECTORS = [
        ".stand-number",
        ".booth-number",
        "[class*='stand']",
        "text()[contains(., 'Stand No')]",
    ]

    CATEGORY_SELECTORS = [
        # SBC-specific category selectors based on the HTML structure
        ".ed-entry__category span",  # Category spans
        "span[class*='ng-tns-c32-']:contains('#')",  # Spans containing category hashtags
        ".category",
        ".exhibitor-category",
        ".business-type",
        "[class*='category']",
    ]


class CompanySelectors:
    """Universal selectors for company attributes across different websites"""

    # Company name selectors (including SBC-specific ones)
    NAME_SELECTORS = [
        # SBC-specific selectors - more precise for the actual structure
        ".ed-entry__name span:not(:contains('Stand'))",  # Company name spans, excluding stand numbers
        ".ed-entry__name span",  # All spans in name section (will filter out stand numbers in code)
        ".ed-entry__name",
        "div._ngcontent-iun-c32[class*='ed-entry__name'] span",
        # Original SBC selectors
        ".sbc-exhibitor-entry h3",
        ".sbc-exhibitor-entry .exhibitor-name",
        ".exhibitor-name",
        ".exhibitor-title",
        # Universal selectors
        "h1",
        "h2",
        "h3",
        ".company-name",
        ".business-name",
        ".name",
        ".title",
        '[data-testid*="name"]',
        '[data-cy*="name"]',
        ".listing-title",
        ".company-title",
        ".business-title",
        'a[href*="company"]',
        'a[href*="business"]',
        ".card-title",
        ".item-title",
        ".entry-title",
        '[class*="name"]',
        '[class*="title"]',
        '[id*="name"]',
        '[id*="title"]',
    ]

    # Description selectors
    DESCRIPTION_SELECTORS = [
        # SBC-specific selectors
        ".sbc-exhibitor-entry .description",
        ".sbc-exhibitor-entry p",
        ".exhibitor-description",
        # Universal selectors
        ".description",
        ".bio",
        ".about",
        ".summary",
        '[data-testid*="description"]',
        '[data-cy*="description"]',
        ".company-description",
        ".business-description",
        ".excerpt",
        ".content",
        ".details",
        "p",
        ".text",
        ".info",
        '[class*="description"]',
        '[class*="about"]',
        '[class*="summary"]',
        '[class*="bio"]',
    ]

    # Phone selectors
    PHONE_SELECTORS = [
        ".phone",
        ".tel",
        ".telephone",
        '[data-testid*="phone"]',
        '[data-cy*="phone"]',
        'a[href^="tel:"]',
        '[href^="tel:"]',
        ".contact-phone",
        ".phone-number",
        '[class*="phone"]',
        '[class*="tel"]',
        '[title*="phone"]',
        '[aria-label*="phone"]',
    ]

    # Email selectors
    EMAIL_SELECTORS = [
        ".email",
        ".mail",
        '[data-testid*="email"]',
        '[data-cy*="email"]',
        'a[href^="mailto:"]',
        '[href^="mailto:"]',
        ".contact-email",
        ".email-address",
        '[class*="email"]',
        '[class*="mail"]',
        '[title*="email"]',
        '[aria-label*="email"]',
    ]

    # Logo selectors
    LOGO_SELECTORS = [
        # SBC-specific selectors
        ".sbc-exhibitor-entry img",
        ".exhibitor-logo img",
        ".exhibitor-image img",
        # Universal selectors
        "img.logo",
        ".logo img",
        ".company-logo img",
        '[data-testid*="logo"] img',
        '[data-cy*="logo"] img',
        ".avatar img",
        ".company-avatar img",
        'img[alt*="logo"]',
        'img[alt*="company"]',
        'img[src*="logo"]',
        'img[class*="logo"]',
        ".brand img",
        ".company-image img",
    ]

    # Social media selectors (for fallback when SBC container not found)
    SOCIAL_SELECTORS = [
        # Generic social link selectors
        'a[href*="facebook.com"]',
        'a[href*="fb.com"]',
        'a[href*="instagram.com"]',
        'a[href*="instagr.am"]',
        'a[href*="linkedin.com"]',
        'a[href*="twitter.com"]',
        'a[href*="x.com"]',
        'a[href*="youtube.com"]',
        'a[href*="tiktok.com"]',
        'a[href*="snapchat.com"]',
        'a[href*="pinterest.com"]',
        'a[href*="telegram.org"]',
        'a[href*="whatsapp.com"]',
        'a[href*="discord.com"]',
        'a[href*="reddit.com"]',
        # Generic social selectors
        '[data-testid*="social"]',
        '[data-cy*="social"]',
        ".social-links a",
        ".social-media a",
        ".socials a",
        '[class*="social"] a',
        '[aria-label*="social"]',
    ]

    # Pagination selectors
    PAGINATION_SELECTORS = [
        # SBC-specific selectors
        "div.show-more-btn.btn.ng-star-inserted",
        ".show-more-btn.btn.ng-star-inserted",
        ".show-more-btn",
        ".load-more-btn",
        ".exhibitor-load-more",
        ".btn-load-more",
        # Universal selectors
        'button:has-text("Show more")',
        'button:has-text("Load more")',
        'button:has-text("More")',
        'button:has-text("See more")',
        ".show-more",
        ".load-more",
        ".more-button",
        '[data-testid*="load"]',
        '[data-cy*="load"]',
        'button[class*="more"]',
        'button[class*="load"]',
        ".pagination-next",
        ".next-page",
        'a:has-text("Next")',
        'button:has-text("Next")',
    ]

    # Company container selectors
    COMPANY_CONTAINER_SELECTORS = [
        # SBC-specific selectors (most specific first)
        "sbc-exhibitor-entry",  # Individual company entries within the main container
        ".sbc-exhibitor-entry",
        ".exhibitor-entry",
        ".exhibitor-card",
        # Universal selectors
        ".company",
        ".business",
        ".listing",
        ".card",
        ".item",
        ".entry",
        '[data-testid*="company"]',
        '[data-cy*="company"]',
        ".company-card",
        ".business-card",
        ".result",
        ".search-result",
        '[class*="company"]',
        '[class*="business"]',
        '[class*="listing"]',
        '[class*="card"]',
    ]


def clean_text(text: str) -> str:
    """Clean and normalize extracted text"""
    if not text:
        return ""

    # Remove extra whitespace and newlines
    text = re.sub(r"\s+", " ", text.strip())

    # Remove special characters that might break CSV
    text = re.sub(r'["\n\r\t]', " ", text)

    return text


def clean_company_name(text: str) -> str:
    """Clean company name and filter out stand numbers"""
    if not text:
        return ""

    # Clean the text first
    text = clean_text(text)

    # Filter out stand numbers and related text
    if re.search(
        r"(stand\s*no\.?:?\s*[a-z]?\d+|booth\s*no\.?:?\s*[a-z]?\d+)",
        text,
        re.IGNORECASE,
    ):
        return ""  # Return empty if this looks like a stand number

    # Filter out standalone numbers that might be stand numbers
    if re.match(r"^[a-z]?\d+$", text.strip(), re.IGNORECASE):
        return ""

    # Filter out common stand-related phrases
    stand_patterns = [
        r"^stand\s*no\.?:?",
        r"^booth\s*no\.?:?",
        r"^hall\s*\d+",
        r"^pavilion\s*\d+",
    ]

    for pattern in stand_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return ""

    return text


def extract_phone(text: str) -> str:
    """Extract phone number from text"""
    if not text:
        return ""

    # Phone number patterns
    phone_patterns = [
        r"\+?\d{1,4}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}",
        r"\(\d{3}\)\s?\d{3}-\d{4}",
        r"\d{3}-\d{3}-\d{4}",
        r"\d{10,}",
    ]

    for pattern in phone_patterns:
        match = re.search(pattern, text)
        if match:
            return clean_text(match.group())

    return clean_text(text)


def extract_email(text: str) -> str:
    """Extract email from text"""
    if not text:
        return ""

    # Email pattern
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    match = re.search(email_pattern, text)

    if match:
        return match.group()

    return clean_text(text)


def extract_url(text: str, href: Optional[str] = None) -> str:
    """Extract URL from text or href attribute"""
    if href and href.startswith("http"):
        return href

    if not text:
        return ""

    # URL pattern
    url_pattern = r'https?://[^\s<>"\']+|www\.[^\s<>"\']+|\b[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?'
    match = re.search(url_pattern, text)

    if match:
        url = match.group()
        if not url.startswith("http"):
            url = "http://" + url
        return url

    return ""


def get_element_text_or_attribute(
    element: Any, attribute: Optional[str] = None
) -> str:
    """Get text content or attribute value from element"""
    try:
        if attribute:
            # For async elements, we need to handle this differently
            if hasattr(element, "get_attribute"):
                return element.get_attribute(attribute) or ""
            else:
                return ""
        else:
            if hasattr(element, "inner_text"):
                return element.inner_text() or ""
            else:
                return ""
    except:
        return ""


async def get_element_text_or_attribute_async(
    element: Any, attribute: Optional[str] = None
) -> str:
    """Get text content or attribute value from element (async version)"""
    try:
        if attribute:
            value = await element.get_attribute(attribute)
            return value or ""
        else:
            text = await element.inner_text()
            return text or ""
    except:
        return ""


async def try_selectors_async(
    page: Any, selectors: List[str], container: Optional[Any] = None
) -> List[Any]:
    """Try multiple selectors and return found elements (async version)"""
    elements = []

    for selector in selectors:
        try:
            if container:
                found = await container.locator(selector).all()
            else:
                found = await page.locator(selector).all()

            if found:
                elements.extend(found)
        except:
            continue

    return elements


def try_selectors(
    page: Any, selectors: List[str], container: Optional[Any] = None
) -> List[Any]:
    """Try multiple selectors and return found elements (sync version for compatibility)"""
    elements = []

    for selector in selectors:
        try:
            if container:
                found = container.locator(selector).all()
            else:
                found = page.locator(selector).all()

            if found:
                elements.extend(found)
        except:
            continue

    return elements


async def find_company_containers_async(page: Any) -> List[Any]:
    """Find all company containers on the page (async version)"""
    # First try to find SBC main container and get companies from within it
    try:
        main_container = await page.locator(
            "div.exhibitors-directory-element.ng-star-inserted > div.ed-body"
        ).first()
        if main_container:
            # Look for individual company entries within the main container
            containers = await main_container.locator(
                "sbc-exhibitor-entry"
            ).all()
            if containers:
                return containers
    except:
        pass

    # Fallback to universal selectors
    containers = await try_selectors_async(
        page, CompanySelectors.COMPANY_CONTAINER_SELECTORS
    )

    # If no specific containers found, try to find any elements that might contain company info
    if not containers:
        # Look for elements that contain typical company information
        containers = (
            await page.locator("div, article, section, li")
            .filter(
                has_text=re.compile(
                    r"(company|business|phone|email)", re.IGNORECASE
                )
            )
            .all()
        )

    return containers


def find_company_containers(page: Any) -> List[Any]:
    """Find all company containers on the page"""
    # First try to find SBC main container and get companies from within it
    try:
        main_container = page.locator(
            "div.exhibitors-directory-element.ng-star-inserted > div.ed-body"
        ).first()
        if main_container:
            # Look for individual company entries within the main container
            containers = main_container.locator("sbc-exhibitor-entry").all()
            if containers:
                return containers
    except:
        pass

    # Fallback to universal selectors
    containers = try_selectors(
        page, CompanySelectors.COMPANY_CONTAINER_SELECTORS
    )

    # If no specific containers found, try to find any elements that might contain company info
    if not containers:
        # Look for elements that contain typical company information
        containers = (
            page.locator("div, article, section, li")
            .filter(
                has_text=re.compile(
                    r"(company|business|phone|email)", re.IGNORECASE
                )
            )
            .all()
        )

    return containers


async def extract_social_links_async(
    container: Any, page: Any
) -> Dict[str, str]:
    """Extract all social media links (async version)"""
    socials = {
        "facebook": "",
        "instagram": "",
        "linkedin": "",
        "twitter": "",
        "other_socials": "",
    }

    # First check for SBC-specific social container
    sbc_social_container = None
    try:
        sbc_social_containers = await container.locator(
            "div.ed-entry__socials.ng-tns-c32-1.ng-star-inserted, .ed-entry__socials"
        ).all()
        if sbc_social_containers:
            sbc_social_container = sbc_social_containers[0]
    except:
        pass

    if sbc_social_container:
        # Extract all social links from SBC container
        try:
            all_social_links = await sbc_social_container.locator("a").all()
            other_socials = []

            for link in all_social_links:
                href = await get_element_text_or_attribute_async(link, "href")
                if not href:
                    continue

                href_lower = href.lower()

                # Categorize social links
                if "facebook.com" in href_lower or "fb.com" in href_lower:
                    socials["facebook"] = href
                elif (
                    "instagram.com" in href_lower or "instagr.am" in href_lower
                ):
                    socials["instagram"] = href
                elif "linkedin.com" in href_lower:
                    socials["linkedin"] = href
                elif "twitter.com" in href_lower or "x.com" in href_lower:
                    socials["twitter"] = href
                else:
                    # Other social platforms
                    if any(
                        platform in href_lower
                        for platform in [
                            "youtube.",
                            "tiktok.",
                            "snapchat.",
                            "pinterest.",
                            "telegram.",
                            "whatsapp.",
                            "discord.",
                            "reddit.",
                        ]
                    ):
                        other_socials.append(href)

            if other_socials:
                socials["other_socials"] = "; ".join(other_socials)

        except Exception as e:
            logger.debug(f"Error extracting from SBC social container: {e}")
    else:
        # Fallback to universal selectors - get all social links and categorize them
        social_elements = await try_selectors_async(
            page, CompanySelectors.SOCIAL_SELECTORS, container
        )

        other_socials = []

        for element in social_elements:
            href = await get_element_text_or_attribute_async(element, "href")
            if not href:
                continue

            href_lower = href.lower()

            # Categorize social links
            if "facebook.com" in href_lower or "fb.com" in href_lower:
                if not socials["facebook"]:  # Only set if not already found
                    socials["facebook"] = href
            elif "instagram.com" in href_lower or "instagr.am" in href_lower:
                if not socials["instagram"]:
                    socials["instagram"] = href
            elif "linkedin.com" in href_lower:
                if not socials["linkedin"]:
                    socials["linkedin"] = href
            elif "twitter.com" in href_lower or "x.com" in href_lower:
                if not socials["twitter"]:
                    socials["twitter"] = href
            else:
                # Other social platforms
                if any(
                    platform in href_lower
                    for platform in [
                        "youtube.",
                        "tiktok.",
                        "snapchat.",
                        "pinterest.",
                        "telegram.",
                        "whatsapp.",
                        "discord.",
                        "reddit.",
                    ]
                ):
                    other_socials.append(href)

        if other_socials:
            socials["other_socials"] = "; ".join(other_socials)

    return socials


def extract_social_links(container: Any, page: Any) -> Dict[str, str]:
    """Extract all social media links"""
    socials = {
        "facebook": "",
        "instagram": "",
        "linkedin": "",
        "twitter": "",
        "other_socials": "",
    }

    # First check for SBC-specific social container
    sbc_social_container = None
    try:
        sbc_social_containers = container.locator(
            "div.ed-entry__socials.ng-tns-c32-1.ng-star-inserted, .ed-entry__socials"
        ).all()
        if sbc_social_containers:
            sbc_social_container = sbc_social_containers[0]
    except:
        pass

    if sbc_social_container:
        # Extract all social links from SBC container
        try:
            all_social_links = sbc_social_container.locator("a").all()
            other_socials = []

            for link in all_social_links:
                href = get_element_text_or_attribute(link, "href")
                if not href:
                    continue

                href_lower = href.lower()

                # Categorize social links
                if "facebook.com" in href_lower or "fb.com" in href_lower:
                    socials["facebook"] = href
                elif (
                    "instagram.com" in href_lower or "instagr.am" in href_lower
                ):
                    socials["instagram"] = href
                elif "linkedin.com" in href_lower:
                    socials["linkedin"] = href
                elif "twitter.com" in href_lower or "x.com" in href_lower:
                    socials["twitter"] = href
                else:
                    # Other social platforms
                    if any(
                        platform in href_lower
                        for platform in [
                            "youtube.",
                            "tiktok.",
                            "snapchat.",
                            "pinterest.",
                            "telegram.",
                            "whatsapp.",
                            "discord.",
                            "reddit.",
                        ]
                    ):
                        other_socials.append(href)

            if other_socials:
                socials["other_socials"] = "; ".join(other_socials)

        except Exception as e:
            logger.debug(f"Error extracting from SBC social container: {e}")
    else:
        # Fallback to universal selectors - get all social links and categorize them
        social_elements = try_selectors(
            page, CompanySelectors.SOCIAL_SELECTORS, container
        )

        other_socials = []

        for element in social_elements:
            href = get_element_text_or_attribute(element, "href")
            if not href:
                continue

            href_lower = href.lower()

            # Categorize social links
            if "facebook.com" in href_lower or "fb.com" in href_lower:
                if not socials["facebook"]:  # Only set if not already found
                    socials["facebook"] = href
            elif "instagram.com" in href_lower or "instagr.am" in href_lower:
                if not socials["instagram"]:
                    socials["instagram"] = href
            elif "linkedin.com" in href_lower:
                if not socials["linkedin"]:
                    socials["linkedin"] = href
            elif "twitter.com" in href_lower or "x.com" in href_lower:
                if not socials["twitter"]:
                    socials["twitter"] = href
            else:
                # Other social platforms
                if any(
                    platform in href_lower
                    for platform in [
                        "youtube.",
                        "tiktok.",
                        "snapchat.",
                        "pinterest.",
                        "telegram.",
                        "whatsapp.",
                        "discord.",
                        "reddit.",
                    ]
                ):
                    other_socials.append(href)

        if other_socials:
            socials["other_socials"] = "; ".join(other_socials)

    return socials
