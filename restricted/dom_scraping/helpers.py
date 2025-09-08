"""Helper functions and selectors for SBC scraper."""

from dataclasses import dataclass
from typing import Dict, List, Optional
import csv
import os
from urllib.parse import urljoin, urlparse


@dataclass
class AttendeeData:
    """Data structure for attendee information."""

    full_name: str
    company_name: str = ""
    position: str = ""
    linkedin_url: str = ""
    facebook_url: str = ""
    x_twitter_url: str = ""
    other_socials: str = ""
    country: str = ""
    responsibility: str = ""
    gaming_vertical: str = ""
    organization_type: str = ""
    introduction: str = ""
    source_url: str = ""
    profile_image_url: str = ""

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for CSV writing."""
        return {
            "full_name": self.full_name,
            "company_name": self.company_name,
            "position": self.position,
            "linkedin_url": self.linkedin_url,
            "facebook_url": self.facebook_url,
            "x_twitter_url": self.x_twitter_url,
            "other_socials": self.other_socials,
            "country": self.country,
            "responsibility": self.responsibility,
            "gaming_vertical": self.gaming_vertical,
            "organization_type": self.organization_type,
            "introduction": self.introduction,
            "source_url": self.source_url,
            "profile_image_url": self.profile_image_url,
        }


class SBCSelectors:
    """CSS selectors for SBC website elements."""

    # Login page selectors
    LOGIN_USERNAME_INPUT = "#email"
    LOGIN_PASSWORD_INPUT = "#password"
    LOGIN_NEXT_BUTTON = "#page-top > app-root > app-home > div > div > app-login > app-login-step1 > app-login-box > div > div > form > button"
    LOGIN_SUBMIT_BUTTON = "#page-top > app-root > app-home > div > div > app-login > app-login-step2-with-password > app-login-box > div > div > form > div:nth-child(3) > button"

    # Attendees page selectors - Updated based on actual HTML structure
    ATTENDEES_LIST_CONTAINER = "[class*='attendees-grid']"

    ATTENDEE_ITEM = "app-attendee-item.ng-star-inserted"
    ATTENDEE_NAME = ".name"

    # Meeting/Profile action buttons
    MEETING_REQUEST_BUTTON = (
        "button.mat-ripple.profile-widget-actions-btn.ng-star-inserted"
    )
    ATTENDEE_ACTIONS_SECTION = ".attendee-actions"

    # Attendee detail page selectors - Updated based on actual HTML structure
    PROFILE_NAME = (
        ".name.fw-600, h1, .profile-name, .attendee-name, .full-name, .name"
    )
    PROFILE_COMPANY = (
        ".fw-600.overflow-hidden.text-overflow-ellipsis.text-nowrap:nth-of-type(3), "
        ".company, .company-name, .organization"
    )
    PROFILE_POSITION = (
        ".overflow-hidden.text-overflow-ellipsis.text-nowrap:not(.name):not(.fw-600), "
        ".position, .title, .job-title"
    )
    PROFILE_COUNTRY = (
        ".details-item:has(h5:contains('Country')) .field-value, "
        ".country, .location"
    )
    PROFILE_RESPONSIBILITY = (
        ".details-item:has(h5:contains('Area of responsibility')) .field-value, "
        ".details-item:has(h5:contains('responsibility')) .field-value, "
        ".responsibility, .role"
    )
    PROFILE_GAMING_VERTICAL = (
        ".details-item:has(h5:contains('Main gaming vertical')) .field-value, "
        ".details-item:has(h5:contains('gaming vertical')) .field-value, "
        ".gaming-vertical, .vertical, .industry"
    )
    PROFILE_ORGANIZATION_TYPE = (
        ".details-item:has(h5:contains('Organisation type')) .field-value, "
        ".details-item:has(h5:contains('organization type')) .field-value, "
        ".organization-type, .company-type"
    )
    PROFILE_INTRODUCTION = (
        ".details-item--introduction .field-value, "
        ".details-item:has(h5:contains('Introduction')) .field-value, "
        ".introduction, .bio, .about, .description"
    )
    PROFILE_IMAGE = "#page-top > app-root > app-event > div > div > div > app-autofetch-person-profile > div > app-person-profile > div > div.profile-main-row.d-lg-flex.align-items-end.justify-content-between > div.profile-main-info > div.avatar-wrapper > app-circle-avatar > div > div > div > img"

    # Social media container - dynamically loaded by JS
    SOCIAL_MEDIA_CONTAINER = "#page-top > app-root > app-event > div > div > div > app-autofetch-person-profile > div > app-person-profile > div > div.profile-detail-row > div:nth-child(1) > div.d-flex.justify-content-center.justify-content-lg-start > div"
    SOCIAL_MEDIA_BUTTONS = "button"  # Buttons inside the social container

    # Navigation and pagination
    NEXT_PAGE_BUTTON = ".next, .next-page, .pagination-next"
    LOAD_MORE_BUTTON = ".load-more, .show-more"

    # Loading indicators
    LOADING_SPINNER = ".spinner, .loading, .loader"


class CSVManager:
    """Manages CSV file operations."""

    @staticmethod
    def create_csv_if_not_exists(filepath: str, fieldnames: List[str]) -> None:
        """Create CSV file with headers if it doesn't exist."""
        if not os.path.exists(filepath):
            with open(filepath, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()

    @staticmethod
    def read_existing_names(
        filepath: str, name_column: str = "full_name"
    ) -> set:
        """Read existing names from CSV file."""
        existing_names = set()
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if name_column in row and row[name_column]:
                        existing_names.add(row[name_column].strip().lower())
        return existing_names

    @staticmethod
    def read_existing_name_company_pairs(filepath: str) -> set:
        """Read existing name-company pairs from CSV file for advanced duplicate checking."""
        existing_pairs = set()
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if "full_name" in row and row["full_name"]:
                        full_name = row["full_name"].strip().lower()
                        company_name = ""
                        if "company_name" in row and row["company_name"]:
                            company_name = row["company_name"].strip().lower()
                        
                        # Create a tuple pair for duplicate checking
                        pair = (full_name, company_name)
                        existing_pairs.add(pair)
        return existing_pairs

    @staticmethod
    def is_duplicate_attendee(full_name: str, company_name: str, existing_pairs: set) -> bool:
        """Check if attendee is a duplicate based on name-company pair."""
        if not full_name:
            return False
        
        # Normalize the input data
        normalized_name = full_name.strip().lower()
        normalized_company = ""
        if company_name:
            normalized_company = company_name.strip().lower()
        
        # Create the pair to check
        check_pair = (normalized_name, normalized_company)
        
        return check_pair in existing_pairs

    @staticmethod
    def append_to_csv(filepath: str, data: Dict[str, str]) -> None:
        """Append data to CSV file."""
        fieldnames = list(data.keys())
        file_exists = os.path.exists(filepath)

        with open(filepath, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)


class URLHelper:
    """Helper functions for URL manipulation."""

    @staticmethod
    def normalize_url(url: str, base_url: str = "") -> str:
        """Normalize and clean URL."""
        if not url:
            return ""

        # Remove whitespace
        url = url.strip()

        # If relative URL, make it absolute
        if url.startswith("/") and base_url:
            url = urljoin(base_url, url)

        # Ensure proper protocol
        if url and not url.startswith(("http://", "https://")):
            url = "https://" + url

        return url

    @staticmethod
    def is_valid_social_url(url: str, platform: str) -> bool:
        """Check if URL is a valid social media URL for the given platform."""
        if not url:
            return False

        domain_mapping = {
            "linkedin": "linkedin.com",
            "facebook": "facebook.com",
            "twitter": ["twitter.com", "x.com"],
        }

        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc.replace("www.", "")

            expected_domains = domain_mapping.get(platform, [])
            if isinstance(expected_domains, str):
                expected_domains = [expected_domains]

            return any(
                domain.endswith(expected_domain)
                for expected_domain in expected_domains
            )
        except:
            return False


def clean_text(text: str) -> str:
    """Clean and normalize text data."""
    if not text:
        return ""

    # Remove extra whitespace and newlines
    text = " ".join(text.split())

    # Remove common unwanted characters
    text = text.replace("\u00a0", " ")  # Non-breaking space
    text = text.replace("\u200b", "")  # Zero-width space

    return text.strip()


def get_attendee_csv_fieldnames() -> List[str]:
    """Get fieldnames for attendee CSV file."""
    return [
        "full_name",
        "company_name",
        "position",
        "linkedin_url",
        "facebook_url",
        "x_twitter_url",
        "other_socials",
        "country",
        "responsibility",
        "gaming_vertical",
        "organization_type",
        "introduction",
        "source_url",
        "profile_image_url",
    ]
