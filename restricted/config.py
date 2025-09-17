"""Configuration settings for SBC scraper using Pydantic."""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
from dotenv import load_dotenv
import os

# Load .env from the same directory as this config file (restricted directory)
config_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(config_dir, ".env")
load_dotenv(env_path)


class Settings(BaseSettings):
    """Settings class for SBC scraper configuration."""

    # Account Credentials - Scraper Account
    scraper_username: str = Field(default="", env="SCRAPER_USERNAME")
    scraper_password: str = Field(default="", env="SCRAPER_PASSWORD")
    scraper_user_id: str = Field(default="", env="SCRAPER_USER_ID")

    # Account Credentials - Messenger Account 1
    messenger1_username: str = Field(default="", env="MESSENGER1_USERNAME")
    messenger1_password: str = Field(default="", env="MESSENGER1_PASSWORD")
    messenger1_user_id: str = Field(default="", env="MESSENGER1_USER_ID")

    # Account Credentials - Messenger Account 2
    messenger2_username: str = Field(default="", env="MESSENGER2_USERNAME")
    messenger2_password: str = Field(default="", env="MESSENGER2_PASSWORD")
    messenger2_user_id: str = Field(default="", env="MESSENGER2_USER_ID")

    # Account Credentials - Messenger Account 3
    messenger3_username: str = Field(default="", env="MESSENGER3_USERNAME")
    messenger3_password: str = Field(default="", env="MESSENGER3_PASSWORD")
    messenger3_user_id: str = Field(default="", env="MESSENGER3_USER_ID")

    # Account Credentials - Affiliate Account
    affiliate_username: str = Field(default="", env="AFFILIATE_USERNAME")
    affiliate_password: str = Field(default="", env="AFFILIATE_PASSWORD")
    affiliate_user_id: str = Field(default="", env="AFFILIATE_USER_ID")

    # URLs
    sbc_login_url: str = Field(
        default="https://sbcconnect.com", env="SBC_LOGIN_URL"
    )
    sbc_attendees_url: str = Field(default="", env="SBC_ATTENDEES_URL")
    sbc_companies_url: str = Field(default="", env="SBC_COMPANIES_URL")
    sbc_exhibitors_url: str = Field(default="", env="SBC_EXHIBITORS_URL")

    # Output files
    attendees_csv_path: str = Field(
        default="restricted/data/attendees.csv", env="ATTENDEES_CSV_PATH"
    )
    companies_csv_path: str = Field(
        default="restricted/data/companies.csv", env="COMPANIES_CSV_PATH"
    )
    exhibitors_csv_path: str = Field(
        default="restricted/data/exhibitors.csv", env="EXHIBITORS_CSV_PATH"
    )

    # Browser settings
    headless: bool = Field(default=True, env="HEADLESS")
    browser_timeout: int = Field(default=30000, env="BROWSER_TIMEOUT")
    delay_between_requests: int = Field(
        default=2, env="DELAY_BETWEEN_REQUESTS"
    )

    class Config:
        env_file = os.path.join(config_dir, ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra fields like proxy_server


# Global settings instance
settings = Settings()
