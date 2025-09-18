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

    # Proxy Settings
    proxy_enabled: bool = Field(default=True, env="PROXY_ENABLED")
    proxy_server: str = Field(default="http://93.115.200.159:8001", env="PROXY_SERVER")
    proxy_username: str = Field(default="yevheniir", env="PROXY_USERNAME")
    proxy_password: str = Field(default="Test_proxy_1", env="PROXY_PASSWORD")

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
    
    def get_proxy_config(self) -> Optional[dict]:
        """Get proxy configuration dictionary"""
        if self.proxy_enabled and self.proxy_server:
            return {
                "server": self.proxy_server,
                "username": self.proxy_username,
                "password": self.proxy_password
            }
        return None
    
    def get_account_config(self, account_type: str = "scraper") -> dict:
        """Get account configuration dictionary"""
        account_mapping = {
            "scraper": {
                "username": self.scraper_username,
                "password": self.scraper_password,
                "user_id": self.scraper_user_id,
                "name": "Scraper Account"
            },
            "messenger1": {
                "username": self.messenger1_username,
                "password": self.messenger1_password,
                "user_id": self.messenger1_user_id,
                "name": "Messenger Account 1"
            },
            "messenger2": {
                "username": self.messenger2_username,
                "password": self.messenger2_password,
                "user_id": self.messenger2_user_id,
                "name": "Messenger Account 2"
            },
            "messenger3": {
                "username": self.messenger3_username,
                "password": self.messenger3_password,
                "user_id": self.messenger3_user_id,
                "name": "Messenger Account 3"
            },
            "affiliate": {
                "username": self.affiliate_username,
                "password": self.affiliate_password,
                "user_id": self.affiliate_user_id,
                "name": "Affiliate Account"
            }
        }
        
        return account_mapping.get(account_type, account_mapping["scraper"])
    
    def get_all_accounts(self) -> dict:
        """Get all account configurations"""
        return {
            "scraper": self.get_account_config("scraper"),
            "messenger1": self.get_account_config("messenger1"),
            "messenger2": self.get_account_config("messenger2"),
            "messenger3": self.get_account_config("messenger3"),
            "affiliate": self.get_account_config("affiliate")
        }

    class Config:
        env_file = os.path.join(config_dir, ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra fields


# Global settings instance
settings = Settings()