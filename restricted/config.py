"""Configuration settings for SBC scraper using Pydantic."""

from pydantic_settings import BaseSettings
from pydantic.v1 import Field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    """Settings class for SBC scraper configuration."""

    # Credentials
    sbc_username: str = Field(env="SBC_USERNAME")
    sbc_password: str = Field(env="SBC_PASSWORD")

    # URLs
    sbc_login_url: str = Field(env="SBC_LOGIN_URL")
    sbc_attendees_url: str = Field(env="SBC_ATTENDEES_URL")
    sbc_companies_url: str = Field(env="SBC_COMPANIES_URL")
    sbc_exhibitors_url: str = Field(env="SBC_EXHIBITORS_URL")

    # Output files
    attendees_csv_path: str = Field(env="ATTENDEES_CSV_PATH")
    companies_csv_path: str = Field(env="COMPANIES_CSV_PATH")
    exhibitors_csv_path: str = Field(env="EXHIBITORS_CSV_PATH")

    # Browser settings
    headless: bool = Field(True, env="HEADLESS")
    browser_timeout: int = Field(30000, env="BROWSER_TIMEOUT")
    delay_between_requests: int = Field(2, env="DELAY_BETWEEN_REQUESTS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
