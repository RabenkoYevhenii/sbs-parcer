"""
API Scraping Package for SBC Attendees

This package provides a refactored and modular approach to scraping SBC attendees
and managing messaging campaigns.

Components:
- BaseScraper: Core browser automation and API requests
- CompanyFilter: Company exclusion logic and similarity matching
- DataProcessor: CSV handling and data manipulation
- MessagingHandler: Chat management and messaging functionality
- SBCAttendeesScraper: Main orchestrator class
"""

from .base_scraper import BaseScraper
from .company_filter import CompanyFilter
from .data_processor import DataProcessor
from .messaging import MessagingHandler
from .main_scraper import SBCAttendeesScraper

__all__ = [
    "BaseScraper",
    "CompanyFilter",
    "DataProcessor",
    "MessagingHandler",
    "SBCAttendeesScraper",
]

__version__ = "1.0.0"
