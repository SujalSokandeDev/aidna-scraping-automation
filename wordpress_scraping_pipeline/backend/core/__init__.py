# WordPress Scraping Pipeline - Core Package
"""
Core components for WordPress scraping:
- sitemap_crawler: Parse WordPress sitemaps
- content_scraper: Extract content from pages
- content_cleaner: Clean HTML to plain text
- wikimedia_augmenter: Fetch Wikimedia Commons images
- bigquery_manager: Insert data to BigQuery
- database_manager: SQLite for URL tracking
"""

from .database_manager import DatabaseManager
from .sitemap_crawler import SitemapCrawler
from .content_scraper import ContentScraper
from .content_cleaner import ContentCleaner
from .wikimedia_augmenter import WikimediaAugmenter
from .bigquery_manager import BigQueryManager

__all__ = [
    'DatabaseManager',
    'SitemapCrawler', 
    'ContentScraper',
    'ContentCleaner',
    'WikimediaAugmenter',
    'BigQueryManager'
]
