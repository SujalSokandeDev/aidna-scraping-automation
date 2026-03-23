#!/usr/bin/env python3
"""
WordPress Scraping Pipeline - Main CLI Orchestrator
Scrapes WordPress sites, cleans content, augments with Wikimedia images,
and stores in BigQuery.

Usage:
    python pipeline.py --mode full              # Full scrape all sites
    python pipeline.py --mode incremental       # Only new URLs
    python pipeline.py --mode incremental --site FashionABC  # Single site
    python pipeline.py --check-new              # Dry run - count new URLs
    python pipeline.py --stats                  # Show statistics
"""

import argparse
import signal
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.config import Config, WORDPRESS_SITES
from utils.logger import setup_logging, get_source_logger
from core.database_manager import DatabaseManager
from core.sitemap_crawler import SitemapCrawler
from core.content_scraper import ContentScraper
from core.content_cleaner import ContentCleaner
from core.wikimedia_augmenter import WikimediaAugmenter
from core.bigquery_manager import BigQueryManager, DryRunBigQueryManager


class WordPressPipeline:
    """Main pipeline orchestrator for WordPress scraping."""
    
    def __init__(self, config: Config = None, dry_run: bool = False):
        """
        Initialize the pipeline.
        
        Args:
            config: Configuration object
            dry_run: If True, don't insert into BigQuery
        """
        self.config = config or Config.load()
        self.dry_run = dry_run
        self.stop_requested = False
        
        # Setup logging
        self.logger = setup_logging(
            log_level=self.config.log_level,
            log_file=self.config.log_file
        )
        
        self.progress_callback = None
        
        # Initialize components immediately so CLI runs work even without a progress callback
        self._init_components()
    
    def set_progress_callback(self, callback):
        """Set callback for progress updates (current, total, message)."""
        self.progress_callback = callback
        
        # Setup signal handler for graceful shutdown
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            # Signals only work in main thread, skip if running in background thread
            self.logger.debug("Running in background thread, signal handlers disabled")
    
    def _init_components(self):
        """Initialize all pipeline components."""
        # Database
        data_dir = self.config.base_dir / "data"
        self.db = DatabaseManager(str(data_dir))
        
        # Sitemap crawler
        self.crawler = SitemapCrawler(
            user_agent=self.config.user_agent,
            timeout=self.config.request_timeout,
            max_retries=self.config.max_retries,
            delay=0.5  # Faster for sitemaps
        )
        
        # Content scraper
        self.scraper = ContentScraper(
            user_agent=self.config.user_agent,
            timeout=self.config.request_timeout,
            max_retries=self.config.max_retries,
            delay=self.config.request_delay
        )
        
        # Content cleaner
        self.cleaner = ContentCleaner()
        
        # Wikimedia augmenter
        self.wikimedia = WikimediaAugmenter(
            max_images=self.config.wikimedia_max_images
        )
        
        # BigQuery manager
        if self.dry_run:
            self.bq = DryRunBigQueryManager()
            self.logger.info("Running in DRY RUN mode - no BigQuery inserts")
        else:
            try:
                self.bq = BigQueryManager(
                    project_id=self.config.gcp_project_id,
                    dataset=self.config.bigquery_dataset,
                    table=self.config.bigquery_table,
                    credentials_path=self.config.google_credentials_path
                )
            except Exception as e:
                self.logger.warning(f"BigQuery unavailable: {e}. Using dry run mode.")
                self.bq = DryRunBigQueryManager()
                self.dry_run = True
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals for graceful shutdown."""
        self.logger.warning("Interrupt received, stopping gracefully...")
        self.stop_requested = True
    
    def run_full_scrape(self, site_name: str = None):
        """
        Run full scrape for all or single site.
        
        Args:
            site_name: Specific site to scrape (optional)
        """
        self.logger.info("=" * 70)
        self.logger.info("Starting FULL scrape")
        self.logger.info("=" * 70)
        
        sites = {site_name: WORDPRESS_SITES[site_name]} if site_name else WORDPRESS_SITES
        
        total_scraped = 0
        total_success = 0
        total_failed = 0
        
        for name, site_info in sites.items():
            if self.stop_requested:
                break
            
            scraped, success, failed = self._scrape_site(
                name, site_info, mode='full'
            )
            total_scraped += scraped
            total_success += success
            total_failed += failed
        
        self._print_summary(total_scraped, total_success, total_failed)
    
    def run_incremental_scrape(self, site_name: str = None):
        """
        Run incremental scrape - only new URLs.
        
        Args:
            site_name: Specific site to scrape (optional)
        """
        self.logger.info("=" * 70)
        self.logger.info("Starting INCREMENTAL scrape")
        self.logger.info("=" * 70)
        
        sites = {site_name: WORDPRESS_SITES[site_name]} if site_name else WORDPRESS_SITES
        
        total_scraped = 0
        total_success = 0
        total_failed = 0
        
        for name, site_info in sites.items():
            if self.stop_requested:
                break
            
            scraped, success, failed = self._scrape_site(
                name, site_info, mode='incremental'
            )
            total_scraped += scraped
            total_success += success
            total_failed += failed
        
        self._print_summary(total_scraped, total_success, total_failed)
    
    def _scrape_site(self, site_name: str, site_info: Dict[str, str], 
                     mode: str = 'incremental') -> tuple:
        """
        Scrape a single site.
        
        Returns:
            Tuple of (total_scraped, success_count, failed_count)
        """
        source = site_info['source_tag']
        log = get_source_logger(source)
        
        log.info(f"Processing site: {site_name}")
        
        # Create checkpoint
        crawl_id = f"{site_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        # Crawl sitemap
        log.info("Fetching sitemap...")
        result = self.crawler.crawl_site(site_name, site_info['sitemap'])
        
        if result.get('error'):
            log.error(f"Sitemap error: {result['error']}")
            return 0, 0, 0
        
        sitemap_urls = result['urls']
        url_list = self.crawler.get_url_list(sitemap_urls)
        
        log.info(f"Sitemap: {result['total_found']} URLs | Content: {result['total_filtered']} | Skipped: {result['skipped']}")
        
        # Add new URLs to database
        new_count = self.db.add_urls_batch(sitemap_urls, source)
        log.info(f"Added {new_count} new URLs to database")
        
        # Get URLs to scrape based on mode
        if mode == 'full':
            # In full mode, get all URLs from sitemap but skip those already successfully scraped
            # This allows retrying failed ones or picking up new ones, without re-doing success
            completed_urls = set(self.db.get_completed_urls(source))
            urls_to_scrape = [u for u in url_list if u not in completed_urls]
            
            skipped_count = len(url_list) - len(urls_to_scrape)
            if skipped_count > 0:
                log.info(f"Skipping {skipped_count} already successfully scraped URLs")
        else:
            # Only get pending URLs for incremental
            urls_to_scrape = self.db.get_pending_urls(source)
        
        if not urls_to_scrape:
            log.info("No new URLs to scrape")
            return 0, 0, 0
        
        log.info(f"URLs to scrape: {len(urls_to_scrape)}")
        
        # Create checkpoint
        self.db.create_checkpoint(crawl_id, source, mode, len(urls_to_scrape))
        
        # Scrape URLs
        success_count = 0
        failed_count = 0
        articles_batch = []
        processed_urls = 0
        
        # Progress iterator
        if TQDM_AVAILABLE:
            iterator = tqdm(enumerate(urls_to_scrape), total=len(urls_to_scrape),
                           desc=f"Scraping {site_name}", unit="url")
        else:
            iterator = enumerate(urls_to_scrape)
            
        # Notify start of scraping
        if self.progress_callback:
            self.progress_callback(0, len(urls_to_scrape), f"Scraping {len(urls_to_scrape)} URLs from {site_name}")
        
        for i, url in iterator:
            if self.stop_requested:
                log.warning("Stop requested, saving progress...")
                break
            
            try:
                # Scrape URL
                article = self.scraper.scrape_url(url)
                
                # Clean content
                clean_content = self.cleaner.clean_html(article['raw_html'])
                article['content'] = clean_content
                article['word_count'] = self.cleaner.get_word_count(clean_content)
                article['reading_time'] = self.cleaner.get_reading_time(clean_content)
                
                # Clean title and description
                article['title'] = self.cleaner.clean_title(article['title'])
                article['description'] = self.cleaner.clean_description(article['description'])
                
                # Augment with Wikimedia images
                article = self.wikimedia.augment_article(article)
                
                # Queue for batch insert; DB status will be updated after BigQuery success
                articles_batch.append(article)
            
            except Exception as e:
                failed_count += 1
                self.db.mark_url_scraped(url, 'failed', error_message=str(e))
                log.warning(f"Failed: {url} | {str(e)[:50]}")
            finally:
                processed_urls += 1
            
            # Insert batch when full or at end
            if len(articles_batch) >= self.config.insert_batch_size:
                batch_success = self._insert_batch(articles_batch, source, log)
                if batch_success:
                    for art in articles_batch:
                        success_count += 1
                        self.db.mark_url_scraped(
                            art['url'], 'success',
                            content_hash=art.get('content_hash'),
                            bigquery_id=self.bq.generate_record_id(source, art['url'])
                        )
                else:
                    failed_count += len(articles_batch)
                    # Leave URLs pending for retry on next run
                articles_batch = []
            
            # Update checkpoint periodically
            if (i + 1) % self.config.checkpoint_interval == 0:
                self.db.update_checkpoint(
                    crawl_id,
                    processed=processed_urls,
                    successful=success_count,
                    failed=failed_count,
                    last_url=url
                )
            
            # Update progress callback
            if self.progress_callback:
                self.progress_callback(i + 1, len(urls_to_scrape), f"Processed {i + 1}/{len(urls_to_scrape)}: {url}")
        
        # Insert remaining articles
        if articles_batch:
            batch_success = self._insert_batch(articles_batch, source, log)
            if batch_success:
                for art in articles_batch:
                    success_count += 1
                    self.db.mark_url_scraped(
                        art['url'], 'success',
                        content_hash=art.get('content_hash'),
                        bigquery_id=self.bq.generate_record_id(source, art['url'])
                    )
            else:
                failed_count += len(articles_batch)
        
        # Final checkpoint update
        self.db.update_checkpoint(
            crawl_id,
            processed=processed_urls,
            successful=success_count,
            failed=failed_count,
            status='completed' if not self.stop_requested else 'cancelled'
        )
        
        log.info(f"Completed: {success_count} success, {failed_count} failed")
        
        return len(urls_to_scrape), success_count, failed_count
    
    def _insert_batch(self, articles: List[Dict], source: str, log):
        """Insert a batch of articles into BigQuery."""
        if not articles:
            return True
        
        success, errors, error_msgs = self.bq.insert_batch(
            articles, source, self.config.insert_batch_size
        )
        
        if errors > 0:
            log.warning(f"BigQuery batch: {success} inserted, {errors} failed")
            for msg in error_msgs[:3]:  # Log first 3 errors
                log.error(f"  {msg}")
            return False
        else:
            log.info(f"BigQuery batch: {success} records inserted")
            return True
    
    def check_new_urls(self, site_name: str = None):
        """
        Check for new URLs without scraping (dry run).
        
        Args:
            site_name: Specific site to check (optional)
        """
        print("\n" + "=" * 70)
        print("NEW URL CHECK (Dry Run)")
        print("=" * 70)
        
        sites = {site_name: WORDPRESS_SITES[site_name]} if site_name else WORDPRESS_SITES
        
        total_new = 0
        
        for name, site_info in sites.items():
            source = site_info['source_tag']
            
            # Crawl sitemap
            result = self.crawler.crawl_site(name, site_info['sitemap'])
            
            if result.get('error'):
                print(f"\n❌ {name}: Error - {result['error']}")
                continue
            
            sitemap_urls = self.crawler.get_url_list(result['urls'])
            
            # Get existing URLs from database
            existing_urls = set(self.db.get_all_urls(source))
            
            # Find new URLs
            new_urls = [u for u in sitemap_urls if u not in existing_urls]
            
            print(f"\nSite: {source}")
            print(f"  Sitemap URLs: {len(sitemap_urls):,}")
            print(f"  Scraped URLs: {len(existing_urls):,}")
            print(f"  New URLs: {len(new_urls):,}")
            
            total_new += len(new_urls)
        
        print("\n" + "-" * 70)
        print(f"Total new URLs across all sites: {total_new:,}")
        print("=" * 70 + "\n")
    
    def show_stats(self, site_name: str = None):
        """
        Show scraping statistics.
        
        Args:
            site_name: Specific site to show (optional)
        """
        print("\n" + "=" * 70)
        print("SCRAPING STATISTICS")
        print("=" * 70)
        
        if site_name:
            source = WORDPRESS_SITES[site_name]['source_tag']
            stats = self.db.get_stats(source)
            print(f"\nSite: {source}")
        else:
            stats = self.db.get_stats()
            print("\nAll Sites Combined:")
        
        print(f"  Total URLs: {stats['total_urls']:,}")
        print(f"  Pending: {stats['pending']:,}")
        print(f"  Success: {stats['success']:,}")
        print(f"  Failed: {stats['failed']:,}")
        print(f"  Skipped: {stats['skipped']:,}")
        print(f"  Last Scraped: {stats['last_scraped'] or 'Never'}")
        
        if stats.get('sites'):
            print("\n" + "-" * 50)
            print("Per-Site Breakdown:")
            print("-" * 50)
            
            for source, site_stats in stats['sites'].items():
                success_rate = (site_stats['success'] / site_stats['total'] * 100) if site_stats['total'] > 0 else 0
                print(f"\n{source}:")
                print(f"  Total: {site_stats['total']:,} | Success: {site_stats['success']:,} ({success_rate:.1f}%)")
                print(f"  Pending: {site_stats['pending']:,} | Failed: {site_stats['failed']:,}")
        
        # Recent checkpoints
        print("\n" + "-" * 50)
        print("Recent Scrape Runs:")
        print("-" * 50)
        
        checkpoints = self.db.get_recent_checkpoints(5)
        for cp in checkpoints:
            print(f"\n{cp['crawl_id'][:40]}...")
            print(f"  Source: {cp['source']} | Mode: {cp['mode']}")
            print(f"  Status: {cp['status']} | Processed: {cp['processed_urls']}/{cp['total_urls']}")
            print(f"  Started: {cp['start_time']}")
        
        print("\n" + "=" * 70 + "\n")
    
    def _print_summary(self, total: int, success: int, failed: int):
        """Print final scraping summary."""
        self.logger.info("=" * 70)
        self.logger.info("PIPELINE COMPLETE!")
        self.logger.info(f"Total URLs processed: {total:,}")
        self.logger.info(f"Success: {success:,} | Failed: {failed:,}")
        if total > 0:
            self.logger.info(f"Success Rate: {success/total*100:.1f}%")
        self.logger.info("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='WordPress Scraping Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --mode full              # Full scrape all sites
  python pipeline.py --mode incremental       # Only new URLs
  python pipeline.py --mode incremental --site FashionABC
  python pipeline.py --check-new              # Count new URLs (dry run)
  python pipeline.py --stats                  # Show statistics
        """
    )
    
    parser.add_argument(
        '--mode', '-m',
        choices=['full', 'incremental'],
        help='Scraping mode: full (all URLs) or incremental (new URLs only)'
    )
    
    parser.add_argument(
        '--site', '-s',
        choices=list(WORDPRESS_SITES.keys()),
        help='Specific site to scrape'
    )
    
    parser.add_argument(
        '--check-new', '-c',
        action='store_true',
        help='Check for new URLs without scraping (dry run)'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show scraping statistics'
    )
    
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Run without inserting into BigQuery'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    try:
        pipeline = WordPressPipeline(dry_run=args.dry_run)
    except Exception as e:
        print(f"Failed to initialize pipeline: {e}")
        sys.exit(1)
    
    # Execute based on arguments
    if args.check_new:
        pipeline.check_new_urls(args.site)
    elif args.stats:
        pipeline.show_stats(args.site)
    elif args.mode == 'full':
        pipeline.run_full_scrape(args.site)
    elif args.mode == 'incremental':
        pipeline.run_incremental_scrape(args.site)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
