#!/usr/bin/env python3
"""
CMS Incremental Pipeline - Main Orchestrator
Scrapes BusinessABC and SportsABC CMS sources incrementally.

Usage:
    python pipeline.py --mode incremental        # Only new records
    python pipeline.py --mode full               # All records (re-scrape)
    python pipeline.py --mode incremental --source businessabc
    python pipeline.py --stats                   # Show statistics
    python pipeline.py --check-new               # Check for new records
    python pipeline.py --diagnostics             # Test connections
"""

import argparse
import json
import sys
import time
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline_runtime import content_hash, utc_now, timestamp

from utils.config import Config, CMS_SOURCES
from utils.logger import setup_logging, get_logger
from core.database_manager import DatabaseManager
from core.cms_fetcher import CMSFetcher, create_fetcher, BUSINESSABC_QUERIES, SPORTSABC_QUERIES
from core.content_processor import ContentProcessor
from core.bigquery_manager import BigQueryManager, DryRunBigQueryManager, BIGQUERY_AVAILABLE
from core.wikipedia_client import WikipediaClient


class CMSPipeline:
    """
    Main pipeline orchestrator for CMS incremental scraping.
    Supports both full and incremental modes.
    """
    
    def __init__(self, config: Config = None, dry_run: bool = False):
        """
        Initialize the pipeline.
        
        Args:
            config: Configuration object
            dry_run: If True, don't insert into BigQuery
        """
        self.config = config or Config.load()
        self.dry_run = dry_run
        self.logger = setup_logging(
            log_level=self.config.log_level,
            log_file=self.config.log_file
        )
        
        # Initialize components
        self.db = DatabaseManager(str(self.config.data_dir))
        self.processor = ContentProcessor(self.logger)
        self.wikipedia = WikipediaClient(self.logger)
        
        # BigQuery (lazy initialization)
        self._bq_manager = None
        
        # Pipeline state
        self.stop_requested = False
        self.progress_callback = None
        
        # Wikipedia settings - Disabled for speed as requested
        # To enable, use --with-wikipedia flag (if implemented) or set to True
        self.enable_wikipedia = False
        self.wikipedia_max_images = 7
        self.max_records = max(1, int(os.getenv("PIPELINE_RECORD_LIMIT", "200")))
        self.delivery_manifest = []
    
    @property
    def bq_manager(self):
        """Lazy initialization of BigQuery manager."""
        if self._bq_manager is None:
            if self.dry_run:
                self._bq_manager = DryRunBigQueryManager(self.logger)
            else:
                self._bq_manager = BigQueryManager(
                    project_id=self.config.gcp_project_id,
                    dataset=self.config.bigquery_dataset,
                    table=self.config.bigquery_table,
                    credentials_path=self.config.google_credentials_path,
                    logger=self.logger
                )
        return self._bq_manager
    
    def set_progress_callback(self, callback):
        """Set callback for progress updates."""
        self.progress_callback = callback
    
    def _update_progress(self, current: int, total: int, message: str):
        """Update progress via callback if available."""
        if self.progress_callback:
            self.progress_callback(current, total, message)
    
    def run_diagnostics(self) -> bool:
        """Test connections to CMS sources and BigQuery with detailed output."""
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("🔍 CMS INCREMENTAL PIPELINE - DIAGNOSTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📁 Data Directory: {self.config.data_dir}")
        self.logger.info("")
        
        all_ok = True
        
        # Validate config
        self.logger.info("─" * 70)
        self.logger.info("📋 STEP 1: Configuration Validation")
        self.logger.info("─" * 70)
        errors = self.config.validate()
        if errors:
            self.logger.error("❌ Configuration errors found:")
            for e in errors:
                self.logger.error(f"   • {e}")
            return False
        self.logger.info("✅ Configuration validated successfully")
        self.logger.info("")
        
        # Test BusinessABC with collection details
        self.logger.info("─" * 70)
        self.logger.info("📋 STEP 2: BusinessABC CMS Connection")
        self.logger.info("─" * 70)
        self.logger.info(f"   URL: {self.config.businessabc_url}")
        try:
            fetcher = CMSFetcher(
                self.config.businessabc_url,
                self.config.businessabc_token,
                None  # Suppress internal logging for cleaner output
            )
            
            # Get details for each collection
            collections = CMS_SOURCES['businessabc']['collections']
            self.logger.info(f"   Collections: {len(collections)}")
            self.logger.info("")
            
            total_records = 0
            for collection in collections:
                try:
                    records, stats = fetcher.fetch_collection(
                        'businessabc', collection,
                        page_size=1, max_records=1
                    )
                    count = stats.get('total_available', 0)
                    if stats.get('failed_pages'):
                        raise RuntimeError(stats.get('error', 'Collection readiness failed'))
                    total_records += count
                    status = "✅" if count > 0 else "⚠️"
                    self.logger.info(f"   {status} {collection:15} │ {count:,} records")
                except Exception as e:
                    self.logger.warning(f"   ❌ {collection:15} │ Error: {str(e)[:30]}")
                    all_ok = False
            
            self.logger.info(f"   ─────────────────────┼───────────────")
            self.logger.info(f"   📊 Total             │ {total_records:,} records")
            self.logger.info("")
            self.logger.info("✅ BusinessABC connection successful")
            
        except Exception as e:
            self.logger.error(f"❌ BusinessABC connection failed: {e}")
            all_ok = False
        self.logger.info("")
        
        # Test SportsABC with collection details
        self.logger.info("─" * 70)
        self.logger.info("📋 STEP 3: SportsABC CMS Connection")
        self.logger.info("─" * 70)
        self.logger.info(f"   URL: {self.config.sportsabc_url}")
        try:
            fetcher = CMSFetcher(
                self.config.sportsabc_url,
                self.config.sportsabc_token,
                None
            )
            
            collections = CMS_SOURCES['sportsabc']['collections']
            self.logger.info(f"   Collections: {len(collections)}")
            self.logger.info("")
            
            total_records = 0
            for collection in collections:
                try:
                    records, stats = fetcher.fetch_collection(
                        'sportsabc', collection,
                        page_size=1, max_records=1
                    )
                    count = stats.get('total_available', 0)
                    if stats.get('failed_pages'):
                        raise RuntimeError(stats.get('error', 'Collection readiness failed'))
                    total_records += count
                    status = "✅" if count > 0 else "⚠️"
                    self.logger.info(f"   {status} {collection:15} │ {count:,} records")
                except Exception as e:
                    self.logger.warning(f"   ❌ {collection:15} │ Error: {str(e)[:30]}")
                    all_ok = False
            
            self.logger.info(f"   ─────────────────────┼───────────────")
            self.logger.info(f"   📊 Total             │ {total_records:,} records")
            self.logger.info("")
            self.logger.info("✅ SportsABC connection successful")
            
        except Exception as e:
            self.logger.error(f"❌ SportsABC connection failed: {e}")
            all_ok = False
        self.logger.info("")
        
        # Test BigQuery
        self.logger.info("─" * 70)
        self.logger.info("📋 STEP 4: BigQuery Connection")
        self.logger.info("─" * 70)
        if self.dry_run:
            self.logger.info("⏩ Skipped (dry run mode)")
        else:
            self.logger.info(f"   Project: {self.config.gcp_project_id}")
            self.logger.info(f"   Dataset: {self.config.bigquery_dataset}")
            self.logger.info(f"   Table: {self.config.bigquery_table}")
            try:
                success, message = self.bq_manager.test_connection()
                if success:
                    self.logger.info(f"✅ {message}")
                else:
                    self.logger.error(f"❌ {message}")
                    all_ok = False
            except Exception as e:
                self.logger.error(f"❌ BigQuery connection failed: {e}")
                all_ok = False
        self.logger.info("")
        
        # Test SQLite
        self.logger.info("─" * 70)
        self.logger.info("STEP 5: Supabase state storage")
        self.logger.info("─" * 70)
        try:
            stats = self.db.get_stats()
            self.db.check_ready()
            self.logger.info(f"   📊 Total Records Tracked: {stats['total_records']:,}")
            self.logger.info(f"   ✅ Success: {stats['success']:,}")
            self.logger.info(f"   ⏳ Pending: {stats['pending']:,}")
            self.logger.info(f"   ❌ Failed: {stats['failed']:,}")
            if stats['last_scraped']:
                self.logger.info(f"   🕐 Last Scraped: {stats['last_scraped']}")
            self.logger.info("Supabase read/write/checkpoint readiness confirmed")
        except Exception as e:
            self.logger.error(f"Supabase readiness error: {e}")
            all_ok = False
        self.logger.info("")
        
        # Test Wikipedia
        self.logger.info("─" * 70)
        self.logger.info("📋 STEP 6: Wikipedia API")
        self.logger.info("─" * 70)
        try:
            success, message = self.wikipedia.test_connection()
            if success:
                self.logger.info(f"✅ {message}")
            else:
                self.logger.warning(f"⚠️ {message} (non-critical)")
        except Exception as e:
            self.logger.warning(f"⚠️ Wikipedia: {e} (non-critical)")
        self.logger.info("")
        
        # Final Summary
        self.logger.info("=" * 70)
        if all_ok:
            self.logger.info("🎉 DIAGNOSTICS COMPLETE: ALL CHECKS PASSED")
        else:
            self.logger.info("⚠️ DIAGNOSTICS COMPLETE: SOME CHECKS FAILED")
        self.logger.info("=" * 70)
        self.logger.info("")
        
        return all_ok
    
    def check_new_records(self, source_name: str = None) -> Dict[str, Dict]:
        """
        Check for new records without scraping.
        
        Args:
            source_name: Specific source to check (optional)
        
        Returns:
            Dictionary of results per source/collection
        """
        self.logger.info("=" * 70)
        self.logger.info("CHECKING FOR NEW RECORDS")
        self.logger.info("=" * 70)
        
        results = {}
        sources_to_check = [source_name] if source_name else list(CMS_SOURCES.keys())
        
        for cms_key in sources_to_check:
            if cms_key not in CMS_SOURCES:
                continue
            
            cms_config = CMS_SOURCES[cms_key]
            source_tag = cms_config['source_tag']
            
            self.logger.info(f"\n{cms_key.upper()} ({source_tag}):")
            
            fetcher = CMSFetcher(
                self.config.businessabc_url if cms_key == 'businessabc' else self.config.sportsabc_url,
                self.config.businessabc_token if cms_key == 'businessabc' else self.config.sportsabc_token,
                self.logger
            )
            
            for collection in cms_config['collections']:
                try:
                    # Fetch first page to get total
                    records, stats = fetcher.fetch_collection(
                        cms_key, collection,
                        page_size=1, max_records=1
                    )
                    
                    api_total = stats.get('total_available', 0)
                    
                    # Get tracked count
                    db_stats = self.db.get_stats(source=source_tag, collection=collection)
                    tracked = db_stats['total_records']
                    pending = db_stats['pending']
                    
                    new_estimate = max(0, api_total - tracked + pending)
                    
                    results[f"{cms_key}/{collection}"] = {
                        'api_total': api_total,
                        'tracked': tracked,
                        'pending': pending,
                        'new_estimate': new_estimate
                    }
                    
                    self.logger.info(
                        f"  {collection}: API={api_total}, Tracked={tracked}, "
                        f"Pending={pending}, New≈{new_estimate}"
                    )
                    
                except Exception as e:
                    self.logger.error(f"  {collection}: Error - {e}")
                    results[f"{cms_key}/{collection}"] = {'error': str(e)}
        
        return results
    
    def run_incremental_scrape(self, source_name: str = None,
                               collection_name: str = None) -> Dict:
        """
        Run incremental scrape - only process new/pending records.
        
        Args:
            source_name: Specific source to scrape (optional)
            collection_name: Specific collection to scrape (optional)
        
        Returns:
            Statistics dictionary
        """
        return self._run_scrape('incremental', source_name, collection_name)
    
    def run_full_scrape(self, source_name: str = None,
                       collection_name: str = None) -> Dict:
        """
        Run full scrape - process all records.
        
        Args:
            source_name: Specific source to scrape (optional)
            collection_name: Specific collection to scrape (optional)
        
        Returns:
            Statistics dictionary
        """
        return self._run_scrape('full', source_name, collection_name)
    
    def _run_scrape(self, mode: str, source_name: str = None,
                   collection_name: str = None) -> Dict:
        """
        Internal method to run scraping.
        
        Args:
            mode: 'full' or 'incremental'
            source_name: Specific source
            collection_name: Specific collection
        
        Returns:
            Statistics dictionary
        """
        crawl_id = f"cms_crawl_{uuid.uuid4().hex}"
        deadline = time.monotonic() + max(60, int(os.getenv('PIPELINE_MAX_SECONDS', '3300')))
        if not self.dry_run:
            self.db.check_ready()
        if not self.dry_run:
            self.bq_manager.test_connection()
        
        self.logger.info("=" * 70)
        self.logger.info(f"CMS INCREMENTAL PIPELINE - {mode.upper()} MODE")
        self.logger.info(f"Crawl ID: {crawl_id}")
        self.logger.info("=" * 70)
        
        all_stats = {
            'crawl_id': crawl_id,
            'mode': mode,
            'start_time': datetime.now().isoformat(),
            'sources': {}
        }
        
        sources_to_process = [source_name] if source_name else list(CMS_SOURCES.keys())
        
        for cms_key in sources_to_process:
            if cms_key not in CMS_SOURCES:
                continue
            
            if self.stop_requested:
                self.logger.info("Stop requested, aborting...")
                break
            
            cms_config = CMS_SOURCES[cms_key]
            source_tag = cms_config['source_tag']
            collections = [collection_name] if collection_name else cms_config['collections']
            
            # Filter to valid collections
            collections = [c for c in collections if c in cms_config['collections']]
            
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"Processing {cms_key.upper()} ({source_tag})")
            self.logger.info(f"Collections: {collections}")
            self.logger.info("=" * 70)
            
            # Create checkpoint
            source_crawl_id = f"{crawl_id}_{cms_key}"
            if not self.dry_run:
                self.db.create_checkpoint(source_crawl_id, source_tag, mode)
            
            # Initialize fetcher
            fetcher = CMSFetcher(
                self.config.businessabc_url if cms_key == 'businessabc' else self.config.sportsabc_url,
                self.config.businessabc_token if cms_key == 'businessabc' else self.config.sportsabc_token,
                self.logger
            )
            fetcher.deadline = deadline
            
            source_stats = []
            
            for collection in collections:
                if self.stop_requested:
                    break
                
                stats = self._process_collection(
                    fetcher, cms_key, collection, source_tag, mode
                )
                source_stats.append(stats)
                
                # Update checkpoint
                total_processed = sum(s.get('inserted', 0) for s in source_stats)
                if not self.dry_run:
                    self.db.update_checkpoint(
                    source_crawl_id,
                    total=sum(s.get('fetched', 0) for s in source_stats),
                    processed=total_processed,
                    successful=total_processed,
                    status='running'
                )
                
                time.sleep(1)
            
            # Mark checkpoint complete
            total_success = sum(s.get('inserted', 0) for s in source_stats)
            total_failed = sum(s.get('failed', 0) for s in source_stats)
            if not self.dry_run:
                self.db.update_checkpoint(
                source_crawl_id,
                total=sum(s.get('fetched', 0) for s in source_stats),
                processed=total_success + total_failed,
                successful=total_success,
                failed=total_failed,
                status='cancelled' if self.stop_requested else ('failed' if any(s.get('status') == 'failed' for s in source_stats) else 'completed')
            )
            
            all_stats['sources'][cms_key] = source_stats
        
        all_stats['end_time'] = datetime.now().isoformat()
        all_stats['delivery_manifest'] = self.delivery_manifest
        all_stats['status'] = 'failed' if self.stop_requested or any(
            c.get('status') == 'failed' for rows in all_stats['sources'].values() for c in rows
        ) else ('dry_run' if self.dry_run else 'backlog' if any(
            c.get('deferred') for rows in all_stats['sources'].values() for c in rows) else 'completed')
        report_path = Path(os.getenv('PIPELINE_REPORT_PATH', self.config.base_dir / 'logs' / 'cms-report.json'))
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(all_stats, indent=2), encoding='utf-8')
        
        # Print summary
        self._print_summary(all_stats)
        
        return all_stats
    
    def _process_collection(self, fetcher: CMSFetcher, cms_key: str,
                           collection: str, source_tag: str,
                           mode: str) -> Dict:
        """Reconcile changed content in bounded batches; commit state after confirmed delivery."""
        stats = dict(collection=collection, source=source_tag, fetched=0, processed=0,
                     downloaded=0, inserted=0, failed=0, skipped=0, deferred=0, status='started')
        try:
            records, fetch_stats = fetcher.fetch_inventory(
                cms_key, collection, page_size=self.config.page_size, delay=self.config.request_delay)
            stats['fetched'] = len(records)
            stats['failed_pages'] = fetch_stats.get('failed_pages', [])
            tracking = self.db.tracking_records(source_tag, collection)
            pending = []
            for record in records:
                key = f"{cms_key}_{collection}_{record['id']}"
                previous = tracking.get(key, {})
                modified = timestamp((record.get('attributes') or {}).get('updatedAt'))
                scraped = timestamp(previous.get('last_scraped'))
                if (mode == 'full' or previous.get('scrape_status') != 'success' or not previous.get('content_hash')
                        or not modified or not scraped or modified > scraped):
                    pending.append((record, key, previous))
                else:
                    stats['skipped'] += 1
            # Previously untracked records take precedence over legacy hash refreshes.
            pending.sort(key=lambda item: (bool(item[2]), item[2].get('last_scraped') or '', str(item[0]['id'])))
            stats['deferred'] = max(0, len(pending) - self.max_records)
            pending = pending[:self.max_records]
            for offset in range(0, len(pending), self.config.insert_batch_size):
                if self.stop_requested:
                    raise RuntimeError('Interrupted; remaining records will resume next run')
                batch = pending[offset:offset + self.config.insert_batch_size]
                downloaded = fetcher.fetch_records(cms_key, collection, [row[0]['id'] for row in batch])
                full_records = {str(row['id']): row for row in downloaded}
                stats['downloaded'] += len(downloaded)
                prepared, states = [], []
                for inventory, key, previous in batch:
                    raw = full_records[str(inventory['id'])]
                    digest = content_hash(raw)
                    record = self.processor.structure_record(raw, collection, source_tag, cms_key)
                    # Preserve optional historical image enrichment on existing rows during refresh.
                    record['external_images'] = None
                    prepared.append(record)
                    states.append({**previous, 'record_id': key, 'source': source_tag, 'cms_key': cms_key,
                        'collection': collection, 'strapi_id': str(raw['id']), 'title': record['title'],
                        'published_at': record['published_at'], 'scrape_status': 'success',
                        'content_hash': digest, 'last_scraped': utc_now(), 'bigquery_id': record['id'], 'error_message': None})
                stats['processed'] += len(prepared)
                if self.dry_run:
                    continue
                inserted, failed, errors = self.bq_manager.insert_records(prepared)
                if failed or inserted != len(prepared):
                    stats['failed'] += len(prepared)
                    raise RuntimeError('BigQuery delivery failed: ' + '; '.join(errors))
                stats['inserted'] += inserted
                self.db.confirm_records(states)
                self.delivery_manifest.extend({k: record[k] for k in ('id', 'source', 'content_type', 'processing_timestamp')} for record in prepared)
            if fetch_stats.get('failed_pages') or not fetch_stats.get('complete'):
                raise RuntimeError('Incomplete extraction: ' + str(fetch_stats.get('error', 'pagination incomplete')))
            stats['status'] = 'dry_run' if self.dry_run else 'completed'
        except Exception as error:
            self.logger.error(f"{cms_key}/{collection} failed: {error}")
            stats['status'] = 'failed'
            stats['error'] = str(error)
        self.logger.info(json.dumps(stats))
        return stats
    
    def _print_summary(self, stats: Dict):
        """Print pipeline execution summary."""
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("PIPELINE SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Crawl ID: {stats['crawl_id']}")
        self.logger.info(f"Mode: {stats['mode']}")
        self.logger.info(f"Start: {stats['start_time']}")
        self.logger.info(f"End: {stats.get('end_time', 'N/A')}")
        self.logger.info("")
        
        for source, collections in stats.get('sources', {}).items():
            total_fetched = sum(c.get('fetched', 0) for c in collections)
            total_inserted = sum(c.get('inserted', 0) for c in collections)
            source_tag = CMS_SOURCES[source]['source_tag']
            
            self.logger.info(f"{source.upper()} ({source_tag})")
            self.logger.info(f"  Total: {total_inserted}/{total_fetched} records inserted")
            
            for c in collections:
                status_icon = "✅" if c.get('status') == 'completed' else "❌"
                self.logger.info(f"  {status_icon} {c['collection']}: {c.get('inserted', 0)}/{c.get('fetched', 0)}")
            
            self.logger.info("")
    
    def show_stats(self, source_name: str = None):
        """Display database statistics."""
        self.logger.info("=" * 70)
        self.logger.info("DATABASE STATISTICS")
        self.logger.info("=" * 70)
        
        stats = self.db.get_stats(source=source_name)
        
        self.logger.info(f"Total Records: {stats['total_records']}")
        self.logger.info(f"  - Success: {stats['success']}")
        self.logger.info(f"  - Pending: {stats['pending']}")
        self.logger.info(f"  - Failed: {stats['failed']}")
        self.logger.info(f"Last Scraped: {stats['last_scraped'] or 'Never'}")
        
        if stats['breakdown']:
            self.logger.info("\nBreakdown by Source/Collection:")
            for key, data in stats['breakdown'].items():
                self.logger.info(f"  {key}: {data['success']}/{data['total']} success")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='CMS Incremental Pipeline')
    parser.add_argument('--mode', choices=['incremental', 'full'],
                       help='Scraping mode')
    parser.add_argument('--source', choices=['businessabc', 'sportsabc'],
                       help='Specific source to scrape')
    parser.add_argument('--collection', help='Specific collection to scrape')
    parser.add_argument('--stats', action='store_true',
                       help='Show database statistics')
    parser.add_argument('--check-new', action='store_true',
                       help='Check for new records without scraping')
    parser.add_argument('--diagnostics', action='store_true',
                       help='Run connection diagnostics')
    parser.add_argument('--dry-run', action='store_true',
                       help='Dry run (no BigQuery insert)')
    parser.add_argument('--no-wikipedia', action='store_true',
                       help='Disable Wikipedia image enrichment')
    parser.add_argument('--limit', type=int, help='Maximum changed records delivered per collection this run')
    
    args = parser.parse_args()
    
    pipeline = CMSPipeline(dry_run=args.dry_run)
    if args.limit is not None:
        if args.limit < 1:
            parser.error('--limit must be positive')
        pipeline.max_records = args.limit
    
    # Apply Wikipedia setting
    if args.no_wikipedia:
        pipeline.enable_wikipedia = False
    
    if args.diagnostics:
        sys.exit(0 if pipeline.run_diagnostics() else 1)
    elif args.stats:
        pipeline.show_stats(args.source)
    elif args.check_new:
        pipeline.check_new_records(args.source)
    elif args.mode:
        if args.mode == 'incremental':
            result = pipeline.run_incremental_scrape(args.source, args.collection)
        else:
            result = pipeline.run_full_scrape(args.source, args.collection)
        sys.exit(1 if result['status'] == 'failed' else 0)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

