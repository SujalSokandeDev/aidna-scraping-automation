#!/usr/bin/env python3
"""Bounded, resumable website ingestion with confirmed BigQuery delivery."""

import argparse
import json
import os
import signal
import sys
import time
import uuid
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline_runtime import timestamp, utc_now
from utils.config import Config, WORDPRESS_SITES
from utils.logger import setup_logging
from core.database_manager import DatabaseManager
from core.sitemap_crawler import SitemapCrawler
from core.content_scraper import ContentScraper
from core.content_cleaner import ContentCleaner
from core.wikimedia_augmenter import WikimediaAugmenter
from core.bigquery_manager import BigQueryManager, DryRunBigQueryManager

ATTACHMENT_REASON = 'Media attachment page; not an article'


class UnusablePage(ValueError):
    """A fetched page cannot safely be indexed as an article."""


def excessive_page_failures(processed, warnings, excluded_media=0):
    attempts = processed - excluded_media
    return bool(warnings and (warnings == attempts or (warnings >= 5 and warnings / attempts >= 0.2)))

class WordPressPipeline:
    def __init__(self, config=None, dry_run=False):
        self.config = config or Config.load()
        self.dry_run, self.stop_requested, self.progress_callback = dry_run, False, None
        self.max_records = max(1, int(os.getenv('PIPELINE_RECORD_LIMIT', '250')))
        self.max_seconds = max(60, int(os.getenv('PIPELINE_MAX_SECONDS', '2400')))
        self.logger = setup_logging(log_level=self.config.log_level, log_file=self.config.log_file)
        self.db = DatabaseManager(supabase_url=self.config.supabase_url, supabase_key=self.config.supabase_key)
        self.crawler = SitemapCrawler(user_agent=self.config.user_agent, timeout=self.config.request_timeout,
                                      max_retries=self.config.max_retries, delay=0.1)
        self.scraper = ContentScraper(user_agent=self.config.user_agent, timeout=self.config.request_timeout,
                                      max_retries=self.config.max_retries, delay=self.config.request_delay)
        self.cleaner = ContentCleaner()
        self.wikimedia = WikimediaAugmenter(max_images=self.config.wikimedia_max_images)
        self.bq = DryRunBigQueryManager() if dry_run else BigQueryManager(
            project_id=self.config.gcp_project_id, dataset=self.config.bigquery_dataset,
            table=self.config.bigquery_table, credentials_path=self.config.google_credentials_path)
        self.delivery_manifest = []
        self._register_signals()

    def _register_signals(self):
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            pass  # A GUI caller can run in a worker thread.

    def _signal_handler(self, signum, frame):
        self.stop_requested = True

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def run_full_scrape(self, site_name=None):
        # Full discovery is non-destructive: successful unchanged URLs remain intact.
        return self._run('full', site_name)

    def run_incremental_scrape(self, site_name=None):
        return self._run('incremental', site_name)

    def _run(self, mode, site_name):
        if not self.dry_run:
            self.db.check_ready()
        sites = {site_name: WORDPRESS_SITES[site_name]} if site_name else WORDPRESS_SITES
        results = []
        for name, info in sites.items():
            if self.stop_requested:
                break
            results.append(self._scrape_site(name, info, mode))
        report = {'started_by': os.getenv('GITHUB_RUN_ID', 'local'), 'finished_at': utc_now(),
                  'dry_run': self.dry_run, 'sites': results, 'delivery_manifest': self.delivery_manifest}
        report['status'] = ('failed' if self.stop_requested or any(row['status'] == 'failed' for row in results)
                            else 'dry_run' if self.dry_run else 'completed_with_warnings' if any(row['warning_count'] for row in results)
                            else 'backlog' if any(row['deferred'] for row in results) else 'completed')
        path = Path(os.getenv('PIPELINE_REPORT_PATH', self.config.base_dir / 'logs' / 'wp-report.json'))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        self.logger.info(json.dumps({k: v for k, v in report.items() if k != 'delivery_manifest'}))
        return report

    def _scrape_site(self, site_name, site_info, mode='incremental'):
        source = site_info['source_tag']
        stats = dict(site=site_name, source=source, discovered=0, eligible=0, processed=0,
                     uploaded=0, failed=0, skipped=0, excluded_media=0, deferred=0, status='running',
                     warning_count=0, warnings=[])
        crawl_id = f'{site_name}_{uuid.uuid4().hex}'
        checkpoint_created = False
        deadline = time.monotonic() + self.max_seconds
        upload_reserve = min(120, self.max_seconds / 4)
        try:
            discovery = self.crawler.crawl_site(site_name, site_info['sitemap'], deadline=deadline - upload_reserve)
            stats['discovered'] = len(discovery['urls'])
            stats['discovery_errors'] = discovery.get('errors', [])
            stats['ignored_navigation_sitemaps'] = discovery.get('ignored_navigation_sitemaps', [])
            tracking = self.db.tracking_records(source)
            candidates = []
            for item in discovery['urls']:
                previous = tracking.get(item['url'], {})
                changed = bool(item.get('lastmod') and previous.get('last_scraped')
                               and timestamp(item['lastmod']) > timestamp(previous['last_scraped']))
                if (mode != 'full' and previous.get('scrape_status') == 'skipped'
                        and previous.get('error_message') == ATTACHMENT_REASON and not changed):
                    stats['skipped'] += 1
                    continue
                if previous.get('scrape_status') != 'success' or not previous.get('content_hash') or changed:
                    candidates.append((item, previous))
                else:
                    stats['skipped'] += 1
            candidates.sort(key=lambda pair: (bool(pair[1]), pair[1].get('last_scraped') or '', pair[0]['url']))
            stats['eligible'] = len(candidates)
            candidates = candidates[:self.max_records]
            if not self.dry_run:
                self.db.create_checkpoint(crawl_id, source, mode, len(candidates))
                checkpoint_created = True
            for offset in range(0, len(candidates), self.config.insert_batch_size):
                if self.stop_requested or time.monotonic() >= deadline - upload_reserve:
                    break
                batch = candidates[offset:offset + self.config.insert_batch_size]
                articles, states, non_delivery_states = [], [], []
                for item, previous in batch:
                    if self.stop_requested or time.monotonic() >= deadline - upload_reserve:
                        break
                    url = item['url']
                    state = {**previous, 'url': url, 'source': source, 'last_modified': item.get('lastmod'),
                             'sitemap_url': item.get('sitemap_url') or previous.get('sitemap_url'),
                             'last_scraped': utc_now(), 'scrape_count': (previous.get('scrape_count') or 0) + 1}
                    try:
                        article = self.scraper.scrape_url(url)
                        state['last_scraped'] = utc_now()
                        if article.get('is_attachment'):
                            state.update(scrape_status='skipped', error_message=ATTACHMENT_REASON)
                            non_delivery_states.append(state)
                            stats['excluded_media'] += 1
                            stats['skipped'] += 1
                            stats['processed'] += 1
                            continue
                        article['content'] = self.cleaner.clean_html(article['raw_html'])
                        if not article.get('title') or not article['content'].strip():
                            raise UnusablePage('No usable title/content; refusing to index an empty page')
                        article['word_count'] = self.cleaner.get_word_count(article['content'])
                        article['reading_time'] = self.cleaner.get_reading_time(article['content'])
                        article['title'] = self.cleaner.clean_title(article['title'])
                        article['description'] = self.cleaner.clean_description(article.get('description', ''))
                        if self.config.wikimedia_max_images > 0:
                            article = self.wikimedia.augment_article(article)
                        state.update(scrape_status='success', content_hash=article['content_hash'], error_message=None,
                                     bigquery_id=self.bq.generate_record_id(source, url))
                        articles.append(article); states.append(state)
                    except (UnusablePage, requests.RequestException) as error:
                        status_code = getattr(getattr(error, 'response', None), 'status_code', None)
                        # Authentication/access failures require operator attention, not a silent skip.
                        if status_code in (401, 403):
                            raise
                        state.update(scrape_status='skipped', error_message=str(error)[:1000], last_scraped=utc_now())
                        non_delivery_states.append(state)
                        stats['warning_count'] += 1
                        stats['skipped'] += 1
                        stats['warnings'].append({'url': url, 'reason': str(error)[:1000], 'http_status': status_code})
                        self.logger.warning(f'{site_name}: skipped {url}: {error}')
                    except Exception as error:
                        state.update(scrape_status='failed', error_message=str(error)[:1000], last_scraped=utc_now())
                        non_delivery_states.append(state); stats['failed'] += 1
                        self.logger.error(f'{site_name}: {url}: {error}')
                    stats['processed'] += 1
                    time.sleep(self.config.request_delay)
                if not self.dry_run:
                    if articles:
                        confirmed, failed, errors = self.bq.insert_batch(articles, source, self.config.insert_batch_size)
                        if failed or confirmed != len(articles):
                            stats['failed'] += len(articles)
                            raise RuntimeError('BigQuery delivery unconfirmed: ' + '; '.join(errors))
                        stats['uploaded'] += confirmed
                        self.db.confirm_urls(states)
                        self.delivery_manifest.extend({'id': self.bq.generate_record_id(source, article['url']),
                            'source': source, 'content_type': 'post', 'url': article['url']} for article in articles)
                    if non_delivery_states:
                        self.db.confirm_urls(non_delivery_states)
                    self.db.update_checkpoint(crawl_id, processed=stats['processed'], successful=stats['uploaded'], failed=stats['failed'])
                if self.progress_callback:
                    self.progress_callback(stats['processed'], len(candidates), site_name)
            stats['deferred'] = max(0, stats['eligible'] - stats['processed'])
            if excessive_page_failures(stats['processed'], stats['warning_count'], stats['excluded_media']):
                stats['error'] = 'Widespread page failures: all attempted articles, or at least 5 and 20%, were rejected'
            stats['status'] = ('failed' if stats['failed'] or stats.get('error') or discovery.get('error') or discovery.get('errors') or self.stop_requested
                               else 'dry_run' if self.dry_run else 'completed_with_warnings' if stats['warning_count'] else 'completed')
        except Exception as error:
            stats.update(status='failed', error=str(error))
            self.logger.error(f'{site_name} failed: {error}')
        finally:
            if checkpoint_created:
                self.db.update_checkpoint(crawl_id, processed=stats['processed'], successful=stats['uploaded'], failed=stats['failed'],
                    status='failed' if stats['status'] == 'failed' else 'completed')
        self.logger.info(json.dumps(stats))
        return stats

    def check_new_urls(self, site_name=None):
        for name, info in WORDPRESS_SITES.items():
            if site_name and name != site_name:
                continue
            discovery = self.crawler.crawl_site(name, info['sitemap'])
            if discovery.get('error'):
                raise RuntimeError(discovery['error'])
            tracked = self.db.tracking_records(info['source_tag'])
            self.logger.info(f"{name}: {len(discovery['urls'])} discovered; {sum(row['url'] not in tracked for row in discovery['urls'])} untracked")

    def show_stats(self, site_name=None):
        self.logger.info(json.dumps(self.db.get_stats(WORDPRESS_SITES[site_name]['source_tag'] if site_name else None)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', '-m', choices=['full', 'incremental'])
    parser.add_argument('--site', '-s', choices=list(WORDPRESS_SITES))
    parser.add_argument('--limit', type=int, help='Maximum URLs processed per site in this run')
    parser.add_argument('--check-new', '-c', action='store_true')
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--dry-run', '-d', action='store_true')
    args = parser.parse_args()
    try:
        pipeline = WordPressPipeline(dry_run=args.dry_run)
        if args.limit is not None:
            if args.limit < 1:
                parser.error('--limit must be positive')
            pipeline.max_records = args.limit
        if args.check_new:
            pipeline.check_new_urls(args.site)
        elif args.stats:
            pipeline.show_stats(args.site)
        elif args.mode:
            result = pipeline._run(args.mode, args.site)
            return 1 if result['status'] == 'failed' else 0
        else:
            parser.print_help()
        return 0
    except Exception as error:
        print(f'Pipeline failed: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
