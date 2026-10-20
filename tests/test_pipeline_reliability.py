import importlib.util
import json
import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests
import yaml
from google.api_core.exceptions import Conflict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline_runtime import merge_records, content_hash, confirm_tracking, check_state_store


def load_pipeline(folder, name):
    for key in list(sys.modules):
        if key in ('utils', 'core') or key.startswith(('utils.', 'core.')):
            del sys.modules[key]
    backend = ROOT / folder / 'backend'
    sys.path.insert(0, str(backend))
    spec = importlib.util.spec_from_file_location(name, backend / 'pipeline.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.path.remove(str(backend))
    return module


CMS = load_pipeline('cms_incremental_pipeline', 'cms_test_pipeline')
WP = load_pipeline('wordpress_scraping_pipeline', 'wp_test_pipeline')


def raw_record(id='1', title='Article'):
    return {'id': id, 'attributes': {'title': title, 'content': 'Useful content', 'slug': 'article',
            'updatedAt': '2026-10-20T01:00:00Z', 'createdAt': '2020-01-01T00:00:00Z',
            'applications': {'data': [{'id': '3'}]}}}


class CMSReliabilityTests(unittest.TestCase):
    def pipeline(self):
        pipeline = CMS.CMSPipeline.__new__(CMS.CMSPipeline)
        pipeline.config = SimpleNamespace(page_size=100, request_delay=0, insert_batch_size=2)
        pipeline.logger = Mock()
        pipeline.max_records, pipeline.dry_run, pipeline.stop_requested = 2, False, False
        pipeline.db = Mock(); pipeline.db.tracking_records.return_value = {}
        pipeline.processor = CMS.ContentProcessor()
        pipeline._bq_manager = Mock(); pipeline._bq_manager.insert_records.side_effect = lambda rows: (len(rows), 0, [])
        pipeline.delivery_manifest = []
        return pipeline

    def fetcher(self, rows=None, complete=True):
        return Mock(fetch_collection=Mock(return_value=(rows or [raw_record()],
                    {'complete': complete, 'failed_pages': [] if complete else [2], 'error': 'Page failed'})))

    def test_upload_failure_never_marks_records_successful(self):
        pipeline = self.pipeline()
        pipeline._bq_manager.insert_records.return_value = (0, 1, ['Denied'])
        pipeline._bq_manager.insert_records.side_effect = None
        result = pipeline._process_collection(self.fetcher(), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(result['status'], 'failed')
        pipeline.db.confirm_records.assert_not_called()

    def test_confirmed_upload_precedes_state_and_retains_history(self):
        pipeline = self.pipeline(); previous = {'record_id': 'businessabc_post_1', 'first_seen': '2020-01-01T00:00:00Z'}
        pipeline.db.tracking_records.return_value = {'businessabc_post_1': previous}
        events = []
        pipeline._bq_manager.insert_records.side_effect = lambda rows: (events.append('delivery') or len(rows), 0, [])
        pipeline.db.confirm_records.side_effect = lambda rows: events.append('state')
        result = pipeline._process_collection(self.fetcher(), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(result['status'], 'completed'); self.assertEqual(events, ['delivery', 'state'])
        state = pipeline.db.confirm_records.call_args.args[0][0]
        self.assertEqual(state['first_seen'], previous['first_seen'])
        self.assertEqual(state['content_hash'], content_hash(raw_record()))
        self.assertEqual(pipeline.delivery_manifest[0]['source'], '3')

    def test_unchanged_success_skips_but_modified_content_refreshes(self):
        pipeline = self.pipeline()
        pipeline.db.tracking_records.return_value = {'businessabc_post_1': {
            'scrape_status': 'success', 'content_hash': content_hash(raw_record())}}
        skipped = pipeline._process_collection(self.fetcher(), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(skipped['skipped'], 1); pipeline.db.confirm_records.assert_not_called()
        changed = pipeline._process_collection(self.fetcher([raw_record(title='Updated')]), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(changed['inserted'], 1)

    def test_incomplete_extraction_is_not_reported_complete(self):
        result = self.pipeline()._process_collection(self.fetcher(complete=False), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(result['status'], 'failed'); self.assertEqual(result['failed_pages'], [2])

    def test_limits_leave_remaining_records_eligible(self):
        result = self.pipeline()._process_collection(self.fetcher([raw_record(str(i)) for i in range(5)]), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(result['inserted'], 2); self.assertEqual(result['deferred'], 3)

    def test_dry_run_never_writes_tracking_or_uploads(self):
        pipeline = self.pipeline(); pipeline.dry_run = True
        result = pipeline._process_collection(self.fetcher(), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(result['status'], 'dry_run')
        pipeline.db.confirm_records.assert_not_called(); pipeline._bq_manager.insert_records.assert_not_called()

    def test_state_failure_after_delivery_is_visible_and_retryable(self):
        pipeline = self.pipeline(); pipeline.db.confirm_records.side_effect = RuntimeError('State unavailable')
        result = pipeline._process_collection(self.fetcher(), 'businessabc', 'post', 'BusinessABC/CitiesABC', 'incremental')
        self.assertEqual(result['status'], 'failed'); self.assertEqual(result['inserted'], 1)

    def test_education_query_uses_location_relation(self):
        self.assertIn('locations { data { id attributes { title } } }', CMS.BUSINESSABC_QUERIES['education'])

    def test_failed_page_does_not_skip_forward(self):
        fetcher = CMS.CMSFetcher('https://example.invalid/graphql', '')
        fetcher.fetch_page = Mock(side_effect=[{'data': [raw_record()], 'meta': {'pagination': {'pageCount': 3, 'total': 3}}}, ValueError('Bad field')])
        rows, stats = fetcher.fetch_all_pages('query', 'posts', page_size=1, delay=0)
        self.assertEqual(len(rows), 1); self.assertFalse(stats['complete'])
        self.assertEqual([call.args[2] for call in fetcher.fetch_page.call_args_list], [1, 2])

    def test_permanent_400_is_not_retried_and_explains_schema_error(self):
        fetcher = CMS.CMSFetcher('https://example.invalid/graphql', '')
        response = requests.Response(); response.status_code = 400
        response._content = b'{"errors":[{"message":"locations requires subfields"}]}'
        fetcher.session.post = Mock(return_value=response)
        with self.assertRaisesRegex(ValueError, 'locations requires subfields'):
            fetcher.fetch_page('query', 'posts', 1)
        self.assertEqual(fetcher.session.post.call_count, 1)


class WebsiteReliabilityTests(unittest.TestCase):
    def pipeline(self, rows=3):
        pipeline = WP.WordPressPipeline.__new__(WP.WordPressPipeline)
        pipeline.config = SimpleNamespace(insert_batch_size=2, request_delay=0, wikimedia_max_images=0)
        pipeline.logger = Mock(); pipeline.db = Mock(); pipeline.db.tracking_records.return_value = {}
        pipeline.crawler = Mock(); pipeline.crawler.crawl_site.return_value = {
            'urls': [{'url': f'https://example.com/article-{i}', 'lastmod': None} for i in range(rows)]}
        pipeline.scraper = Mock(); pipeline.scraper.scrape_url.side_effect = lambda url: {
            'url': url, 'title': 'Article', 'description': '', 'raw_html': '<p>' + 'Useful article content. ' * 10 + '</p>', 'content_hash': 'hash'}
        pipeline.cleaner = WP.ContentCleaner(); pipeline.wikimedia = Mock()
        pipeline.bq = Mock(); pipeline.bq.generate_record_id.side_effect = lambda source, url: url.rsplit('/', 1)[-1]
        pipeline.bq.insert_batch.side_effect = lambda rows, *args: (len(rows), 0, [])
        pipeline.max_records, pipeline.max_seconds = 2, 2400
        pipeline.dry_run, pipeline.stop_requested, pipeline.progress_callback = False, False, None
        pipeline.delivery_manifest = []
        return pipeline

    def run_site(self, pipeline):
        return pipeline._scrape_site('Example', {'source_tag': 'WordPress/Example', 'sitemap': 'https://example.com/sitemap.xml'})

    def test_limit_delivery_and_resume(self):
        pipeline = self.pipeline(); saved = {}
        pipeline.db.tracking_records.side_effect = lambda source: saved.copy()
        pipeline.db.confirm_urls.side_effect = lambda rows: saved.update((row['url'], row) for row in rows)
        first = self.run_site(pipeline)
        self.assertEqual(first['uploaded'], 2); self.assertEqual(first['deferred'], 1)
        second = self.run_site(pipeline)
        self.assertEqual(second['uploaded'], 1); self.assertEqual(second['skipped'], 2)
        third = self.run_site(pipeline)
        self.assertEqual(third['uploaded'], 0); self.assertEqual(len(saved), 3)
        pipeline.wikimedia.augment_article.assert_not_called()

    def test_upload_failure_does_not_persist_success(self):
        pipeline = self.pipeline(); pipeline.bq.insert_batch.side_effect = None
        pipeline.bq.insert_batch.return_value = (0, 2, ['Permission denied'])
        result = self.run_site(pipeline)
        self.assertEqual(result['status'], 'failed'); pipeline.db.confirm_urls.assert_not_called()

    def test_manual_retry_after_failed_scrape(self):
        pipeline = self.pipeline(1)
        pipeline.db.tracking_records.return_value = {'https://example.com/article-0': {'scrape_status': 'failed'}}
        self.assertEqual(self.run_site(pipeline)['uploaded'], 1)

    def test_modified_sitemap_record_is_refreshed(self):
        pipeline = self.pipeline(1)
        pipeline.crawler.crawl_site.return_value['urls'][0]['lastmod'] = '2026-10-20T00:00:00Z'
        pipeline.db.tracking_records.return_value = {'https://example.com/article-0': {
            'scrape_status': 'success', 'content_hash': 'old', 'last_scraped': '2026-10-19T00:00:00Z'}}
        self.assertEqual(self.run_site(pipeline)['uploaded'], 1)

    def test_dry_run_does_not_mutate_tracking(self):
        pipeline = self.pipeline(); pipeline.dry_run = True
        self.assertEqual(self.run_site(pipeline)['status'], 'dry_run')
        pipeline.bq.insert_batch.assert_not_called(); pipeline.db.confirm_urls.assert_not_called()
        pipeline.db.create_checkpoint.assert_not_called(); pipeline.db.update_checkpoint.assert_not_called()

    def test_partial_sitemap_failure_remains_failure(self):
        pipeline = self.pipeline(); pipeline.crawler.crawl_site.return_value.update(error='Failed child', errors=['Failed child'])
        result = self.run_site(pipeline)
        self.assertEqual(result['status'], 'failed'); self.assertEqual(result['uploaded'], 2)

    def test_bigquery_initialization_cannot_silently_enable_dry_run(self):
        config = SimpleNamespace(log_level='INFO', log_file=None, supabase_url='https://example.invalid', supabase_key='x',
            user_agent='test', request_timeout=1, max_retries=1, request_delay=0, wikimedia_max_images=0,
            gcp_project_id='p', bigquery_dataset='d', bigquery_table='t', google_credentials_path='absent')
        with patch.object(WP, 'DatabaseManager'), patch.object(WP, 'BigQueryManager', side_effect=RuntimeError('Denied')), patch.object(WP, 'DryRunBigQueryManager') as dry:
            with self.assertRaisesRegex(RuntimeError, 'Denied'):
                WP.WordPressPipeline(config)
            dry.assert_not_called()


class SitemapTraversalTests(unittest.TestCase):
    def test_navigation_sitemap_is_explicitly_skipped_not_parsed_as_content(self):
        crawler = WP.SitemapCrawler(delay=0)
        root, tags = 'https://example.com/sitemap.xml', 'https://example.com/post_tag-sitemap3.xml'
        crawler.fetch_sitemap = Mock(return_value=self.index(tags))
        result = crawler.crawl_site('Example', root)
        self.assertTrue(result['complete'])
        self.assertEqual(result['ignored_navigation_sitemaps'], [tags])
        crawler.fetch_sitemap.assert_called_once_with(root)

    def index(self, *urls):
        return '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + ''.join(f'<sitemap><loc>{url}</loc></sitemap>' for url in urls) + '</sitemapindex>'

    def urls(self, *urls):
        return '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + ''.join(f'<url><loc>{url}</loc></url>' for url in urls) + '</urlset>'

    def test_nested_index_cycle_and_duplicates(self):
        crawler = WP.SitemapCrawler(delay=0)
        data = {'https://example.com/root': self.index('/nested', '/child'),
                'https://example.com/nested': self.index('/root', '/child'),
                'https://example.com/child': self.urls('https://example.com/post', 'https://example.com/post')}
        crawler.fetch_sitemap = Mock(side_effect=data.get)
        result = crawler.crawl_site('Example', 'https://example.com/root')
        self.assertTrue(result['complete']); self.assertEqual(len(result['urls']), 1)
        self.assertEqual(crawler.fetch_sitemap.call_count, 3)

    def test_failed_child_and_malformed_xml_are_visible(self):
        crawler = WP.SitemapCrawler(delay=0)
        crawler.fetch_sitemap = Mock(side_effect=[self.index('/missing'), None])
        self.assertFalse(crawler.crawl_site('Example', 'https://example.com/root')['complete'])
        crawler.fetch_sitemap = Mock(return_value='<html>not XML')
        self.assertIn('error', crawler.crawl_site('Example', 'https://example.com/root'))

    def test_cross_site_references_are_not_fetched(self):
        crawler = WP.SitemapCrawler(delay=0)
        crawler.fetch_sitemap = Mock(return_value=self.index('https://other.invalid/private'))
        result = crawler.crawl_site('Example', 'https://example.com/root')
        self.assertIn('error', result); self.assertEqual(crawler.fetch_sitemap.call_count, 1)


class BigQueryDeliveryTests(unittest.TestCase):
    def row(self, source='3'):
        return {'id': '1', 'source': source, 'content_type': 'post', 'title': 'Title',
                'attributes': '{"content":"Article"}', 'external_images': '[]',
                'processing_timestamp': '2026-10-20T00:00:00Z'}

    def client(self, rows=None):
        client = Mock(); client.get_table.return_value.location = 'EU'
        client.query.side_effect = [Mock(result=Mock(return_value=[])), Mock(result=Mock(return_value=
            rows if rows is not None else [SimpleNamespace(id='1', source='3', content_type='post')]))]
        return client

    def test_merge_preserves_composite_identity_and_verifies_readback(self):
        client = self.client()
        with patch.dict(os.environ, {'EXPECTED_GCP_PROJECT_ID': 'project'}):
            self.assertEqual(merge_records(client, 'project.dataset.table', [self.row()]), 1)
        call = client.query.call_args_list[0]
        self.assertIn('MERGE', call.args[0]); self.assertNotIn('DELETE', call.args[0])
        self.assertIn('T.source=S.source', call.args[0]); self.assertIn('T.content_type=S.content_type', call.args[0])
        self.assertEqual(call.kwargs['location'], 'EU')
        payload = json.loads(call.kwargs['job_config'].query_parameters[0].value)
        self.assertEqual(payload[0]['attributes'], {'content': 'Article'})

    def test_replayed_batch_uses_same_job_id(self):
        ids = []
        for _ in range(2):
            client = self.client(); merge_records(client, 'project.dataset.table', [self.row()])
            ids.append(client.query.call_args_list[0].kwargs['job_id'])
        self.assertEqual(ids[0], ids[1])

    def test_uncertain_accepted_job_is_retrieved_before_readback(self):
        client = self.client(); client.query.side_effect = [Conflict('exists'), Mock(result=Mock(return_value=[SimpleNamespace(id='1', source='3', content_type='post')]))]
        self.assertEqual(merge_records(client, 'project.dataset.table', [self.row()]), 1)
        client.get_job.assert_called_once()

    def test_missing_readback_is_not_success(self):
        with self.assertRaisesRegex(RuntimeError, 'readback'):
            merge_records(self.client([]), 'project.dataset.table', [self.row()])

    def test_duplicate_input_and_wrong_destination_fail_before_delivery(self):
        client = self.client()
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            merge_records(client, 'project.dataset.table', [self.row(), self.row()])
        with patch.dict(os.environ, {'EXPECTED_GCP_PROJECT_ID': 'other'}):
            with self.assertRaisesRegex(ValueError, 'approved project'):
                merge_records(client, 'project.dataset.table', [self.row()])
        client.query.assert_not_called()

    def test_state_readback_mismatch_is_visible(self):
        client = Mock(); client.table.return_value.select.return_value.in_.return_value.execute.return_value.data = []
        with self.assertRaisesRegex(RuntimeError, 'State write/readback'):
            confirm_tracking(client, 'tracking', 'id', [{'id': '1', 'scrape_status': 'success', 'content_hash': 'h'}])

    def test_bulk_state_normalizes_keys_without_replacing_first_seen(self):
        client = Mock()
        rows = [{'id': 'old', 'scrape_status': 'success', 'content_hash': 'h', 'first_seen': '2020-01-01T00:00:00Z', 'sitemap_url': 'https://example.com/sitemap.xml'},
                {'id': 'new', 'scrape_status': 'success', 'content_hash': 'h'}]
        client.table.return_value.select.return_value.in_.return_value.execute.return_value.data = rows
        confirm_tracking(client, 'tracking', 'id', rows)
        batch = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(set(batch[0]), set(batch[1]))
        self.assertEqual(batch[0]['first_seen'], rows[0]['first_seen'])
        self.assertTrue(batch[1]['first_seen'])
        self.assertNotIn('first_seen', rows[1])

    def test_readiness_probe_cleanup_is_scoped_even_when_readback_fails(self):
        client = Mock()
        client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = []
        with self.assertRaisesRegex(RuntimeError, 'readiness'):
            check_state_store(client, 'tracking', 'id', 'cms')
        cleanup = client.table.return_value.delete.return_value.eq.call_args
        self.assertEqual(cleanup.args[0], 'crawl_id'); self.assertTrue(cleanup.args[1].startswith('aidna-readiness-'))


class WorkflowTests(unittest.TestCase):
    def test_website_jobs_are_independent_and_bounded(self):
        workflow = yaml.safe_load((ROOT / '.github/workflows/nightly-scrape.yml').read_text())
        wp, cms = workflow['jobs']['wp-pipeline'], workflow['jobs']['cms-pipeline']
        self.assertNotIn('needs', wp)
        self.assertFalse(wp['strategy']['fail-fast']); self.assertFalse(cms['strategy']['fail-fast'])
        self.assertEqual(len(wp['strategy']['matrix']['site']), 5)
        self.assertFalse(workflow['concurrency']['cancel-in-progress'])
        self.assertEqual(wp['env']['WIKIMEDIA_MAX_IMAGES'], '0')
        self.assertIn('--limit', next(step['run'] for step in wp['steps'] if step.get('name') == 'Reconcile website records'))


if __name__ == '__main__':
    unittest.main()
