"""
BigQuery Manager
Handles data insertion into BigQuery's unified_all_cms_content table.
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None
    service_account = None


class BigQueryManager:
    """Manages BigQuery operations for the WordPress scraping pipeline."""
    
    def __init__(self, project_id: str, dataset: str, table: str,
                 credentials_path: str = None):
        """
        Initialize BigQuery manager.
        
        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
            credentials_path: Path to service account JSON file
        """
        if not BIGQUERY_AVAILABLE:
            raise ImportError(
                "google-cloud-bigquery is not installed. "
                "Run: pip install google-cloud-bigquery"
            )
        
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.table_id = f"{project_id}.{dataset}.{table}"
        
        # Initialize client
        if credentials_path and Path(credentials_path).exists():
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.client = bigquery.Client(
                project=project_id,
                credentials=credentials
            )
        else:
            # Try default credentials
            self.client = bigquery.Client(project=project_id)
        
        # Ensure schema exists
        self.ensure_table()

    def ensure_table(self):
        """Ensure the BigQuery table exists with correct schema."""
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="Unique record ID"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED", description="Data source identifier"),
            bigquery.SchemaField("content_type", "STRING", mode="REQUIRED", description="Type of content (post, page, etc)"),
            bigquery.SchemaField("attributes", "JSON", mode="NULLABLE", description="JSON blob of all attributes"),
            bigquery.SchemaField("external_images", "JSON", mode="NULLABLE", description="JSON list of external images"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE", description="Article title"),
            bigquery.SchemaField("slug", "STRING", mode="NULLABLE", description="URL slug"),
            bigquery.SchemaField("locale", "STRING", mode="NULLABLE", description="Content locale"),
            bigquery.SchemaField("published_at", "TIMESTAMP", mode="NULLABLE", description="Published timestamp"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="Last updated timestamp"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Ingestion timestamp"),
            bigquery.SchemaField("processing_timestamp", "TIMESTAMP", mode="NULLABLE", description="Processing timestamp"),
        ]

        try:
            self.client.get_table(self.table_id)
            print(f"Table {self.table_id} already exists.")
        except Exception:
            print(f"Table {self.table_id} not found. Creating...")
            try:
                # Ensure dataset exists first
                dataset_ref = self.client.dataset(self.dataset)
                try:
                    self.client.get_dataset(dataset_ref)
                except Exception:
                    print(f"Dataset {self.dataset} not found. Creating...")
                    dataset = bigquery.Dataset(dataset_ref)
                    dataset.location = "US"  # Default location
                    self.client.create_dataset(dataset, timeout=30)
                
                # Create table
                table = bigquery.Table(self.table_id, schema=schema)
                # Partition by created_at for better query performance
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at"
                )
                self.client.create_table(table)
                print(f"Table {self.table_id} validated/created successfully.")
            except Exception as e:
                print(f"Failed to create table/dataset: {e}")
                # We don't raise here to allow init to complete, but inserts will fail
    
    def generate_record_id(self, source: str, url: str) -> str:
        """
        Generate unique record ID.
        
        Args:
            source: Source tag (e.g., "WordPress/FashionABC")
            url: Article URL
        
        Returns:
            Unique ID string
        """
        site_name = source.split('/')[-1] if '/' in source else source
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        return f"{site_name}_{url_hash}"
    
    def format_record(self, article: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        Format article data for BigQuery insertion.
        
        Args:
            article: Scraped article data
            source: Source tag
        
        Returns:
            Formatted record matching BigQuery schema
        """
        record_id = self.generate_record_id(source, article['url'])
        
        # Build attributes JSON
        attributes = {
            'title': article.get('title', ''),
            'slug': article.get('slug', ''),
            'description': article.get('description', ''),
            'content': article.get('content', ''),  # Cleaned plain text
            'published_date': article.get('published_date'),
            'modified_date': article.get('modified_date'),
            'author': article.get('author'),
            'categories': article.get('categories', []),
            'tags': article.get('tags', []),
            'feature_image': article.get('feature_image'),
            'images': article.get('images', []),
            'sources': article.get('sources', []),
            'word_count': article.get('word_count', 0),
            'reading_time': article.get('reading_time', 0),
            'original_url': article['url'],
        }
        
        # Format external images
        external_images = article.get('external_images', [])
        
        # Parse dates
        published_at = self._parse_datetime(article.get('published_date'))
        updated_at = self._parse_datetime(article.get('modified_date'))
        
        # Use current UTC time for created_at (ingestion time) to avoid partition errors
        # BigQuery only allows partitions within 3650 days past and 366 days future
        created_at = datetime.utcnow().isoformat()
        
        return {
            'id': record_id,
            'source': source,
            'content_type': 'post',
            'attributes': json.dumps(attributes),
            'external_images': json.dumps(external_images),
            'title': article.get('title', '')[:500],  # Limit length
            'slug': article.get('slug', '')[:200],
            'locale': article.get('locale', 'en'),
            'published_at': published_at,
            'updated_at': updated_at,
            'created_at': created_at,
            'processing_timestamp': datetime.now().isoformat(),
        }
    
    def _parse_datetime(self, dt_str: str) -> Optional[str]:
        """Parse datetime string to ISO format."""
        if not dt_str:
            return None
        
        try:
            # Try parsing common formats
            for fmt in [
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S.%f%z',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
            ]:
                try:
                    dt = datetime.strptime(dt_str[:26], fmt[:len(dt_str)])
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # If all else fails, return as-is if it looks like ISO
            if 'T' in dt_str or '-' in dt_str:
                return dt_str
            
            return None
        except Exception:
            return None
    
    def insert_records(self, records: List[Dict[str, Any]]) -> Tuple[int, int, List[str]]:
        """
        Insert records into BigQuery.
        
        Args:
            records: List of formatted records
        
        Returns:
            Tuple of (success_count, error_count, error_messages)
        """
        if not records:
            return 0, 0, []
        
        errors = self.client.insert_rows_json(
            self.table_id,
            records,
            row_ids=[r['id'] for r in records]
        )
        
        if not errors:
            return len(records), 0, []
        
        # Process errors
        error_messages = []
        for error in errors:
            error_messages.append(str(error))
        
        success_count = len(records) - len(errors)
        return success_count, len(errors), error_messages
    
    def insert_batch(self, articles: List[Dict[str, Any]], source: str,
                     batch_size: int = 20) -> Tuple[int, int, List[str]]:
        """
        Insert articles in batches.
        
        Args:
            articles: List of article data dicts
            source: Source tag
            batch_size: Records per batch
        
        Returns:
            Tuple of (total_success, total_errors, all_error_messages)
        """
        total_success = 0
        total_errors = 0
        all_errors = []
        
        # Format all records
        records = [self.format_record(a, source) for a in articles]
        
        # Insert in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            success, errors, error_msgs = self.insert_records(batch)
            total_success += success
            total_errors += errors
            all_errors.extend(error_msgs)
        
        return total_success, total_errors, all_errors
    
    def check_existing_ids(self, record_ids: List[str]) -> set:
        """
        Check which record IDs already exist in BigQuery.
        
        Args:
            record_ids: List of record IDs to check
        
        Returns:
            Set of existing IDs
        """
        if not record_ids:
            return set()
        
        # Build query
        ids_str = ', '.join(f"'{id}'" for id in record_ids)
        query = f"""
            SELECT id FROM `{self.table_id}`
            WHERE id IN ({ids_str})
        """
        
        try:
            results = self.client.query(query).result()
            return {row.id for row in results}
        except Exception as e:
            print(f"Error checking existing IDs: {e}")
            return set()
    
    def get_source_stats(self, source: str = None) -> Dict[str, Any]:
        """
        Get statistics for records in BigQuery.
        
        Args:
            source: Source tag to filter by (optional)
        
        Returns:
            Dictionary with statistics
        """
        where_clause = f"WHERE source = '{source}'" if source else ""
        
        query = f"""
            SELECT 
                source,
                COUNT(*) as total_records,
                MIN(published_at) as earliest_published,
                MAX(published_at) as latest_published,
                MAX(processing_timestamp) as last_processed
            FROM `{self.table_id}`
            {where_clause}
            GROUP BY source
            ORDER BY total_records DESC
        """
        
        try:
            results = self.client.query(query).result()
            
            stats = {}
            for row in results:
                stats[row.source] = {
                    'total_records': row.total_records,
                    'earliest_published': row.earliest_published.isoformat() if row.earliest_published else None,
                    'latest_published': row.latest_published.isoformat() if row.latest_published else None,
                    'last_processed': row.last_processed.isoformat() if row.last_processed else None,
                }
            
            return stats
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {}


class DryRunBigQueryManager:
    """Mock BigQuery manager for dry-run/testing without actual BigQuery."""
    
    def __init__(self, *args, **kwargs):
        """Initialize dry-run manager (ignores all args)."""
        self.records = []
        self.table_id = "dry_run.test.table"
    
    def generate_record_id(self, source: str, url: str) -> str:
        """Generate record ID."""
        site_name = source.split('/')[-1] if '/' in source else source
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        return f"{site_name}_{url_hash}"
    
    def format_record(self, article: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Format record (returns minimal data for dry run)."""
        return {
            'id': self.generate_record_id(source, article['url']),
            'source': source,
            'title': article.get('title', ''),
            'url': article['url'],
        }
    
    def insert_records(self, records: List[Dict]) -> Tuple[int, int, List[str]]:
        """Simulate inserting records."""
        self.records.extend(records)
        return len(records), 0, []
    
    def insert_batch(self, articles: List[Dict], source: str,
                     batch_size: int = 20) -> Tuple[int, int, List[str]]:
        """Simulate batch insert."""
        records = [self.format_record(a, source) for a in articles]
        self.records.extend(records)
        return len(records), 0, []
    
    def check_existing_ids(self, record_ids: List[str]) -> set:
        """Return empty set (no existing records in dry run)."""
        return set()
    
    def get_source_stats(self, source: str = None) -> Dict[str, Any]:
        """Return stats from dry-run records."""
        return {'dry_run': {'total_records': len(self.records)}}
