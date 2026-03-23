"""
BigQuery Manager for CMS Incremental Pipeline
Handles BigQuery table creation and record insertion.
"""

import json
from typing import List, Dict, Tuple, Optional
from datetime import datetime

# BigQuery imports with fallback
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False


class BigQueryManager:
    """
    Manages BigQuery operations for CMS content storage.
    Uses the unified table schema with source column.
    """
    
    def __init__(self, project_id: str, dataset: str, table: str,
                 credentials_path: str, logger=None):
        """
        Initialize BigQuery manager.
        
        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
            credentials_path: Path to service account JSON
            logger: Optional logger instance
        """
        if not BIGQUERY_AVAILABLE:
            raise ImportError("google-cloud-bigquery package is not installed")
        
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.table_id = f"{project_id}.{dataset}.{table}"
        self.logger = logger
        
        self.client = self._init_client(credentials_path)
        self._ensure_table_exists()
    
    def _log(self, level: str, message: str):
        """Log a message if logger is available."""
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")
    
    def _init_client(self, credentials_path: str) -> "bigquery.Client":
        """Initialize BigQuery client with service account credentials."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client = bigquery.Client(
                credentials=credentials,
                project=self.project_id,
                location='EU'
            )
            self._log('info', f"BigQuery client initialized for project: {self.project_id}")
            return client
        except Exception as e:
            self._log('error', f"Failed to initialize BigQuery client: {str(e)}")
            raise
    
    def _ensure_table_exists(self):
        """Create table with unified schema if it doesn't exist."""
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("content_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("attributes", "JSON"),
            bigquery.SchemaField("external_images", "JSON"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("slug", "STRING"),
            bigquery.SchemaField("locale", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("processing_timestamp", "TIMESTAMP"),
        ]
        
        try:
            self.client.get_table(self.table_id)
            self._log('info', f"Table exists: {self.table_id}")
        except Exception:
            table = bigquery.Table(self.table_id, schema=schema)
            self.client.create_table(table)
            self._log('info', f"Created table: {self.table_id}")
    
    def insert_records(self, records: List[Dict]) -> Tuple[int, int, List[str]]:
        """
        Insert records into BigQuery.
        
        Args:
            records: List of structured records
        
        Returns:
            Tuple of (success_count, error_count, error_messages)
        """
        if not records:
            return 0, 0, []
        
        rows = []
        for record in records:
            try:
                row = {
                    'id': str(record['id']),
                    'source': record['source'],
                    'content_type': record['content_type'],
                    'attributes': json.dumps(record.get('attributes_json', {}), ensure_ascii=False),
                    'external_images': record.get('external_images', json.dumps([])),
                    'title': record.get('title', ''),
                    'slug': record.get('slug', ''),
                    'locale': record.get('locale', 'en'),
                    'published_at': record.get('published_at'),
                    'updated_at': record.get('updated_at'),
                    'created_at': record.get('created_at'),
                    'processing_timestamp': record.get('processing_timestamp'),
                }
                rows.append(row)
            except Exception as e:
                self._log('error', f"Error preparing record {record.get('id')}: {e}")
        
        if not rows:
            return 0, 0, []
        
        try:
            errors = self.client.insert_rows_json(self.table_id, rows)
            success_count = len(rows) - len(errors)
            error_messages = [str(err) for err in errors[:5]]  # First 5 errors
            
            if errors:
                self._log('warning', f"Insert completed with {len(errors)} errors")
            else:
                self._log('info', f"Inserted {success_count} records successfully")
            
            return success_count, len(errors), error_messages
            
        except Exception as e:
            self._log('error', f"Batch insert failed: {e}")
            return 0, len(rows), [str(e)]
    
    def insert_batch(self, records: List[Dict], batch_size: int = 20,
                    source: str = None, collection: str = None,
                    batch_callback=None) -> Tuple[int, int]:
        """
        Insert records in batches.
        
        Args:
            records: List of structured records
            batch_size: Records per batch
            source: Source name for logging
            collection: Collection name for logging
            batch_callback: Optional callback(batch_num, total_batches, inserted)
        
        Returns:
            Tuple of (total_inserted, total_failed)
        """
        if not records:
            return 0, 0
        
        total_inserted = 0
        total_failed = 0
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(records))
            batch = records[start_idx:end_idx]
            
            inserted, failed, errors = self.insert_records(batch)
            total_inserted += inserted
            total_failed += failed
            
            # Log batch insert
            log_prefix = ""
            if source and collection:
                log_prefix = f"{source}/{collection} - "
            
            status = "SUCCESS" if failed == 0 else "PARTIAL"
            self._log('info', 
                f"[INSERT] {log_prefix}Batch {batch_idx + 1}/{total_batches} | "
                f"Inserted: {inserted}/{len(batch)} | {status}"
            )
            
            if batch_callback:
                batch_callback(batch_idx + 1, total_batches, inserted)
        
        return total_inserted, total_failed
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test BigQuery connection.
        
        Returns:
            Tuple of (success boolean, message string)
        """
        try:
            self.client.get_table(self.table_id)
            return True, f"Connected to table: {self.table_id}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"


class DryRunBigQueryManager:
    """
    Mock BigQuery manager for dry run mode.
    Simulates inserts without actually connecting to BigQuery.
    """
    
    def __init__(self, logger=None):
        self.logger = logger
        self.inserted_records = []
        self.table_id = "dry_run_table"
    
    def _log(self, level: str, message: str):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[DRY RUN] [{level.upper()}] {message}")
    
    def insert_records(self, records: List[Dict]) -> Tuple[int, int, List[str]]:
        self.inserted_records.extend(records)
        self._log('info', f"[DRY RUN] Would insert {len(records)} records")
        return len(records), 0, []
    
    def insert_batch(self, records: List[Dict], batch_size: int = 20,
                    source: str = None, collection: str = None,
                    batch_callback=None) -> Tuple[int, int]:
        self.inserted_records.extend(records)
        self._log('info', f"[DRY RUN] Would insert {len(records)} records in batches")
        return len(records), 0
    
    def test_connection(self) -> Tuple[bool, str]:
        return True, "Dry run mode - no BigQuery connection"
