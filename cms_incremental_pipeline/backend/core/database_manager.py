"""
Database Manager for CMS Incremental Pipeline — Supabase Backend
Replaces SQLite with Supabase (PostgreSQL) for cloud-compatible state tracking.
API is identical to the original SQLite version so pipeline.py requires zero changes.
"""

import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Any

from supabase import create_client, Client


class DatabaseManager:
    """
    Manages pipeline state in Supabase.
    Drop-in replacement for the original SQLite DatabaseManager.
    """

    # Table names in Supabase
    RECORDS_TABLE = "aidna_cms_scraped_records"
    CHECKPOINTS_TABLE = "aidna_pipeline_checkpoints"

    def __init__(self, data_dir: str = None, supabase_url: str = None, supabase_key: str = None):
        """
        Initialize Supabase client.

        Args:
            data_dir: Ignored (kept for API compatibility with original)
            supabase_url: Supabase project URL (falls back to env SUPABASE_URL)
            supabase_key: Supabase API key (falls back to env SUPABASE_KEY)
        """
        import os
        url = supabase_url or os.getenv("SUPABASE_URL", "")
        key = supabase_key or os.getenv("SUPABASE_KEY", "")

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

        self.client: Client = create_client(url, key)

    # =========================================================================
    # RECORD TRACKING METHODS
    # =========================================================================

    def add_record(self, record_id: str, source: str, cms_key: str, collection: str,
                   strapi_id: str, title: str = None, published_at: str = None) -> bool:
        """Add a new record to tracking. Returns True if new, False if exists."""
        try:
            self.client.table(self.RECORDS_TABLE).upsert(
                {
                    "record_id": record_id,
                    "source": source,
                    "cms_key": cms_key,
                    "collection": collection,
                    "strapi_id": strapi_id,
                    "title": title,
                    "published_at": published_at,
                },
                on_conflict="record_id",
                ignore_duplicates=True,
            ).execute()
            return True
        except Exception:
            return False

    def add_records_batch(self, records: List[Dict[str, Any]], source: str,
                          cms_key: str, collection: str) -> int:
        """Add multiple records. Returns count of new records added."""
        rows = []
        for record in records:
            strapi_id = str(record.get("id", ""))
            record_id = f"{cms_key}_{collection}_{strapi_id}"
            title = record.get("attributes", {}).get("title", "")
            published_at = record.get("attributes", {}).get("publishedAt")

            rows.append({
                "record_id": record_id,
                "source": source,
                "cms_key": cms_key,
                "collection": collection,
                "strapi_id": strapi_id,
                "title": title,
                "published_at": published_at,
            })

        if not rows:
            return 0

        # Upsert in chunks of 500 (Supabase limit)
        added = 0
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            try:
                resp = self.client.table(self.RECORDS_TABLE).upsert(
                    chunk,
                    on_conflict="record_id",
                    ignore_duplicates=True,
                ).execute()
                added += len(resp.data) if resp.data else 0
            except Exception:
                pass

        return added

    def get_pending_records(self, source: str = None, collection: str = None,
                           limit: int = None) -> List[Dict]:
        """Get records with scrape_status='pending'."""
        query = self.client.table(self.RECORDS_TABLE).select("*").eq("scrape_status", "pending")

        if source:
            query = query.eq("source", source)
        if collection:
            query = query.eq("collection", collection)

        query = query.order("published_at", desc=True)

        if limit:
            query = query.limit(limit)

        resp = query.execute()
        return resp.data or []

    def get_last_published_date(self, source: str = None, collection: str = None) -> Optional[str]:
        """Get most recent published_at for successfully scraped records."""
        query = (
            self.client.table(self.RECORDS_TABLE)
            .select("published_at")
            .eq("scrape_status", "success")
            .order("published_at", desc=True)
            .limit(1)
        )

        if source:
            query = query.eq("source", source)
        if collection:
            query = query.eq("collection", collection)

        resp = query.execute()
        if resp.data:
            return resp.data[0].get("published_at")
        return None

    def get_new_records(self, fetched_ids: List[str], source: str,
                        collection: str) -> List[str]:
        """Find record IDs not yet in database."""
        existing_ids = set(self.get_all_strapi_ids(source, collection))
        return [id for id in fetched_ids if id not in existing_ids]

    def get_all_strapi_ids(self, source: str = None, collection: str = None) -> List[str]:
        """Get all tracked Strapi IDs."""
        query = self.client.table(self.RECORDS_TABLE).select("strapi_id")

        if source:
            query = query.eq("source", source)
        if collection:
            query = query.eq("collection", collection)

        resp = query.execute()
        return [row["strapi_id"] for row in (resp.data or [])]

    def mark_record_scraped(self, record_id: str, status: str = "success",
                           content_hash: str = None, bigquery_id: str = None,
                           error_message: str = None) -> bool:
        """Mark a record as scraped with result status."""
        update_data = {
            "scrape_status": status,
            "last_scraped": datetime.now().isoformat(),
        }
        if content_hash is not None:
            update_data["content_hash"] = content_hash
        if bigquery_id is not None:
            update_data["bigquery_id"] = bigquery_id
        if error_message is not None:
            update_data["error_message"] = error_message

        try:
            resp = (
                self.client.table(self.RECORDS_TABLE)
                .update(update_data)
                .eq("record_id", record_id)
                .execute()
            )
            return bool(resp.data)
        except Exception:
            return False

    def get_stats(self, source: str = None, collection: str = None) -> Dict[str, Any]:
        """Get statistics via Supabase RPC function."""
        try:
            resp = self.client.rpc(
                "aidna_cms_get_stats",
                {"p_source": source, "p_collection": collection}
            ).execute()
            stats = resp.data if resp.data else {}
        except Exception:
            stats = {}

        # Get breakdown if no filters applied
        breakdown = {}
        if not source and not collection:
            try:
                resp2 = self.client.rpc("aidna_cms_get_breakdown").execute()
                breakdown = resp2.data if resp2.data else {}
            except Exception:
                pass

        return {
            "total_records": stats.get("total_records", 0),
            "pending": stats.get("pending", 0),
            "success": stats.get("success", 0),
            "failed": stats.get("failed", 0),
            "skipped": stats.get("skipped", 0),
            "last_scraped": stats.get("last_scraped"),
            "breakdown": breakdown,
        }

    def get_failed_records(self, source: str = None, limit: int = 100) -> List[Dict]:
        """Get list of failed records."""
        query = (
            self.client.table(self.RECORDS_TABLE)
            .select("record_id, source, collection, title, error_message, last_scraped")
            .eq("scrape_status", "failed")
            .order("last_scraped", desc=True)
            .limit(limit)
        )
        if source:
            query = query.eq("source", source)

        resp = query.execute()
        return resp.data or []

    def get_recent_records(self, source: str = None, limit: int = 50) -> List[Dict]:
        """Get recently scraped records."""
        query = (
            self.client.table(self.RECORDS_TABLE)
            .select("record_id, source, collection, title, scrape_status, last_scraped, bigquery_id")
            .not_.is_("last_scraped", "null")
            .order("last_scraped", desc=True)
            .limit(limit)
        )
        if source:
            query = query.eq("source", source)

        resp = query.execute()
        return resp.data or []

    def reset_failed_records(self, source: str = None, collection: str = None) -> int:
        """Reset failed records to pending for retry."""
        query = (
            self.client.table(self.RECORDS_TABLE)
            .update({"scrape_status": "pending", "error_message": None})
            .eq("scrape_status", "failed")
        )
        if source:
            query = query.eq("source", source)
        if collection:
            query = query.eq("collection", collection)

        resp = query.execute()
        return len(resp.data) if resp.data else 0

    # =========================================================================
    # CHECKPOINT METHODS
    # =========================================================================

    def create_checkpoint(self, crawl_id: str, source: str, mode: str,
                          total_records: int = 0) -> bool:
        """Create a new checkpoint for a scraping run."""
        try:
            self.client.table(self.CHECKPOINTS_TABLE).insert({
                "crawl_id": crawl_id,
                "pipeline": "cms",
                "source": source,
                "mode": mode,
                "start_time": datetime.now().isoformat(),
                "total_items": total_records,
                "status": "running",
            }).execute()
            return True
        except Exception:
            return False

    def update_checkpoint(self, crawl_id: str, processed: int = None,
                          successful: int = None, failed: int = None,
                          last_record: str = None, status: str = None):
        """Update an existing checkpoint."""
        update_data = {}
        if processed is not None:
            update_data["processed_items"] = processed
        if successful is not None:
            update_data["successful_items"] = successful
        if failed is not None:
            update_data["failed_items"] = failed
        if last_record:
            update_data["last_item_id"] = last_record
        if status:
            update_data["status"] = status
            if status in ("completed", "failed", "cancelled"):
                update_data["end_time"] = datetime.now().isoformat()

        if update_data:
            self.client.table(self.CHECKPOINTS_TABLE).update(update_data).eq("crawl_id", crawl_id).execute()

    def get_recent_checkpoints(self, limit: int = 10) -> List[Dict]:
        """Get recent checkpoints for CMS pipeline."""
        resp = (
            self.client.table(self.CHECKPOINTS_TABLE)
            .select("*")
            .eq("pipeline", "cms")
            .order("start_time", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
