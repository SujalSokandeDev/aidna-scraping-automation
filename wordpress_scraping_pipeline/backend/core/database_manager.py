"""
Database Manager for WordPress Scraping Pipeline — Supabase Backend
Replaces SQLite with Supabase (PostgreSQL) for cloud-compatible state tracking.
API is identical to the original SQLite version so pipeline.py requires zero changes.
"""

import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

from supabase import create_client, Client


class DatabaseManager:
    """
    Manages pipeline state in Supabase.
    Drop-in replacement for the original SQLite DatabaseManager.
    """

    # Table names in Supabase
    URLS_TABLE = "aidna_wp_scraped_urls"
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
    # URL TRACKING METHODS
    # =========================================================================

    def add_url(self, url: str, source: str, sitemap_url: str = None,
                last_modified: str = None) -> bool:
        """Add a new URL to tracking. Returns True if new, False if exists."""
        try:
            self.client.table(self.URLS_TABLE).upsert(
                {
                    "url": url,
                    "source": source,
                    "sitemap_url": sitemap_url,
                    "last_modified": last_modified,
                },
                on_conflict="url",
                ignore_duplicates=True,
            ).execute()
            return True
        except Exception:
            return False

    def add_urls_batch(self, urls: List[Dict[str, Any]], source: str) -> int:
        """Add multiple URLs. Returns count of new URLs added."""
        rows = []
        for url_data in urls:
            rows.append({
                "url": url_data["url"],
                "source": source,
                "last_modified": url_data.get("lastmod"),
            })

        if not rows:
            return 0

        # Upsert in chunks of 500 (Supabase limit)
        added = 0
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            try:
                resp = self.client.table(self.URLS_TABLE).upsert(
                    chunk,
                    on_conflict="url",
                    ignore_duplicates=True,
                ).execute()
                added += len(resp.data) if resp.data else 0
            except Exception:
                pass

        return added

    def get_all_urls(self, source: str = None) -> List[str]:
        """Get all tracked URLs, optionally filtered by source."""
        query = self.client.table(self.URLS_TABLE).select("url")
        if source:
            query = query.eq("source", source)

        # Paginate to get all results (Supabase default limit is 1000)
        all_urls = []
        offset = 0
        page_size = 1000
        while True:
            resp = query.range(offset, offset + page_size - 1).execute()
            rows = resp.data or []
            all_urls.extend(row["url"] for row in rows)
            if len(rows) < page_size:
                break
            offset += page_size

        return all_urls

    def get_pending_urls(self, source: str = None, limit: int = None) -> List[str]:
        """Get URLs with scrape_status='pending'."""
        query = self.client.table(self.URLS_TABLE).select("url").eq("scrape_status", "pending")
        if source:
            query = query.eq("source", source)

        if limit:
            resp = query.limit(limit).execute()
        else:
            # Paginate to get all pending
            all_urls = []
            offset = 0
            page_size = 1000
            while True:
                resp = query.range(offset, offset + page_size - 1).execute()
                rows = resp.data or []
                all_urls.extend(row["url"] for row in rows)
                if len(rows) < page_size:
                    break
                offset += page_size
            return all_urls

        return [row["url"] for row in (resp.data or [])]

    def get_completed_urls(self, source: str = None) -> List[str]:
        """Get URLs with scrape_status='success'."""
        query = self.client.table(self.URLS_TABLE).select("url").eq("scrape_status", "success")
        if source:
            query = query.eq("source", source)

        all_urls = []
        offset = 0
        page_size = 1000
        while True:
            resp = query.range(offset, offset + page_size - 1).execute()
            rows = resp.data or []
            all_urls.extend(row["url"] for row in rows)
            if len(rows) < page_size:
                break
            offset += page_size

        return all_urls

    def get_new_urls(self, sitemap_urls: List[str], source: str) -> List[str]:
        """Find URLs in sitemap that aren't in database."""
        existing_urls = set(self.get_all_urls(source))
        return [url for url in sitemap_urls if url not in existing_urls]

    def mark_url_scraped(self, url: str, status: str = "success",
                         content_hash: str = None, bigquery_id: str = None,
                         error_message: str = None) -> bool:
        """Mark a URL as scraped with result status."""
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
            # Increment scrape_count via a separate call (Supabase REST doesn't support col + 1)
            # First get current count
            current = self.client.table(self.URLS_TABLE).select("scrape_count").eq("url", url).execute()
            current_count = 0
            if current.data:
                current_count = current.data[0].get("scrape_count", 0) or 0
            update_data["scrape_count"] = current_count + 1

            resp = (
                self.client.table(self.URLS_TABLE)
                .update(update_data)
                .eq("url", url)
                .execute()
            )
            return bool(resp.data)
        except Exception:
            return False

    def sync_with_sitemap(self, sitemap_urls: List[str], source: str) -> int:
        """Remove URLs from database that no longer exist in sitemap."""
        existing_urls = set(self.get_all_urls(source))
        sitemap_set = set(sitemap_urls)
        removed_urls = existing_urls - sitemap_set

        removed_count = 0
        for url in removed_urls:
            try:
                self.client.table(self.URLS_TABLE).delete().eq("url", url).eq("source", source).execute()
                removed_count += 1
            except Exception:
                pass

        return removed_count

    def get_stats(self, source: str = None) -> Dict[str, Any]:
        """Get statistics via Supabase RPC function."""
        try:
            resp = self.client.rpc(
                "aidna_wp_get_stats",
                {"p_source": source}
            ).execute()
            stats = resp.data if resp.data else {}
        except Exception:
            stats = {}

        # Get site breakdown if no filter
        sites_breakdown = {}
        if not source:
            try:
                resp2 = self.client.rpc("aidna_wp_get_breakdown").execute()
                sites_breakdown = resp2.data if resp2.data else {}
            except Exception:
                pass

        return {
            "total_urls": stats.get("total_urls", 0),
            "pending": stats.get("pending", 0),
            "success": stats.get("success", 0),
            "failed": stats.get("failed", 0),
            "skipped": stats.get("skipped", 0),
            "last_scraped": stats.get("last_scraped"),
            "sites": sites_breakdown,
        }

    def get_failed_urls(self, source: str = None, limit: int = 100) -> List[Dict]:
        """Get list of failed URLs."""
        query = (
            self.client.table(self.URLS_TABLE)
            .select("url, source, error_message, last_scraped, scrape_count")
            .eq("scrape_status", "failed")
            .order("last_scraped", desc=True)
            .limit(limit)
        )
        if source:
            query = query.eq("source", source)

        resp = query.execute()
        return resp.data or []

    def get_recent_urls(self, source: str = None, limit: int = 50) -> List[Dict]:
        """Get recently scraped URLs."""
        query = (
            self.client.table(self.URLS_TABLE)
            .select("url, source, scrape_status, last_scraped, bigquery_id")
            .not_.is_("last_scraped", "null")
            .order("last_scraped", desc=True)
            .limit(limit)
        )
        if source:
            query = query.eq("source", source)

        resp = query.execute()
        return resp.data or []

    def delete_url(self, url: str) -> bool:
        """Delete a URL from tracking."""
        try:
            self.client.table(self.URLS_TABLE).delete().eq("url", url).execute()
            return True
        except Exception:
            return False

    # =========================================================================
    # CHECKPOINT METHODS
    # =========================================================================

    def create_checkpoint(self, crawl_id: str, source: str, mode: str,
                          total_urls: int = 0) -> bool:
        """Create a new checkpoint for a scraping run."""
        try:
            self.client.table(self.CHECKPOINTS_TABLE).insert({
                "crawl_id": crawl_id,
                "pipeline": "wp",
                "source": source,
                "mode": mode,
                "start_time": datetime.now().isoformat(),
                "total_items": total_urls,
                "status": "running",
            }).execute()
            return True
        except Exception:
            return False

    def update_checkpoint(self, crawl_id: str, processed: int = None,
                          successful: int = None, failed: int = None,
                          last_url: str = None, status: str = None):
        """Update an existing checkpoint."""
        update_data = {}
        if processed is not None:
            update_data["processed_items"] = processed
        if successful is not None:
            update_data["successful_items"] = successful
        if failed is not None:
            update_data["failed_items"] = failed
        if last_url:
            update_data["last_item_id"] = last_url
        if status:
            update_data["status"] = status
            if status in ("completed", "failed", "cancelled"):
                update_data["end_time"] = datetime.now().isoformat()

        if update_data:
            self.client.table(self.CHECKPOINTS_TABLE).update(update_data).eq("crawl_id", crawl_id).execute()

    def get_checkpoint(self, crawl_id: str) -> Optional[Dict]:
        """Get checkpoint by crawl_id."""
        resp = (
            self.client.table(self.CHECKPOINTS_TABLE)
            .select("*")
            .eq("crawl_id", crawl_id)
            .execute()
        )
        return resp.data[0] if resp.data else None

    def get_active_checkpoint(self, source: str = None) -> Optional[Dict]:
        """Get the current running checkpoint."""
        query = (
            self.client.table(self.CHECKPOINTS_TABLE)
            .select("*")
            .eq("pipeline", "wp")
            .eq("status", "running")
            .order("start_time", desc=True)
            .limit(1)
        )
        if source:
            query = query.eq("source", source)

        resp = query.execute()
        return resp.data[0] if resp.data else None

    def get_recent_checkpoints(self, limit: int = 10) -> List[Dict]:
        """Get recent checkpoints for WordPress pipeline."""
        resp = (
            self.client.table(self.CHECKPOINTS_TABLE)
            .select("*")
            .eq("pipeline", "wp")
            .order("start_time", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
