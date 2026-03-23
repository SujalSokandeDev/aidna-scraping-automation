"""
Wikimedia Client for CMS Pipeline
Fetches images from Wikimedia Commons based on entity titles.
Includes rate limiting to respect API limits.
"""

import time
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import quote
import threading


class WikimediaClient:
    """
    Client for fetching images from Wikimedia Commons.
    Respects rate limits (1 request per second).
    """
    
    # Wikimedia API endpoint
    COMMONS_API = "https://commons.wikimedia.org/w/api.php"
    
    # Rate limiting: minimum seconds between requests
    RATE_LIMIT = 1.0
    
    def __init__(self, logger=None, user_agent: str = "CMSPipeline/1.0"):
        """
        Initialize Wikimedia client.
        
        Args:
            logger: Optional logger instance
            user_agent: User agent string for API requests
        """
        self.logger = logger
        self.last_request_time = 0
        self._lock = threading.Lock()
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent
        })
    
    def _log(self, level: str, message: str):
        """Log a message if logger is available."""
        if self.logger:
            getattr(self.logger, level)(message)
    
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits."""
        with self._lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.RATE_LIMIT:
                sleep_time = self.RATE_LIMIT - elapsed
                time.sleep(sleep_time)
            self.last_request_time = time.time()
    
    def search_images(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Search for images on Wikimedia Commons.
        
        Args:
            query: Search term (entity name/title)
            limit: Maximum number of images to return
        
        Returns:
            List of image info dictionaries
        """
        if not query or not query.strip():
            return []
        
        self._wait_for_rate_limit()
        
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'generator': 'search',
                'gsrnamespace': 6,  # File namespace
                'gsrsearch': f'filetype:bitmap {query}',
                'gsrlimit': limit,
                'prop': 'imageinfo',
                'iiprop': 'url|size|mime|thumburl|extmetadata',
                'iiurlwidth': 800  # Thumbnail width
            }
            
            response = self.session.get(self.COMMONS_API, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            images = []
            pages = data.get('query', {}).get('pages', {})
            
            for page_id, page_data in pages.items():
                if int(page_id) < 0:  # Invalid page
                    continue
                
                imageinfo = page_data.get('imageinfo', [{}])[0]
                extmeta = imageinfo.get('extmetadata', {})
                
                image = {
                    'title': page_data.get('title', '').replace('File:', ''),
                    'url': imageinfo.get('url', ''),
                    'thumb_url': imageinfo.get('thumburl', ''),
                    'width': imageinfo.get('width'),
                    'height': imageinfo.get('height'),
                    'mime': imageinfo.get('mime', ''),
                    'description': extmeta.get('ImageDescription', {}).get('value', ''),
                    'license': extmeta.get('LicenseShortName', {}).get('value', ''),
                    'artist': extmeta.get('Artist', {}).get('value', ''),
                    'source': 'wikimedia_commons'
                }
                
                if image['url']:
                    images.append(image)
            
            return images[:limit]
            
        except Exception as e:
            self._log('debug', f"Wikimedia search failed for '{query}': {e}")
            return []
    
    def get_entity_images(self, entity_title: str, entity_type: str = None,
                          max_images: int = 3) -> List[Dict]:
        """
        Get images for a specific entity using smart search.
        
        Args:
            entity_title: Name/title of the entity
            entity_type: Type of entity (e.g., 'company', 'city', 'athlete')
            max_images: Maximum images to return
        
        Returns:
            List of relevant image dictionaries
        """
        if not entity_title:
            return []
        
        # Build search terms based on entity type
        search_terms = [entity_title]
        
        if entity_type:
            type_keywords = {
                'city': ['city', 'skyline', 'panorama'],
                'company': ['logo', 'headquarters', 'building'],
                'education': ['university', 'campus', 'building'],
                'athlete': ['player', 'sport'],
                'team': ['team', 'logo', 'stadium'],
                'stadium': ['stadium', 'arena', 'venue'],
                'influencer': ['portrait', 'photo'],
                'place': ['landmark', 'tourist'],
                'investor': ['building', 'office'],
                'federation': ['logo', 'emblem']
            }
            
            keywords = type_keywords.get(entity_type.lower(), [])
            if keywords:
                # Add entity type to search for better results
                search_terms.append(f"{entity_title} {keywords[0]}")
        
        all_images = []
        seen_urls = set()
        
        for term in search_terms:
            if len(all_images) >= max_images:
                break
            
            images = self.search_images(term, limit=max_images)
            
            for img in images:
                if img['url'] not in seen_urls:
                    seen_urls.add(img['url'])
                    all_images.append(img)
                    
                    if len(all_images) >= max_images:
                        break
        
        return all_images[:max_images]
    
    def enrich_records_with_images(self, records: List[Dict], 
                                    collection_type: str,
                                    batch_size: int = 10,
                                    max_images_per_record: int = 2,
                                    progress_callback=None) -> List[Dict]:
        """
        Enrich a batch of records with Wikimedia images.
        
        Args:
            records: List of processed records
            collection_type: Type of collection for smart search
            batch_size: How many records to process at once
            max_images_per_record: Max images per record
            progress_callback: Optional callback(current, total)
        
        Returns:
            Records with external_images field populated
        """
        total = len(records)
        enriched = []
        
        # Skip certain collection types that don't need images
        skip_types = {'nationality', 'knowledgebase'}
        if collection_type.lower() in skip_types:
            for record in records:
                record['external_images'] = []
            return records
        
        for idx, record in enumerate(records):
            title = record.get('title', '')
            
            if title:
                images = self.get_entity_images(
                    title,
                    entity_type=collection_type,
                    max_images=max_images_per_record
                )
                record['external_images'] = images
            else:
                record['external_images'] = []
            
            enriched.append(record)
            
            if progress_callback and (idx + 1) % batch_size == 0:
                progress_callback(idx + 1, total)
        
        return enriched
    
    def test_connection(self) -> tuple:
        """
        Test connection to Wikimedia API.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            images = self.search_images("test", limit=1)
            return True, f"Wikimedia API connected - found {len(images)} test images"
        except Exception as e:
            return False, f"Wikimedia API failed: {str(e)}"
