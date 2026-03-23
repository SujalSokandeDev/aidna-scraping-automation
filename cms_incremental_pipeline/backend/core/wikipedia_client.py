"""
Wikipedia Client for CMS Pipeline
Fetches images from Wikipedia/Wikimedia Commons based on entity titles.
Logic directly ported from unified_cms_pipeline.py WikimediaImageAugmenter.
"""

import time
import requests
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

class WikipediaClient:
    """
    Fetches Wikipedia/Commons images for content augmentation.
    Ported from unified_cms_pipeline.py WikimediaImageAugmenter.
    """
    
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'AIDNA-CMSPipeline/1.0'
        })
        self.wikipedia_api = "https://en.wikipedia.org/w/api.php"
        self.commons_api = "https://commons.wikimedia.org/w/api.php"
        self.cache = {}
    
    def _make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """Make API request with polite delay"""
        try:
            time.sleep(0.2)
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if self.logger:
                self.logger.debug(f"API request failed: {e}")
            return None
    
    def search_wikipedia(self, query: str, limit: int = 3) -> List[Dict]:
        """Search Wikipedia for pages matching the query"""
        cache_key = f"search:{query}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        params = {
            'action': 'query',
            'list': 'search',
            'srsearch': query,
            'srlimit': limit,
            'format': 'json'
        }
        
        result = self._make_request(self.wikipedia_api, params)
        if not result or 'query' not in result or 'search' not in result['query']:
            return []
        
        search_results = result['query']['search']
        self.cache[cache_key] = search_results
        return search_results
    
    def get_page_images(self, page_id: int, limit: int = 10) -> List[Tuple[str, str]]:
        """Fetch image filenames for a Wikipedia page"""
        cache_key = f"page_images:{page_id}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        params = {
            'action': 'query',
            'prop': 'images|info',
            'pageids': page_id,
            'imlimit': limit * 2,
            'format': 'json'
        }
        
        result = self._make_request(self.wikipedia_api, params)
        if not result or 'query' not in result or 'pages' not in result['query']:
            return []
        
        page_data = result['query']['pages'].get(str(page_id), {})
        page_title = page_data.get('title', '')
        images = page_data.get('images', []) or []
        
        filenames = []
        for img in images:
            title = img.get('title')
            if not title or not title.startswith('File:'):
                continue
            filenames.append((title.replace('File:', ''), page_title))
            if len(filenames) >= limit * 2:
                break
        
        self.cache[cache_key] = filenames
        return filenames
    
    def get_commons_metadata(self, filename: str) -> Optional[Dict]:
        """Get metadata for an image from Wikimedia Commons"""
        cache_key = f"commons:{filename}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        params = {
            'action': 'query',
            'titles': f'File:{filename}',
            'prop': 'imageinfo',
            'iiprop': 'url|mime|extmetadata|size',
            'format': 'json'
        }
        
        result = self._make_request(self.commons_api, params)
        if not result or 'query' not in result or 'pages' not in result['query']:
            return None
        
        pages = result['query']['pages']
        for page_id, page_data in pages.items():
            if page_id == '-1':
                continue
            
            if 'imageinfo' not in page_data or not page_data['imageinfo']:
                continue
            
            img_info = page_data['imageinfo'][0]
            width = img_info.get('width', 0)
            height = img_info.get('height', 0)
            
            # Skip images smaller than 600px
            if max(width, height) < 600:
                return None
            
            extmeta = img_info.get('extmetadata', {})
            
            metadata = {
                'file_name': f"File:{filename}",
                'image_url': img_info.get('url'),
                'width': width,
                'height': height,
                'license': extmeta.get('LicenseShortName', {}).get('value', 'Unknown'),
                'author': extmeta.get('Artist', {}).get('value', 'Unknown'),
                'description': extmeta.get('ImageDescription', {}).get('value')
            }
            
            self.cache[cache_key] = metadata
            return metadata
        
        return None
    
    def find_images(self, title: str, slug: str = None, max_images: int = 7) -> List[Dict]:
        """Find Wikimedia images for a content item"""
        images = []
        queries = []
        seen_files = set()
        
        if title:
            queries.append(title.strip())
        if slug and slug != title:
            slug_query = slug.replace('-', ' ').replace('_', ' ').title()
            queries.append(slug_query)
        
        if not queries:
            return []
        
        for query in queries[:2]:
            try:
                search_results = self.search_wikipedia(query, limit=5)
                
                for result in search_results:
                    if len(images) >= max_images:
                        break
                    
                    page_id = result.get('pageid')
                    if not page_id:
                        continue
                    
                    page_images = self.get_page_images(page_id, limit=max_images)
                    for filename, page_title in page_images:
                        if len(images) >= max_images:
                            break
                        if filename in seen_files:
                            continue
                        
                        metadata = self.get_commons_metadata(filename)
                        if not metadata:
                            continue
                        
                        seen_files.add(filename)
                        image_entry = {
                            'role': 'augment',
                            'source': 'wikimedia_commons',
                            'wiki_page_title': page_title,
                            'file_name': metadata['file_name'],
                            'image_url': metadata['image_url'],
                            'width': metadata['width'],
                            'height': metadata['height'],
                            'license': metadata['license'],
                            'author': metadata['author'],
                            'description': metadata['description'],
                            'retrieved_at': datetime.utcnow().isoformat() + 'Z'
                        }
                        
                        images.append(image_entry)
                        if len(images) >= max_images:
                            break
                    
                    if images:
                        break
            
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error searching for '{query}': {e}")
                continue
        
        return images

    def augment_record(self, record: Dict) -> Dict:
        """Augment a record with Wikipedia images"""
        title = record.get('title', '').strip()
        slug = record.get('slug', '').strip()
        
        if not title and not slug:
            record['external_images'] = []  # Return list, not json string here
            return record
        
        try:
            images = self.find_images(title, slug, max_images=7)
            record['external_images'] = images # Return list, pipeline handles json dump
            
            if images and self.logger:
                self.logger.debug(f"Augmented '{title or slug}' with {len(images)} images")
        
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to augment '{title or slug}': {e}")
            record['external_images'] = []
            
        return record

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to Wikipedia API."""
        try:
            images = self.search_wikipedia("test", limit=1)
            return True, f"Wikipedia API connected - found {len(images)} test pages"
        except Exception as e:
            return False, f"Wikipedia API failed: {str(e)}"
