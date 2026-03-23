"""
Wikipedia Augmenter
Fetches relevant images from Wikipedia articles to augment scraped content.
"""

import re
import time
from typing import List, Dict, Optional, Any
from urllib.parse import quote

import requests


class WikimediaAugmenter:
    """Fetches Wikipedia article images to augment content."""
    
    # Wikipedia API endpoint
    API_URL = "https://en.wikipedia.org/w/api.php"
    
    # Common licenses mapping
    LICENSE_MAPPING = {
        'cc-by-sa-4.0': 'CC BY-SA 4.0',
        'cc-by-4.0': 'CC BY 4.0',
        'cc-by-sa-3.0': 'CC BY-SA 3.0',
        'cc-by-3.0': 'CC BY 3.0',
        'cc0': 'CC0 1.0',
        'public domain': 'Public Domain',
    }
    
    def __init__(self, max_images: int = 2, timeout: int = 10, rate_delay: float = 1.0):
        """
        Initialize the Wikipedia augmenter.
        
        Args:
            max_images: Maximum images to fetch per article
            timeout: Request timeout in seconds
            rate_delay: Delay between API calls in seconds (default 1.0 to avoid 429)
        """
        self.max_images = max_images
        self.timeout = timeout
        self.rate_delay = rate_delay
        self.last_request_time = 0
        self.max_retries = 3
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WordPressScraper/1.0 (Educational/Research Project)'
        })

    def _request_with_retry(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Perform a GET with basic backoff for 429s and transient failures."""
        for attempt in range(1, self.max_retries + 1):
            try:
                self._rate_limit()
                response = self.session.get(
                    self.API_URL,
                    params=params,
                    timeout=self.timeout
                )
                if response.status_code == 429:
                    # Backoff on rate limit responses
                    sleep_for = self.rate_delay * attempt
                    time.sleep(sleep_for)
                    continue
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt == self.max_retries:
                    print(f"Wikipedia request failed after retries: {e}")
                    return None
                time.sleep(self.rate_delay * attempt)
        return None
    
    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_delay:
            time.sleep(self.rate_delay - elapsed)
        self.last_request_time = time.time()
    
    def _extract_keywords(self, title: str) -> List[str]:
        """
        Extract keywords from article title for search.
        
        Args:
            title: Article title
        
        Returns:
            List of keywords
        """
        if not title:
            return []
        
        # Remove common words and punctuation
        stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
            'for', 'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were',
            'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'could', 'should', 'can', 'may', 'might',
            'this', 'that', 'these', 'those', 'it', 'its', 'how', 'why',
            'what', 'when', 'where', 'who', 'which', 'new', 'top',
        }
        
        # Clean title
        title = re.sub(r'[^\w\s]', ' ', title.lower())
        words = title.split()
        
        # Filter keywords
        keywords = [w for w in words if w not in stopwords and len(w) > 2]
        
        return keywords[:5]  # Limit to 5 keywords
    
    def search_wikipedia(self, query: str) -> List[str]:
        """
        Search Wikipedia for relevant article titles.
        
        Args:
            query: Search query
        
        Returns:
            List of Wikipedia article titles
        """
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': query,
                'srlimit': 3,  # Get top 3 articles
            }
            
            data = self._request_with_retry(params)
            if not data:
                return []
            
            if 'query' not in data or 'search' not in data['query']:
                return []
            
            titles = [result['title'] for result in data['query']['search']]
            return titles
        
        except Exception as e:
            print(f"Wikipedia search failed: {e}")
            return []
    
    def get_article_images(self, article_title: str) -> List[Dict[str, Any]]:
        """
        Get images from a specific Wikipedia article.
        
        Args:
            article_title: Wikipedia article title
        
        Returns:
            List of image dicts
        """
        try:
            # Get images from the article
            params = {
                'action': 'query',
                'format': 'json',
                'titles': article_title,
                'prop': 'images|pageimages|info',
                'inprop': 'url',
                'piprop': 'original|thumbnail',
                'pithumbsize': 800,
                'imlimit': 10,  # Get up to 10 image names
            }
            
            data = self._request_with_retry(params)
            if not data:
                return []
            
            if 'query' not in data or 'pages' not in data['query']:
                return []
            
            images = []
            for page_id, page_data in data['query']['pages'].items():
                if page_id == '-1':  # Page not found
                    continue
                
                article_url = page_data.get('fullurl', '')
                
                # Get the main page image (thumbnail)
                if 'original' in page_data:
                    images.append({
                        'role': 'augment',
                        'source': 'wikipedia',
                        'image_url': page_data.get('thumbnail', {}).get('source', page_data['original']['source']),
                        'full_url': page_data['original']['source'],
                        'license': 'Wikipedia',
                        'title': article_title,
                        'description': f'Image from Wikipedia article: {article_title}',
                        'article_url': article_url,
                    })
                
                # Get additional images from the article
                if 'images' in page_data:
                    for img in page_data['images'][:5]:  # Limit to first 5
                        img_title = img.get('title', '')
                        # Skip common non-content images
                        if any(skip in img_title.lower() for skip in 
                               ['icon', 'logo', 'commons-logo', 'wiki', 'symbol', 'flag', 'edit', 'portal']):
                            continue
                        
                        # Get image info
                        img_info = self._get_image_info(img_title)
                        if img_info:
                            img_info['article_url'] = article_url
                            images.append(img_info)
                        
                        if len(images) >= self.max_images:
                            break
            
            return images[:self.max_images]
        
        except Exception as e:
            print(f"Failed to get article images: {e}")
            return []
    
    def _get_image_info(self, image_title: str) -> Optional[Dict[str, Any]]:
        """Get detailed info about a specific image."""
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'titles': image_title,
                'prop': 'imageinfo',
                'iiprop': 'url|extmetadata',
                'iiurlwidth': 800,
            }
            
            data = self._request_with_retry(params)
            if not data:
                return None
            
            if 'query' not in data or 'pages' not in data['query']:
                return None
            
            for page_id, page_data in data['query']['pages'].items():
                if page_id == '-1' or 'imageinfo' not in page_data:
                    continue
                
                info = page_data['imageinfo'][0]
                extmeta = info.get('extmetadata', {})
                
                # Get license
                license_short = extmeta.get('LicenseShortName', {}).get('value', 'Wikipedia')
                
                # Get description
                desc = extmeta.get('ImageDescription', {}).get('value', '')
                if desc:
                    desc = re.sub(r'<[^>]+>', '', desc)[:200]
                
                return {
                    'role': 'augment',
                    'source': 'wikipedia',
                    'image_url': info.get('thumburl') or info.get('url'),
                    'full_url': info.get('url'),
                    'license': license_short,
                    'title': image_title.replace('File:', ''),
                    'description': desc or f'Image from Wikipedia',
                }
            
            return None
        
        except Exception:
            return None
    
    def get_images_for_article(self, title: str, 
                               categories: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get Wikipedia images relevant to an article.
        
        Args:
            title: Article title
            categories: Article categories for additional context
        
        Returns:
            List of image dicts for external_images field
        """
        if not title:
            return []
        
        # Extract keywords
        keywords = self._extract_keywords(title)
        
        # Add first category if available
        if categories and categories[0]:
            cat_keywords = self._extract_keywords(categories[0])
            keywords.extend(cat_keywords[:2])
        
        if not keywords:
            return []
        
        # Build search query
        query = ' '.join(keywords[:4])
        
        # Search for relevant Wikipedia articles
        wiki_titles = self.search_wikipedia(query)
        
        if not wiki_titles:
            # Try with fewer keywords
            query = ' '.join(keywords[:2])
            wiki_titles = self.search_wikipedia(query)
        
        if not wiki_titles:
            return []
        
        # Get images from the first matching Wikipedia article
        all_images = []
        for wiki_title in wiki_titles[:2]:  # Check top 2 articles
            images = self.get_article_images(wiki_title)
            all_images.extend(images)
            if len(all_images) >= self.max_images:
                break
        
        return all_images[:self.max_images]
    
    def augment_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add Wikipedia images to article data.
        
        Args:
            article: Article data dict
        
        Returns:
            Article with external_images added
        """
        title = article.get('title', '')
        categories = article.get('categories', [])
        
        images = self.get_images_for_article(title, categories)
        
        article['external_images'] = images
        
        return article
