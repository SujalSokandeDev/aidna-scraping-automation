"""
Content Scraper
Extracts article content and metadata from WordPress pages.
"""

import re
import time
import hashlib
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


class ContentScraper:
    """Scrapes and extracts content from WordPress article pages."""
    
    # Common article container selectors (in order of preference - most specific first)
    ARTICLE_SELECTORS = [
        '.entry-content',           # Most common WordPress content wrapper
        '.post-content',            # Alternative WordPress content
        '.article-content',
        '.post-body',
        '.article-body',
        '.content-body',
        '.td-post-content',         # Theme-specific (flavor theme)
        '.jeg_inner_content',       # JNews theme
        '.single-content',
        '.article__content',
        'article .content',
        'article .entry',
        '.single-post-content',
        '.post-entry',
        '.the-content',
        '[itemprop="articleBody"]', # Schema.org markup
        'article',                   # Generic article tag (gets more than just content)
        '#content .entry',
        '#content',
        'main .content',
        'main',
    ]
    
    def __init__(self, user_agent: str = None, timeout: int = 30,
                 max_retries: int = 3, delay: float = 1.5):
        """
        Initialize the content scraper.
        
        Args:
            user_agent: User agent string for requests
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            delay: Delay between requests in seconds
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.delay = delay
        
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry adapter."""
        session = requests.Session()
        
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        return session
    
    def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch page HTML content.
        
        Args:
            url: URL to fetch
        
        Returns:
            HTML content as string, or None if failed
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            raise Exception(f"Failed to fetch page: {e}")
    
    def extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Extract metadata from page (og tags, meta tags, etc).
        
        Args:
            soup: BeautifulSoup object of the page
            url: Page URL
        
        Returns:
            Dictionary with metadata
        """
        metadata = {
            'title': None,
            'description': None,
            'published_date': None,
            'modified_date': None,
            'author': None,
            'feature_image': None,
            'locale': 'en',
        }
        
        # Title
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'):
            metadata['title'] = og_title['content'].strip()
        else:
            title_tag = soup.find('title')
            if title_tag:
                metadata['title'] = title_tag.get_text(strip=True)
            else:
                h1 = soup.find('h1')
                if h1:
                    metadata['title'] = h1.get_text(strip=True)
        
        # Description
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            metadata['description'] = og_desc['content'].strip()
        else:
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                metadata['description'] = meta_desc['content'].strip()
        
        # Published date
        published_meta = soup.find('meta', property='article:published_time')
        if published_meta and published_meta.get('content'):
            metadata['published_date'] = published_meta['content']
        else:
            # Look for time element
            time_elem = soup.find('time', class_=re.compile(r'published|entry-date'))
            if time_elem and time_elem.get('datetime'):
                metadata['published_date'] = time_elem['datetime']
        
        # Modified date
        modified_meta = soup.find('meta', property='article:modified_time')
        if modified_meta and modified_meta.get('content'):
            metadata['modified_date'] = modified_meta['content']
        
        # Author
        author_meta = soup.find('meta', attrs={'name': 'author'})
        if author_meta and author_meta.get('content'):
            metadata['author'] = author_meta['content'].strip()
        else:
            # Look for author link/span
            author_elem = soup.find(class_=re.compile(r'author|byline'))
            if author_elem:
                author_link = author_elem.find('a')
                if author_link:
                    metadata['author'] = author_link.get_text(strip=True)
                else:
                    metadata['author'] = author_elem.get_text(strip=True)
        
        # Feature image
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            img_url = og_image['content']
            metadata['feature_image'] = {
                'url': img_url if img_url.startswith('http') else urljoin(url, img_url),
                'alt': metadata.get('title', '')
            }
        
        # Locale
        og_locale = soup.find('meta', property='og:locale')
        if og_locale and og_locale.get('content'):
            metadata['locale'] = og_locale['content'][:2].lower()
        
        return metadata
    
    def find_article_content(self, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """
        Find the main article content container.
        
        Args:
            soup: BeautifulSoup object of the page
        
        Returns:
            BeautifulSoup object of article container, or None
        """
        for selector in self.ARTICLE_SELECTORS:
            article = soup.select_one(selector)
            if article:
                return article
        
        # Fallback: find largest text block
        return self._find_largest_content_block(soup)
    
    def _find_largest_content_block(self, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """Find the div with the most text content."""
        max_text_len = 0
        best_block = None
        
        for div in soup.find_all('div'):
            # Skip if it's header, footer, sidebar
            if div.get('class'):
                class_str = ' '.join(div.get('class', []))
                if any(x in class_str.lower() for x in ['header', 'footer', 'sidebar', 'nav', 'menu', 'widget']):
                    continue
            
            text = div.get_text(strip=True)
            if len(text) > max_text_len:
                max_text_len = len(text)
                best_block = div
        
        return best_block
    
    def extract_images(self, article: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
        """
        Extract all images from article content.
        
        Args:
            article: BeautifulSoup object of article container
            base_url: Base URL for resolving relative paths
        
        Returns:
            List of image dicts with 'url' and 'alt'
        """
        images = []
        seen_urls = set()
        
        for img in article.find_all('img'):
            # Get image URL
            img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
            if not img_url:
                continue
            
            # Make absolute URL
            if not img_url.startswith('http'):
                img_url = urljoin(base_url, img_url)
            
            # Skip duplicates
            if img_url in seen_urls:
                continue
            seen_urls.add(img_url)
            
            # Skip tiny images (likely icons)
            width = img.get('width')
            height = img.get('height')
            if width and height:
                try:
                    if int(width) < 100 or int(height) < 100:
                        continue
                except ValueError:
                    pass
            
            images.append({
                'url': img_url,
                'alt': img.get('alt', '').strip()
            })
        
        return images
    
    def extract_categories_tags(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """
        Extract categories and tags from the page.
        
        Args:
            soup: BeautifulSoup object of the page
        
        Returns:
            Dict with 'categories' and 'tags' lists
        """
        result = {'categories': [], 'tags': []}
        
        # Categories (look for category links)
        cat_links = soup.find_all('a', rel='category tag')
        if cat_links:
            result['categories'] = [a.get_text(strip=True) for a in cat_links]
        else:
            cat_container = soup.find(class_=re.compile(r'cat-links|categories'))
            if cat_container:
                for a in cat_container.find_all('a'):
                    result['categories'].append(a.get_text(strip=True))
        
        # Tags
        tag_links = soup.find_all('a', rel='tag')
        if tag_links:
            result['tags'] = [a.get_text(strip=True) for a in tag_links if 'category' not in str(a.get('rel', []))]
        else:
            tag_container = soup.find(class_=re.compile(r'tag-links|tags'))
            if tag_container:
                for a in tag_container.find_all('a'):
                    result['tags'].append(a.get_text(strip=True))
        
        # Remove duplicates while preserving order
        result['categories'] = list(dict.fromkeys(result['categories']))
        result['tags'] = list(dict.fromkeys(result['tags']))
        
        return result
    
    def extract_external_links(self, article: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
        """
        Extract external links from article as sources.
        
        Args:
            article: BeautifulSoup object of article container
            base_url: Base URL for determining external links
        
        Returns:
            List of source dicts with 'text' and 'url'
        """
        sources = []
        base_domain = urlparse(base_url).netloc
        seen_urls = set()
        
        for a in article.find_all('a', href=True):
            href = a['href']
            
            # Skip internal links
            if not href.startswith('http'):
                continue
            
            link_domain = urlparse(href).netloc
            if link_domain == base_domain:
                continue
            
            # Skip duplicates
            if href in seen_urls:
                continue
            seen_urls.add(href)
            
            # Skip social media sharing links
            if any(x in href.lower() for x in ['facebook.com/share', 'twitter.com/intent', 'linkedin.com/share']):
                continue
            
            text = a.get_text(strip=True) or href
            sources.append({
                'text': text[:200],  # Limit text length
                'url': href
            })
        
        return sources[:20]  # Limit to 20 sources
    
    def extract_slug(self, url: str) -> str:
        """Extract slug from URL."""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        parts = path.split('/')
        return parts[-1] if parts else ''
    
    def scrape_url(self, url: str) -> Dict[str, Any]:
        """
        Scrape a single URL and extract all content.
        
        Args:
            url: URL to scrape
        
        Returns:
            Dictionary with all extracted content and metadata
        """
        # Fetch page
        html = self.fetch_page(url)
        if not html:
            raise Exception("Failed to fetch page")
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract metadata
        metadata = self.extract_metadata(soup, url)
        
        # Find article content
        article = self.find_article_content(soup)
        
        # Extract raw HTML content (for cleaning later)
        raw_html = str(article) if article else ''
        
        # Extract images
        images = self.extract_images(article, url) if article else []
        
        # Extract categories and tags
        cat_tags = self.extract_categories_tags(soup)
        
        # Extract external links as sources
        sources = self.extract_external_links(article, url) if article else []
        
        # Generate content hash for change detection
        content_hash = hashlib.md5(raw_html.encode()).hexdigest()
        
        return {
            'url': url,
            'slug': self.extract_slug(url),
            'title': metadata['title'],
            'description': metadata['description'],
            'raw_html': raw_html,
            'published_date': metadata['published_date'],
            'modified_date': metadata['modified_date'],
            'author': metadata['author'],
            'locale': metadata['locale'],
            'feature_image': metadata['feature_image'],
            'images': images,
            'categories': cat_tags['categories'],
            'tags': cat_tags['tags'],
            'sources': sources,
            'content_hash': content_hash,
            'scraped_at': datetime.now().isoformat()
        }
    
    def scrape_urls_batch(self, urls: List[str], 
                          progress_callback: callable = None) -> List[Dict[str, Any]]:
        """
        Scrape multiple URLs with delays.
        
        Args:
            urls: List of URLs to scrape
            progress_callback: Optional callback for progress updates
        
        Returns:
            List of scraped content dicts
        """
        results = []
        
        for i, url in enumerate(urls):
            try:
                content = self.scrape_url(url)
                content['status'] = 'success'
                results.append(content)
            except Exception as e:
                results.append({
                    'url': url,
                    'status': 'failed',
                    'error': str(e)
                })
            
            if progress_callback:
                progress_callback(i + 1, len(urls), url)
            
            # Delay between requests
            if i < len(urls) - 1:
                time.sleep(self.delay)
        
        return results
