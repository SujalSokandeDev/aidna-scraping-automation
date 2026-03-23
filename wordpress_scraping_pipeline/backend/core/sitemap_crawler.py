"""
Sitemap Crawler
Parses WordPress sitemaps and filters URLs for content pages.
"""

import re
import time
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# URL patterns to skip (taxonomy/navigation pages)
SKIP_PATTERNS = [
    r'/tag/',
    r'/list-tags/',
    r'/category/',
    r'/author/',
    r'/page/\d+',
    r'\?',  # Query parameters
    r'/feed/?$',
    r'/wp-json/',
    r'/wp-admin/',
    r'/wp-content/',
    r'/wp-includes/',
]

# Compiled regex for performance
SKIP_REGEX = re.compile('|'.join(SKIP_PATTERNS), re.IGNORECASE)


class SitemapCrawler:
    """Crawls WordPress sitemaps and extracts article URLs."""
    
    # Sitemap XML namespace
    SITEMAP_NS = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    def __init__(self, user_agent: str = None, timeout: int = 30, 
                 max_retries: int = 3, delay: float = 0.5):
        """
        Initialize the sitemap crawler.
        
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
        
        # Setup session with retry logic
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
            'Connection': 'keep-alive',
        })
        
        return session
    
    def fetch_sitemap(self, url: str) -> Optional[str]:
        """
        Fetch sitemap XML content.
        
        Args:
            url: Sitemap URL
        
        Returns:
            XML content as string, or None if failed
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Failed to fetch sitemap {url}: {e}")
            return None
    
    def parse_sitemap_index(self, xml_content: str) -> List[str]:
        """
        Parse sitemap index to get list of sub-sitemaps.
        
        Args:
            xml_content: XML content of sitemap index
        
        Returns:
            List of sitemap URLs
        """
        try:
            root = ET.fromstring(xml_content)
            
            # Check if it's a sitemap index
            sitemaps = root.findall('.//ns:sitemap/ns:loc', self.SITEMAP_NS)
            if sitemaps:
                return [s.text.strip() for s in sitemaps if s.text]
            
            # If not an index, return empty (will be parsed as regular sitemap)
            return []
        
        except ET.ParseError as e:
            print(f"Failed to parse sitemap index: {e}")
            return []
    
    def parse_sitemap_urls(self, xml_content: str) -> List[Dict[str, str]]:
        """
        Parse sitemap to extract URLs and metadata.
        
        Args:
            xml_content: XML content of sitemap
        
        Returns:
            List of dicts with 'url' and 'lastmod' keys
        """
        urls_data = []
        
        try:
            root = ET.fromstring(xml_content)
            
            for url_elem in root.findall('.//ns:url', self.SITEMAP_NS):
                loc = url_elem.find('ns:loc', self.SITEMAP_NS)
                lastmod = url_elem.find('ns:lastmod', self.SITEMAP_NS)
                
                if loc is not None and loc.text:
                    urls_data.append({
                        'url': loc.text.strip(),
                        'lastmod': lastmod.text.strip() if lastmod is not None and lastmod.text else None
                    })
        
        except ET.ParseError as e:
            print(f"Failed to parse sitemap URLs: {e}")
        
        return urls_data
    
    def should_skip_url(self, url: str) -> bool:
        """
        Check if URL should be skipped (taxonomy/navigation page).
        
        Args:
            url: URL to check
        
        Returns:
            True if URL should be skipped
        """
        return bool(SKIP_REGEX.search(url))
    
    def filter_content_urls(self, urls: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Filter URLs to keep only content pages.
        
        Args:
            urls: List of URL dicts from sitemap
        
        Returns:
            Filtered list of content URLs
        """
        content_urls = []
        
        for url_data in urls:
            url = url_data['url']
            
            # Skip taxonomy/navigation URLs
            if self.should_skip_url(url):
                continue
            
            # Skip homepage (empty path)
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            if not path:
                continue
            
            content_urls.append(url_data)
        
        return content_urls
    
    def crawl_sitemap(self, sitemap_url: str, filter_urls: bool = True) -> List[Dict[str, str]]:
        """
        Crawl a single sitemap and extract URLs.
        
        Args:
            sitemap_url: URL of sitemap to crawl
            filter_urls: Whether to filter out taxonomy URLs
        
        Returns:
            List of URL dicts with 'url' and 'lastmod'
        """
        all_urls = []
        
        # Fetch main sitemap
        xml_content = self.fetch_sitemap(sitemap_url)
        if not xml_content:
            return []
        
        # Check if it's a sitemap index
        sub_sitemaps = self.parse_sitemap_index(xml_content)
        
        if sub_sitemaps:
            # It's a sitemap index - crawl each sub-sitemap
            for sub_url in sub_sitemaps:
                time.sleep(self.delay)
                
                sub_xml = self.fetch_sitemap(sub_url)
                if sub_xml:
                    urls = self.parse_sitemap_urls(sub_xml)
                    all_urls.extend(urls)
        else:
            # It's a regular sitemap
            urls = self.parse_sitemap_urls(xml_content)
            all_urls.extend(urls)
        
        # Filter URLs if requested
        if filter_urls:
            all_urls = self.filter_content_urls(all_urls)
        
        return all_urls
    
    def crawl_site(self, site_name: str, sitemap_url: str, 
                   filter_urls: bool = True) -> Dict[str, any]:
        """
        Crawl a WordPress site's sitemap.
        
        Args:
            site_name: Name of the site (for logging)
            sitemap_url: URL of the sitemap index
            filter_urls: Whether to filter out taxonomy URLs
        
        Returns:
            Dict with crawl results:
            - urls: List of URL dicts
            - total_found: Total URLs found in sitemap
            - total_filtered: Number of URLs after filtering
            - skipped: Number of URLs skipped
        """
        print(f"Crawling sitemap for {site_name}: {sitemap_url}")
        
        # Get all URLs
        all_urls = []
        
        xml_content = self.fetch_sitemap(sitemap_url)
        if not xml_content:
            return {
                'urls': [],
                'total_found': 0,
                'total_filtered': 0,
                'skipped': 0,
                'error': f"Failed to fetch sitemap: {sitemap_url}"
            }
        
        # Check if it's a sitemap index
        sub_sitemaps = self.parse_sitemap_index(xml_content)
        
        if sub_sitemaps:
            print(f"  Found {len(sub_sitemaps)} sub-sitemaps")
            for i, sub_url in enumerate(sub_sitemaps):
                time.sleep(self.delay)
                
                sub_xml = self.fetch_sitemap(sub_url)
                if sub_xml:
                    urls = self.parse_sitemap_urls(sub_xml)
                    all_urls.extend(urls)
                    print(f"  [{i+1}/{len(sub_sitemaps)}] {len(urls)} URLs from {sub_url.split('/')[-1]}")
        else:
            urls = self.parse_sitemap_urls(xml_content)
            all_urls.extend(urls)
        
        total_found = len(all_urls)
        
        # Filter URLs
        if filter_urls:
            filtered_urls = self.filter_content_urls(all_urls)
        else:
            filtered_urls = all_urls
        
        total_filtered = len(filtered_urls)
        skipped = total_found - total_filtered
        
        print(f"  Total: {total_found} URLs | Content: {total_filtered} | Skipped: {skipped}")
        
        return {
            'urls': filtered_urls,
            'total_found': total_found,
            'total_filtered': total_filtered,
            'skipped': skipped
        }
    
    def get_url_list(self, urls_data: List[Dict[str, str]]) -> List[str]:
        """Extract just the URL strings from URL data list."""
        return [u['url'] for u in urls_data]
