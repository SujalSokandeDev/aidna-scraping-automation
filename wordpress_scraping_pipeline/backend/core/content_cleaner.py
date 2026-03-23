"""
Content Cleaner
Converts HTML content to clean plain text.
"""

import re
import html
from typing import Optional

from bs4 import BeautifulSoup, Tag


class ContentCleaner:
    """Cleans HTML content and converts to plain text."""
    
    # Elements to remove completely (including their content)
    REMOVE_ELEMENTS = [
        'script', 'style', 'noscript', 'iframe', 'form', 'button', 'input',
        'svg', 'canvas', 'video', 'audio', 'map',
    ]
    
    # Classes/IDs that indicate non-content areas (use word boundaries to avoid false matches)
    SKIP_PATTERNS = [
        r'\bsidebar\b', r'\bwidget\b', r'\bmenu\b', r'\bfooter\b',
        r'\bheader\b', r'\bcomment\b', r'\bsocial\b', r'\bshare\b', r'\brelated\b',
        r'\badvertisement\b', r'\bad-box\b', r'\bads-\b', r'\bpromo\b', r'\bbanner\b',
    ]
    
    # Minimum text length to protect an element from being removed
    MIN_CONTENT_TO_PROTECT = 500
    
    def __init__(self):
        """Initialize the content cleaner."""
        self.skip_regex = re.compile('|'.join(self.SKIP_PATTERNS), re.IGNORECASE)
    
    def _should_skip_element(self, element) -> bool:
        """Check if element should be skipped based on class/id."""
        try:
            if not isinstance(element, Tag):
                return False
            if element.attrs is None:
                return False
            
            # Never skip elements with significant text content
            text_len = len(element.get_text(strip=True))
            if text_len > self.MIN_CONTENT_TO_PROTECT:
                return False
            
            classes = ' '.join(element.get('class', []) or [])
            element_id = element.get('id', '') or ''
            return bool(self.skip_regex.search(classes) or self.skip_regex.search(element_id))
        except Exception:
            return False
    
    def clean_html(self, html_content: str, min_length: int = 100) -> str:
        """
        Convert HTML to clean plain text.
        
        Args:
            html_content: Raw HTML string
            min_length: Minimum length for valid content
        
        Returns:
            Cleaned plain text string
        """
        if not html_content:
            return ""
        
        # Parse HTML
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Remove unwanted elements
        for tag_name in self.REMOVE_ELEMENTS:
            for element in soup.find_all(tag_name):
                element.decompose()
        
        # Remove elements with skip patterns in class/id
        for element in soup.find_all(True):
            if self._should_skip_element(element):
                element.decompose()
        
        # Get text
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean up the text
        text = self._normalize_text(text)
        
        # Validate minimum length
        if len(text) < min_length:
            return ""
        
        return text
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text by cleaning up whitespace and special characters.
        
        Args:
            text: Raw text string
        
        Returns:
            Normalized text
        """
        # Decode HTML entities
        text = html.unescape(text)
        
        # Replace multiple whitespace with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove zero-width characters
        text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
        
        # Remove excessive punctuation patterns
        text = re.sub(r'[.]{3,}', '...', text)
        text = re.sub(r'[-]{3,}', '---', text)
        
        # Trim whitespace
        text = text.strip()
        
        return text
    
    def clean_title(self, title: str) -> str:
        """
        Clean and normalize article title.
        
        Args:
            title: Raw title string
        
        Returns:
            Cleaned title
        """
        if not title:
            return ""
        
        # Decode HTML entities
        title = html.unescape(title)
        
        # Remove site name suffix (common pattern: "Title - Site Name")
        title = re.sub(r'\s*[-|–—]\s*[^-|–—]+$', '', title)
        
        # Normalize whitespace
        title = re.sub(r'\s+', ' ', title).strip()
        
        return title
    
    def clean_description(self, description: str, max_length: int = 500) -> str:
        """
        Clean and truncate description.
        
        Args:
            description: Raw description string
            max_length: Maximum length
        
        Returns:
            Cleaned description
        """
        if not description:
            return ""
        
        # Remove HTML tags
        soup = BeautifulSoup(description, 'lxml')
        text = soup.get_text(separator=' ', strip=True)
        
        # Normalize
        text = self._normalize_text(text)
        
        # Truncate if needed
        if len(text) > max_length:
            text = text[:max_length].rsplit(' ', 1)[0] + '...'
        
        return text
    
    def extract_excerpt(self, content: str, max_length: int = 300) -> str:
        """
        Extract an excerpt from content.
        
        Args:
            content: Full content text
            max_length: Maximum excerpt length
        
        Returns:
            Excerpt string
        """
        if not content:
            return ""
        
        # Get first portion
        excerpt = content[:max_length * 2]  # Take extra for sentence finding
        
        # Try to end at sentence boundary
        sentence_end = max(
            excerpt.rfind('. ', 0, max_length),
            excerpt.rfind('! ', 0, max_length),
            excerpt.rfind('? ', 0, max_length)
        )
        
        if sentence_end > max_length // 2:
            excerpt = excerpt[:sentence_end + 1]
        elif len(excerpt) > max_length:
            excerpt = excerpt[:max_length].rsplit(' ', 1)[0] + '...'
        
        return excerpt.strip()
    
    def get_word_count(self, text: str) -> int:
        """Get word count of text."""
        if not text:
            return 0
        return len(text.split())
    
    def get_reading_time(self, text: str, wpm: int = 200) -> int:
        """
        Calculate estimated reading time in minutes.
        
        Args:
            text: Content text
            wpm: Words per minute
        
        Returns:
            Reading time in minutes (minimum 1)
        """
        word_count = self.get_word_count(text)
        reading_time = max(1, round(word_count / wpm))
        return reading_time
