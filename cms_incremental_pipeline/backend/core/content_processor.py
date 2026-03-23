"""
Content Processor for CMS Incremental Pipeline
Cleans, normalizes, and structures content for BigQuery insertion.
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup


class ContentProcessor:
    """
    Cleans and normalizes CMS content for storage.
    Handles HTML cleaning, text normalization, and record structuring.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the content processor.
        
        Args:
            logger: Optional logger instance
        """
        self.logger = logger
    
    def _log(self, level: str, message: str):
        """Log a message if logger is available."""
        if self.logger:
            getattr(self.logger, level)(message)
    
    @staticmethod
    def remove_html_tags(text: str) -> str:
        """Remove HTML tags from text."""
        if not text:
            return ""
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    
    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalize whitespace in text."""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def clean_text(self, text: str) -> str:
        """Complete text cleaning pipeline."""
        if not text:
            return ""
        text = self.remove_html_tags(text)
        text = self.normalize_whitespace(text)
        return text
    
    def extract_links_from_html(self, html_text: str) -> List[Dict]:
        """Extract all links from HTML text."""
        if not html_text or not isinstance(html_text, str):
            return []
        
        try:
            soup = BeautifulSoup(html_text, 'html.parser')
            links = []
            
            for a_tag in soup.find_all('a', href=True):
                link_text = a_tag.get_text(strip=True)
                link_url = a_tag['href']
                
                if link_url and link_url.strip():
                    links.append({
                        'text': link_text if link_text else link_url,
                        'url': link_url.strip()
                    })
            
            return links
        except Exception as e:
            self._log('debug', f"Error extracting links: {e}")
            return []
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[str]:
        """Parse timestamp string to ISO format."""
        if not timestamp_str:
            return None
        
        try:
            if 'T' in timestamp_str and 'Z' in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(timestamp_str)
            return dt.isoformat()
        except (ValueError, TypeError):
            return None
    
    def extract_media_info(self, media_data: Any) -> Dict:
        """Extract media information as dictionary."""
        if not media_data:
            return {}
        
        try:
            if isinstance(media_data, dict) and 'data' in media_data:
                media_attrs = media_data['data'].get('attributes', {}) if media_data['data'] else {}
                media_info = {}
                
                if media_attrs.get('url'):
                    media_info['url'] = media_attrs['url']
                if media_attrs.get('alternativeText'):
                    media_info['alternativeText'] = media_attrs['alternativeText']
                if media_attrs.get('caption'):
                    media_info['caption'] = self.clean_text(media_attrs['caption'])
                if media_attrs.get('width'):
                    media_info['width'] = media_attrs['width']
                if media_attrs.get('height'):
                    media_info['height'] = media_attrs['height']
                
                return media_info
        except Exception as e:
            self._log('debug', f"Error extracting media info: {e}")
        
        return {}
    
    def extract_relation_info(self, relation_data: Any) -> List[Dict]:
        """Extract relation information as list of dictionaries."""
        if not relation_data:
            return []
        
        try:
            if isinstance(relation_data, dict) and 'data' in relation_data:
                if isinstance(relation_data['data'], list):
                    items = []
                    for item in relation_data['data']:
                        if item and 'attributes' in item:
                            attrs = item['attributes']
                            relation_item = {}
                            for key in ['title', 'name', 'fullname', 'slug', 'country', 'countryCode']:
                                if key in attrs:
                                    relation_item[key] = attrs[key]
                            if relation_item:
                                items.append(relation_item)
                    return items
                elif relation_data['data'] and 'attributes' in relation_data['data']:
                    attrs = relation_data['data']['attributes']
                    relation_item = {}
                    for key in ['title', 'name', 'fullname', 'slug', 'country', 'countryCode']:
                        if key in attrs:
                            relation_item[key] = attrs[key]
                    return [relation_item] if relation_item else []
        except Exception as e:
            self._log('debug', f"Error extracting relation info: {e}")
        
        return []
    
    def extract_source_from_applications(self, attrs: Dict, cms_key: str) -> str:
        """
        Extract source value from applications field.
        
        Logic:
        - SportsABC: Always return "SDNA"
        - BusinessABC: 
            - Single app ID: return the ID as string (e.g., "3")
            - Multiple app IDs: return JSON array (e.g., "[3, 4]")
            - No app ID: return "0"
        
        Args:
            attrs: Record attributes containing applications field
            cms_key: CMS key ('businessabc' or 'sportsabc')
        
        Returns:
            Source string value
        """
        # SportsABC always uses "SDNA"
        if cms_key.lower() == 'sportsabc':
            return "SDNA"
        
        # BusinessABC - extract from applications field
        try:
            applications = attrs.get('applications', {})
            app_data = applications.get('data', [])
            
            if not app_data:
                return "0"
            
            # Extract all application IDs
            app_ids = []
            for app in app_data:
                if app and app.get('id'):
                    app_ids.append(str(app['id']))
            
            if not app_ids:
                return "0"
            elif len(app_ids) == 1:
                return app_ids[0]
            else:
                # Multiple IDs - return as JSON array
                return json.dumps([int(id) for id in app_ids])
                
        except Exception as e:
            self._log('debug', f"Error extracting source from applications: {e}")
            return "0"
    
    def extract_seo_info(self, seo_data: Any) -> Dict:
        """Extract SEO information."""
        if not seo_data:
            return {}
        
        try:
            seo_info = {}
            if seo_data.get('metaTitle'):
                seo_info['metaTitle'] = self.clean_text(seo_data['metaTitle'])
            if seo_data.get('metaDescription'):
                seo_info['metaDescription'] = self.clean_text(seo_data['metaDescription'])
            if seo_data.get('canonicalURL'):
                seo_info['canonicalURL'] = seo_data['canonicalURL']
            return seo_info
        except Exception:
            return {}
    
    def extract_attributes_json(self, attrs: Dict) -> Dict:
        """Extract attributes as JSON object with cleaned data."""
        attributes_json = {}
        
        # Important text fields to include
        important_fields = [
            'title', 'slug', 'description', 'content', 'summary', 'introduction',
            'business_tagline', 'place_tagline', 'headline', 'header_description',
            'biography', 'vision', 'mission', 'history', 'general_information',
            'city_name', 'government_type', 'mayor_name', 'timezone', 'headquarters',
            'established', 'occupation', 'known_for', 'residence', 'dean_name',
            'address', 'establishment_year', 'type', 'population_total',
            'AthleteDescription', 'fullDescription', 'career', 'awardsAndRecognition',
            'achievements', 'widgetFullName', 'widgetNickname', 'firstname', 'lastname',
            'fullname', 'tagline', 'birthdate', 'placeOfBirth', 'positions', 'currentTeam',
            'widgetHomeStadium', 'founded', 'owner', 'ownership', 'award', 'location',
            'capacity', 'size', 'keyType', 'competitions', 'focus', 'disciplines',
            'membership', 'president', 'country', 'countryCode', 'phone', 'email', 'website'
        ]
        
        social_media_fields = [
            'facebook_url', 'twitter_url', 'instagram_url', 'linkedin_url',
            'youtube_channel_url', 'youtube_video_url'
        ]
        
        # Process each attribute
        for key, value in attrs.items():
            # Skip system fields
            if key.endswith(('_id', 'locale', 'createdAt', 'updatedAt', 'publishedAt')):
                continue
            
            # Handle media fields
            if key in ['feature_image', 'featureImage', 'heroMedia', 'widgetPicture',
                      'cover_image', 'photo', 'business_logo', 'map_image']:
                media_info = self.extract_media_info(value)
                if media_info:
                    attributes_json[key] = media_info
                continue
            
            # Handle SEO
            if key == 'seo':
                seo_info = self.extract_seo_info(value)
                if seo_info:
                    attributes_json[key] = seo_info
                continue
            
            # Handle relations
            if isinstance(value, dict) and 'data' in value:
                relation_info = self.extract_relation_info(value)
                if relation_info:
                    attributes_json[key] = relation_info
                continue
            
            # Handle important text fields
            if key in important_fields + social_media_fields:
                if value is not None and value != "":
                    if isinstance(value, str):
                        text = self.clean_text(str(value))
                        if text:
                            attributes_json[key] = text
                    else:
                        attributes_json[key] = value
                continue
            
            # Skip complex nested objects
            if isinstance(value, (dict, list)) and value:
                continue
            
            # Include other non-empty values
            if value is not None and value != "":
                if isinstance(value, str):
                    text = self.clean_text(str(value))
                    if text:
                        attributes_json[key] = text
                else:
                    attributes_json[key] = value
        
        # Extract sources/links from HTML fields
        sources = self._extract_all_links(attrs)
        if sources:
            attributes_json['sources'] = sources
        
        return attributes_json
    
    def _extract_all_links(self, attrs: Dict) -> List[Dict]:
        """Extract all links from record."""
        all_links = []
        links_set = set()
        
        # HTML fields that might contain links
        html_fields = [
            'references', 'description', 'content', 'summary', 'history',
            'biography', 'introduction', 'AthleteDescription', 'fullDescription',
            'career', 'awardsAndRecognition', 'achievements', 'disciplines', 'membership'
        ]
        
        for field in html_fields:
            if field in attrs and attrs[field]:
                field_links = self.extract_links_from_html(str(attrs[field]))
                for link in field_links:
                    if link['url'] not in links_set:
                        links_set.add(link['url'])
                        all_links.append(link)
        
        # URL fields
        url_fields = ['website', 'facebook_url', 'twitter_url', 'instagram_url',
                     'linkedin_url', 'youtube_channel_url']
        
        for field in url_fields:
            if field in attrs and attrs[field]:
                url = str(attrs[field]).strip()
                if url and url not in links_set:
                    links_set.add(url)
                    display_name = field.replace('_', ' ').replace('url', '').strip().title()
                    all_links.append({'text': display_name, 'url': url})
        
        return all_links
    
    def structure_record(self, record: Dict, collection_type: str, 
                        source_tag: str, cms_key: str = None) -> Dict:
        """
        Structure and clean a record for BigQuery insertion.
        
        Args:
            record: Raw record from Strapi API
            collection_type: Type of collection (e.g., 'post', 'city')
            source_tag: Fallback source tag (used if cms_key not provided)
            cms_key: CMS key ('businessabc' or 'sportsabc') for dynamic source extraction
        
        Returns:
            Structured record ready for BigQuery
        """
        try:
            attrs = record.get('attributes', {})
            
            # Determine source: use dynamic extraction if cms_key provided
            if cms_key:
                source = self.extract_source_from_applications(attrs, cms_key)
            else:
                source = source_tag
            
            structured = {
                'id': str(record['id']),
                'source': source,
                'content_type': collection_type,
                'title': self.clean_text(attrs.get('title', '')),
                'slug': attrs.get('slug', ''),
                'attributes_json': self.extract_attributes_json(attrs),
                'locale': attrs.get('locale', 'en'),
                'published_at': self.parse_timestamp(attrs.get('publishedAt')),
                'updated_at': self.parse_timestamp(attrs.get('updatedAt')),
                'created_at': self.parse_timestamp(attrs.get('createdAt')),
                'processing_timestamp': datetime.now().isoformat()
            }
            
            return structured
            
        except Exception as e:
            self._log('error', f"Error structuring record {record.get('id')}: {str(e)}")
            raise
    
    def process_batch(self, records: List[Dict], collection_type: str,
                     source_tag: str, cms_key: str = None) -> List[Dict]:
        """
        Process a batch of records.
        
        Args:
            records: List of raw records from Strapi
            collection_type: Collection type
            source_tag: Fallback source tag
            cms_key: CMS key for dynamic source extraction
        
        Returns:
            List of structured records
        """
        processed = []
        for record in records:
            try:
                structured = self.structure_record(record, collection_type, source_tag, cms_key)
                processed.append(structured)
            except Exception as e:
                self._log('error', f"Failed to process record {record.get('id')}: {e}")
        return processed

