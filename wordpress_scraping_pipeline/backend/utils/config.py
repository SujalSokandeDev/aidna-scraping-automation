"""
Configuration Module
Loads settings from .env file and provides WordPress sites registry.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv


# WordPress Sites Registry
WORDPRESS_SITES: Dict[str, Dict[str, str]] = {
    "FashionABC": {
        "url": "https://www.fashionabc.org/",
        "sitemap": "https://www.fashionabc.org/sitemap_index.xml",
        "source_tag": "WordPress/FashionABC"
    },
    "FreedomX": {
        "url": "https://freedomx.com/",
        "sitemap": "https://freedomx.com/sitemap_index.xml",
        "source_tag": "WordPress/FreedomX"
    },
    "HedgeThink": {
        "url": "http://www.hedgethink.com/",
        "sitemap": "http://www.hedgethink.com/sitemap_index.xml",
        "source_tag": "WordPress/HedgeThink"
    },
    "IntelligentHQ": {
        "url": "https://www.intelligenthq.com/",
        "sitemap": "https://www.intelligenthq.com/sitemap_index.xml",
        "source_tag": "WordPress/IntelligentHQ"
    },
    "TradersDNA": {
        "url": "http://www.tradersdna.com/",
        "sitemap": "http://www.tradersdna.com/sitemap_index.xml",
        "source_tag": "WordPress/TradersDNA"
    }
}


@dataclass
class Config:
    """Central configuration for the WordPress scraping pipeline."""
    
    # BigQuery
    gcp_project_id: str = ""
    bigquery_dataset: str = "strapi_content_data"
    bigquery_table: str = "unified_all_cms_content"
    google_credentials_path: str = ""
    
    # Scraping
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    request_timeout: int = 30
    request_delay: float = 1.5
    max_retries: int = 3
    
    # Wikimedia
    wikimedia_api_key: str = ""
    wikimedia_max_images: int = 2
    
    # Flask
    flask_host: str = "0.0.0.0"
    flask_port: int = 5000
    flask_debug: bool = True
    
    # Supabase
    supabase_url: str = ""
    supabase_key: str = ""
    
    # Database paths
    scraped_urls_db: str = ""
    checkpoints_db: str = ""
    
    # Logging
    log_level: str = "INFO"
    log_file: str = ""
    
    # Batch processing
    insert_batch_size: int = 20
    checkpoint_interval: int = 50
    
    # Base directory (set during load)
    base_dir: Path = field(default_factory=Path)
    
    @classmethod
    def load(cls, env_path: str = None) -> 'Config':
        """Load configuration from .env file."""
        # Find base directory
        base_dir = Path(__file__).parent.parent.parent
        
        # Load .env file
        if env_path:
            load_dotenv(env_path)
        else:
            env_file = base_dir / ".env"
            if env_file.exists():
                load_dotenv(env_file)
        
        # Create data and logs directories
        data_dir = base_dir / "data"
        logs_dir = base_dir / "logs"
        data_dir.mkdir(exist_ok=True)
        logs_dir.mkdir(exist_ok=True)
        
        return cls(
            # BigQuery
            gcp_project_id=os.getenv("GCP_PROJECT_ID", ""),
            bigquery_dataset=os.getenv("BIGQUERY_DATASET", "strapi_content_data"),
            bigquery_table=os.getenv("BIGQUERY_TABLE", "unified_all_cms_content"),
            google_credentials_path=os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS", 
                str(data_dir / "service-account.json")
            ),
            
            # Scraping
            user_agent=os.getenv(
                "USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            request_delay=float(os.getenv("REQUEST_DELAY", "1.5")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            
            # Wikimedia
            wikimedia_api_key=os.getenv("WIKIMEDIA_API_KEY", ""),
            wikimedia_max_images=int(os.getenv("WIKIMEDIA_MAX_IMAGES", "2")),
            
            # Flask
            flask_host=os.getenv("FLASK_HOST", "0.0.0.0"),
            flask_port=int(os.getenv("FLASK_PORT", "5000")),
            flask_debug=os.getenv("FLASK_DEBUG", "True").lower() == "true",
            
            # Database paths
            scraped_urls_db=os.getenv(
                "SCRAPED_URLS_DB",
                str(data_dir / "scraped_urls.db")
            ),
            checkpoints_db=os.getenv(
                "CHECKPOINTS_DB", 
                str(data_dir / "checkpoints.db")
            ),
            
            # Logging
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv(
                "LOG_FILE",
                str(logs_dir / "wordpress_pipeline.log")
            ),
            
            # Batch processing
            insert_batch_size=int(os.getenv("INSERT_BATCH_SIZE", "20")),
            checkpoint_interval=int(os.getenv("CHECKPOINT_INTERVAL", "50")),
            
            # Supabase
            supabase_url=os.getenv("SUPABASE_URL", ""),
            supabase_key=os.getenv("SUPABASE_KEY", ""),
            
            # Base directory
            base_dir=base_dir
        )
    
    def validate(self) -> list:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.gcp_project_id:
            errors.append("GCP_PROJECT_ID is required for BigQuery operations")
        
        if not Path(self.google_credentials_path).exists():
            errors.append(f"Service account file not found: {self.google_credentials_path}")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for logging/display."""
        return {
            "gcp_project_id": self.gcp_project_id,
            "bigquery_dataset": self.bigquery_dataset,
            "bigquery_table": self.bigquery_table,
            "request_delay": self.request_delay,
            "max_retries": self.max_retries,
            "wikimedia_max_images": self.wikimedia_max_images,
            "insert_batch_size": self.insert_batch_size,
            "log_level": self.log_level
        }
