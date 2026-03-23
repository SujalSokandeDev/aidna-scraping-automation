"""
Configuration Module for CMS Incremental Pipeline
Loads settings from .env file and provides CMS source definitions.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List
from dotenv import load_dotenv


# =============================================================================
# CMS SOURCE DEFINITIONS
# =============================================================================

CMS_SOURCES = {
    "businessabc": {
        "source_tag": "BusinessABC/CitiesABC",
        "collections": ["post", "city", "company", "education", "investor", "place", "influencer", "knowledgebase"]
    },
    "sportsabc": {
        "source_tag": "SportsABC",
        "collections": ["athletes", "teams", "stadiums", "sports", "federations", "nationality", "post"]
    }
}


# =============================================================================
# CONFIGURATION CLASS
# =============================================================================

@dataclass
class Config:
    """Configuration for the CMS Incremental Pipeline"""
    
    # Base paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    
    # BusinessABC CMS
    businessabc_url: str = ""
    businessabc_token: str = ""
    
    # SportsABC CMS
    sportsabc_url: str = ""
    sportsabc_token: str = ""
    
    # BigQuery
    gcp_project_id: str = ""
    bigquery_dataset: str = "strapi_content_data"
    bigquery_table: str = "unified_cms_content_combined"
    google_credentials_path: str = ""
    
    # Pipeline settings
    page_size: int = 100
    request_delay: float = 0.1
    max_retries: int = 3
    insert_batch_size: int = 100
    
    # Logging
    log_level: str = "INFO"
    log_file: str = ""
    
    # Flask
    flask_port: int = 5001
    flask_debug: bool = False
    
    # Supabase
    supabase_url: str = ""
    supabase_key: str = ""
    
    @classmethod
    def load(cls) -> "Config":
        """Load configuration from environment variables"""
        base_dir = Path(__file__).parent.parent.parent
        
        # Try to load .env file
        env_path = base_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        
        data_dir = base_dir / "data"
        logs_dir = base_dir / "logs"
        
        # Ensure directories exist
        data_dir.mkdir(exist_ok=True)
        logs_dir.mkdir(exist_ok=True)
        
        return cls(
            base_dir=base_dir,
            # BusinessABC
            businessabc_url=os.getenv("BUSINESSABC_STRAPI_GRAPHQL_URL", "https://cms.businessabc.net/graphql"),
            businessabc_token=os.getenv("BUSINESSABC_STRAPI_BEARER_TOKEN", ""),
            # SportsABC
            sportsabc_url=os.getenv("SPORTSABC_STRAPI_GRAPHQL_URL", "https://cms.sportsabc.org/graphql"),
            sportsabc_token=os.getenv("SPORTSABC_STRAPI_BEARER_TOKEN", ""),
            # BigQuery
            gcp_project_id=os.getenv("GCP_PROJECT_ID", "ztudiumplatforms"),
            bigquery_dataset=os.getenv("BIGQUERY_DATASET", "strapi_content_data"),
            bigquery_table=os.getenv("BIGQUERY_TABLE", "unified_cms_only"),
            google_credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", str(data_dir / "service-account.json")),
            # Pipeline
            page_size=int(os.getenv("PAGE_SIZE", "100")),
            request_delay=float(os.getenv("REQUEST_DELAY", "0.1")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            insert_batch_size=int(os.getenv("INSERT_BATCH_SIZE", "100")),
            # Logging
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", str(logs_dir / "cms_pipeline.log")),
            # Flask
            flask_port=int(os.getenv("FLASK_PORT", "5001")),
            flask_debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
            # Supabase
            supabase_url=os.getenv("SUPABASE_URL", ""),
            supabase_key=os.getenv("SUPABASE_KEY", ""),
        )
    
    def validate(self) -> List[str]:
        """Validate required configuration"""
        errors = []
        
        if not self.businessabc_token:
            errors.append("BUSINESSABC_STRAPI_BEARER_TOKEN is required")
        if not self.sportsabc_token:
            errors.append("SPORTSABC_STRAPI_BEARER_TOKEN is required")
        if not self.gcp_project_id:
            errors.append("GCP_PROJECT_ID is required")
        
        creds_path = Path(self.google_credentials_path)
        if not creds_path.exists():
            errors.append(f"Service account file not found: {self.google_credentials_path}")
        
        return errors
    
    @property
    def data_dir(self) -> Path:
        return self.base_dir / "data"
    
    @property
    def records_db_path(self) -> str:
        return str(self.data_dir / "cms_records.db")
    
    @property
    def checkpoints_db_path(self) -> str:
        return str(self.data_dir / "checkpoints.db")
