"""
Logging Module
Configures logging with custom format for the pipeline.
Format: [TIMESTAMP] [LEVEL] [SOURCE] | Message
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler


# Custom formatter for consistent log format
class PipelineFormatter(logging.Formatter):
    """Custom formatter matching the required log format."""
    
    def format(self, record: logging.LogRecord) -> str:
        # Get the source from the extra dict or use the logger name
        source = getattr(record, 'source', record.name)
        
        # Format timestamp
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        
        # Format the message
        message = record.getMessage()
        
        # Build the formatted line
        return f"[{timestamp}] [{record.levelname}] [{source}] | {message}"


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup logging for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup log files to keep
    
    Returns:
        Configured logger instance
    """
    # Create root logger for pipeline
    logger = logging.getLogger("wordpress_pipeline")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = PipelineFormatter()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if path provided)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = "wordpress_pipeline") -> logging.Logger:
    """Get a logger instance with the given name."""
    return logging.getLogger(name)


class SourceAdapter(logging.LoggerAdapter):
    """Logger adapter that adds source information to log records."""
    
    def __init__(self, logger: logging.Logger, source: str):
        super().__init__(logger, {'source': source})
    
    def process(self, msg, kwargs):
        # Add source to the extra dict
        kwargs.setdefault('extra', {})['source'] = self.extra['source']
        return msg, kwargs


def get_source_logger(source: str) -> SourceAdapter:
    """
    Get a logger adapter with a specific source tag.
    
    Args:
        source: Source identifier (e.g., "WordPress/FashionABC")
    
    Returns:
        Logger adapter with source tag
    """
    logger = get_logger()
    return SourceAdapter(logger, source)
