"""
Logging configuration for DataCenterMap scraper.
"""

import logging
import sys
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

from .config import config


def setup_logging(level: Optional[str] = None) -> logging.Logger:
    """Setup application logging with rich formatting."""
    
    # Use provided level or config default
    log_level = level or config.LOG_LEVEL
    
    # Create console for rich output
    console = Console(stderr=True)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=console,
                show_path=False,
                rich_tracebacks=True,
                tracebacks_show_locals=True,
            )
        ],
    )
    
    # Get application logger
    logger = logging.getLogger("dcm")
    
    # Quiet down some noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    
    return logger


# Global logger instance
logger = setup_logging()