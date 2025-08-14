"""
Configuration management for DataCenterMap scraper.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Application configuration."""
    
    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql+psycopg://projectseer:projectseer@localhost:5432/projectseer"
    )
    
    # Scraper settings
    USER_AGENT: str = os.getenv(
        "USER_AGENT", 
        "ProjectSeerBot/0.1 (+projectseerai@gmail.com)"
    )
    RATE_LIMIT_DELAY: float = float(os.getenv("RATE_LIMIT_DELAY", "1.0"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    
    # Playwright-specific settings
    PLAYWRIGHT_TIMEOUT: int = int(os.getenv("PLAYWRIGHT_TIMEOUT", "120000"))  # 120 seconds in ms (increased for facility processing)
    VERCEL_CHALLENGE_TIMEOUT: int = int(os.getenv("VERCEL_CHALLENGE_TIMEOUT", "90000"))  # 90 seconds in ms
    PLAYWRIGHT_HEADLESS: bool = os.getenv("PLAYWRIGHT_HEADLESS", "false").lower() == "true"
    
    # Export settings
    DEFAULT_EXPORT_DIR: Path = Path(os.getenv("DEFAULT_EXPORT_DIR", "exports"))
    
    # Development
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # DataCenterMap URLs
    BASE_URL: str = "https://www.datacentermap.com"
    USA_URL: str = f"{BASE_URL}/usa/"
    
    @classmethod
    def validate(cls) -> None:
        """Validate configuration settings."""
        if not cls.DATABASE_URL:
            raise ValueError("DATABASE_URL is required")
        
        if cls.RATE_LIMIT_DELAY < 0.1:
            raise ValueError("RATE_LIMIT_DELAY must be at least 0.1 seconds")
        
        if cls.MAX_RETRIES < 1:
            raise ValueError("MAX_RETRIES must be at least 1")
        
        # Ensure export directory exists
        cls.DEFAULT_EXPORT_DIR.mkdir(exist_ok=True)


# Initialize configuration
config = Config()