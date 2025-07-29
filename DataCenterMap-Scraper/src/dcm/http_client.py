"""
HTTP client with rate limiting and retry logic for datacentermap.com.
"""

import asyncio
from typing import Optional
from urllib.robotparser import RobotFileParser

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.core.config import config
from src.core.logging import logger


class DataCenterMapClient:
    """HTTP client for datacentermap.com with rate limiting and retry logic."""
    
    def __init__(self):
        self.session: Optional[httpx.AsyncClient] = None
        self.last_request_time = 0.0
        self.robots_parser: Optional[RobotFileParser] = None
        self._robots_checked = False
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Initialize the HTTP client session."""
        if self.session is None:
            self.session = httpx.AsyncClient(
                headers={
                    "User-Agent": config.USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                },
                timeout=config.REQUEST_TIMEOUT,
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )
            
            # Check robots.txt
            await self._check_robots_txt()
    
    async def close(self):
        """Close the HTTP client session."""
        if self.session:
            await self.session.aclose()
            self.session = None
    
    async def _check_robots_txt(self):
        """Check robots.txt to ensure we're allowed to crawl."""
        if self._robots_checked:
            return
        
        try:
            robots_url = f"{config.BASE_URL}/robots.txt"
            response = await self.session.get(robots_url)
            
            if response.status_code == 200:
                self.robots_parser = RobotFileParser()
                self.robots_parser.set_url(robots_url)
                self.robots_parser.feed(response.text.splitlines())
                
                # Check if we can fetch the main USA page
                if not self.robots_parser.can_fetch(config.USER_AGENT, config.USA_URL):
                    logger.warning("robots.txt disallows crawling USA pages")
                else:
                    logger.info("robots.txt allows crawling")
            else:
                logger.info("No robots.txt found, proceeding with crawl")
                
        except Exception as e:
            logger.warning(f"Could not check robots.txt: {e}")
        
        self._robots_checked = True
    
    def _can_fetch(self, url: str) -> bool:
        """Check if we're allowed to fetch a specific URL."""
        if not self.robots_parser:
            return True
        
        return self.robots_parser.can_fetch(config.USER_AGENT, url)
    
    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < config.RATE_LIMIT_DELAY:
            sleep_time = config.RATE_LIMIT_DELAY - time_since_last
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = asyncio.get_event_loop().time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    )
    async def get(self, url: str) -> Optional[str]:
        """
        Get a URL with rate limiting, retry logic, and robots.txt compliance.
        
        Args:
            url: The URL to fetch
            
        Returns:
            HTML content as string, or None if failed
        """
        if not self.session:
            raise RuntimeError("Client not started. Use async context manager or call start()")
        
        # Check robots.txt compliance
        if not self._can_fetch(url):
            logger.warning(f"Skipping {url} - disallowed by robots.txt")
            return None
        
        # Apply rate limiting
        await self._rate_limit()
        
        try:
            logger.debug(f"Fetching: {url}")
            response = await self.session.get(url)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get("content-type", "").lower()
            if "text/html" not in content_type:
                logger.warning(f"Unexpected content type for {url}: {content_type}")
                return None
            
            logger.debug(f"Successfully fetched {url} ({len(response.text)} chars)")
            return response.text
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"Page not found: {url}")
                return None
            elif e.response.status_code == 429:
                logger.warning(f"Rate limited by server: {url}")
                # Add extra delay for rate limiting
                await asyncio.sleep(5)
                raise  # Will trigger retry
            else:
                logger.error(f"HTTP error {e.response.status_code} for {url}")
                raise
                
        except httpx.RequestError as e:
            logger.error(f"Request error for {url}: {e}")
            raise
        
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None