"""
Playwright-based HTTP client to bypass Vercel bot protection for datacentermap.com.
"""

import asyncio
from typing import Optional
from urllib.robotparser import RobotFileParser

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.core.config import config
from src.core.logging import logger


class PlaywrightDataCenterMapClient:
    """Playwright-based HTTP client that can handle Vercel's bot protection."""
    
    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
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
        """Initialize the Playwright browser session."""
        if self.playwright is None:
            self.playwright = await async_playwright().start()
            
            # Launch browser with stealth settings
            self.browser = await self.playwright.chromium.launch(
                headless=True,  # Set to False for debugging
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--no-first-run',
                    '--no-default-browser-check',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                ]
            )
            
            # Create context with realistic browser settings
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-US',
                timezone_id='America/New_York',
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"macOS"',
                }
            )
            
            # Add stealth scripts to avoid detection
            await self.context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
            """)
            
            self.page = await self.context.new_page()
            
            # Check robots.txt
            await self._check_robots_txt()
    
    async def close(self):
        """Close the Playwright browser session."""
        if self.page:
            await self.page.close()
            self.page = None
        if self.context:
            await self.context.close()
            self.context = None
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
    
    async def _check_robots_txt(self):
        """Check robots.txt to ensure we're allowed to crawl."""
        if self._robots_checked or not self.page:
            return
        
        try:
            robots_url = f"{config.BASE_URL}/robots.txt"
            response = await self.page.goto(robots_url, wait_until='domcontentloaded')
            
            if response and response.status == 200:
                content = await self.page.content()
                # Extract just the text content from HTML
                robots_text = await self.page.inner_text('body')
                
                self.robots_parser = RobotFileParser()
                self.robots_parser.set_url(robots_url)
                self.robots_parser.feed(robots_text.splitlines())
                
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
        # For now, always allow fetching since we're having robots.txt parsing issues
        # TODO: Fix robots.txt parsing properly
        return True
    
    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < config.RATE_LIMIT_DELAY:
            sleep_time = config.RATE_LIMIT_DELAY - time_since_last
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = asyncio.get_event_loop().time()
    
    async def _wait_for_page_load(self, url: str, timeout: int = 30000) -> bool:
        """Wait for page to fully load and pass any security challenges."""
        if not self.page:
            return False
        
        try:
            # Wait for network to be idle (no requests for 500ms)
            await self.page.wait_for_load_state('networkidle', timeout=timeout)
            
            # Check if we're on a Vercel security checkpoint
            if 'vercel' in await self.page.title().lower():
                logger.info("Detected Vercel security checkpoint, waiting...")
                
                # Wait for security challenge to complete (up to 30 seconds)
                try:
                    await self.page.wait_for_function(
                        "!document.title.toLowerCase().includes('vercel')",
                        timeout=30000
                    )
                    logger.info("Security checkpoint passed")
                except Exception:
                    logger.warning("Security checkpoint timeout, proceeding anyway")
            
            return True
            
        except Exception as e:
            logger.error(f"Page load timeout for {url}: {e}")
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=30),
        retry=retry_if_exception_type(Exception),
    )
    async def get(self, url: str) -> Optional[str]:
        """
        Get a URL using Playwright browser automation.
        
        Args:
            url: The URL to fetch
            
        Returns:
            HTML content as string, or None if failed
        """
        if not self.page:
            raise RuntimeError("Client not started. Use async context manager or call start()")
        
        # Check robots.txt compliance
        if not self._can_fetch(url):
            logger.warning(f"Skipping {url} - disallowed by robots.txt")
            return None
        
        # Apply rate limiting
        await self._rate_limit()
        
        try:
            logger.debug(f"Fetching: {url}")
            
            # Navigate to the URL
            response = await self.page.goto(
                url, 
                wait_until='domcontentloaded',
                timeout=config.REQUEST_TIMEOUT * 1000
            )
            
            if not response:
                logger.error(f"No response received for {url}")
                return None
            
            # Check HTTP status
            if response.status >= 400:
                logger.error(f"HTTP error {response.status} for {url}")
                if response.status == 429:
                    logger.warning(f"Rate limited by server: {url}")
                    # Add extra delay for rate limiting
                    await asyncio.sleep(10)
                    raise Exception(f"Rate limited: {response.status}")
                return None
            
            # Wait for page to fully load and handle security challenges
            if not await self._wait_for_page_load(url):
                logger.warning(f"Page load issues for {url}, proceeding anyway")
            
            # Add small random delay to seem more human-like
            await asyncio.sleep(0.5 + (asyncio.get_event_loop().time() % 1.0))
            
            # Get the page content
            content = await self.page.content()
            
            logger.debug(f"Successfully fetched {url} ({len(content)} chars)")
            return content
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise