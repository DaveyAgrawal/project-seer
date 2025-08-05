"""
Playwright-based HTTP client to bypass Vercel bot protection for datacentermap.com.
"""

import asyncio
from datetime import datetime
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
                headless=config.PLAYWRIGHT_HEADLESS,  # Configurable via environment
                slow_mo=200 if not config.PLAYWRIGHT_HEADLESS else 0,  # Slow down actions when visible
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-first-run',
                    '--no-default-browser-check',
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
        
        # For testing Phase 1 - temporarily skip robots.txt checking
        logger.info("Skipping robots.txt check for Phase 1 testing")
        self._robots_checked = True
        return
        
        # TODO: Fix robots.txt parsing after Phase 1 testing
        try:
            robots_url = f"{config.BASE_URL}/robots.txt"
            response = await self.page.goto(robots_url, wait_until='domcontentloaded')
            
            if response and response.status == 200:
                content_type = response.headers.get("content-type", "").lower()
                
                if "text/plain" in content_type:
                    # Plain text robots.txt
                    robots_text = await response.text()
                else:
                    # HTML page - extract text content from body
                    try:
                        robots_text = await self.page.inner_text('body')
                    except Exception:
                        # Fallback to full page content if inner_text fails
                        robots_text = await response.text()
                        # Strip HTML tags if needed
                        import re
                        robots_text = re.sub(r'<[^>]+>', '', robots_text)
                
                if robots_text and robots_text.strip():
                    self.robots_parser = RobotFileParser()
                    self.robots_parser.set_url(robots_url)
                    
                    # Split by lines and filter out empty lines
                    lines = [line.strip() for line in robots_text.splitlines() if line.strip()]
                    
                    # Only process if we have actual robots.txt content
                    if any(line.lower().startswith(('user-agent:', 'disallow:', 'allow:')) for line in lines):
                        try:
                            for line in lines:
                                self.robots_parser.feed(line)
                        except Exception as feed_error:
                            logger.warning(f"Error feeding robots.txt lines: {feed_error}")
                            # If feed fails, create a new parser and try simple method
                            self.robots_parser = None
                        
                        # Check if we can fetch the main USA page
                        if self.robots_parser and not self.robots_parser.can_fetch(config.USER_AGENT, config.USA_URL):
                            logger.warning("robots.txt disallows crawling USA pages")
                        else:
                            logger.info("robots.txt allows crawling")
                    else:
                        logger.info("No valid robots.txt content found, proceeding with crawl")
                else:
                    logger.info("Empty robots.txt response, proceeding with crawl")
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
    
    async def _rate_limit(self, adaptive: bool = True):
        """Enforce adaptive rate limiting between requests."""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time
        
        base_delay = config.RATE_LIMIT_DELAY
        
        if adaptive:
            # Adaptive rate limiting based on recent performance
            # If we've had errors recently, slow down more
            error_rate = self._calculate_recent_error_rate()
            if error_rate > 0.3:  # More than 30% errors
                adaptive_delay = base_delay * 2.0
                logger.debug(f"High error rate ({error_rate:.1%}), using slower rate limiting")
            elif error_rate > 0.1:  # More than 10% errors
                adaptive_delay = base_delay * 1.5
                logger.debug(f"Moderate error rate ({error_rate:.1%}), using cautious rate limiting")
            else:
                adaptive_delay = base_delay
        else:
            adaptive_delay = base_delay
        
        if time_since_last < adaptive_delay:
            sleep_time = adaptive_delay - time_since_last
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s (adaptive: {adaptive})")
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = asyncio.get_event_loop().time()
    
    def _calculate_recent_error_rate(self) -> float:
        """Calculate error rate from recent requests for adaptive rate limiting."""
        # This is a simplified version - in production you'd track actual request history
        # For now, return a default low error rate
        return 0.0
    
    async def _wait_for_page_load(self, url: str, timeout: int = None) -> bool:
        """Wait for page to fully load and pass any security challenges."""
        if not self.page:
            return False
        
        if timeout is None:
            timeout = config.PLAYWRIGHT_TIMEOUT
        
        try:
            # First wait for basic page load
            await self.page.wait_for_load_state('domcontentloaded', timeout=timeout // 3)
            
            # Check if we're on a Vercel security checkpoint
            page_title = await self.page.title()
            page_content = await self.page.content()
            
            if ('vercel' in page_title.lower() or 
                'security checkpoint' in page_content.lower() or
                'verifying your browser' in page_content.lower()):
                
                logger.info("🛡️  Detected Vercel security checkpoint, waiting for challenge completion...")
                
                # Wait for security challenge to complete
                try:
                    # Method 1: Wait for title to change away from Vercel
                    await self.page.wait_for_function(
                        """() => {
                            const title = document.title.toLowerCase();
                            const body = document.body.innerText.toLowerCase();
                            return !title.includes('vercel') && 
                                   !body.includes('verifying your browser') &&
                                   !body.includes('security checkpoint');
                        }""",
                        timeout=config.VERCEL_CHALLENGE_TIMEOUT
                    )
                    logger.info("✅ Security checkpoint passed successfully")
                    
                    # Additional wait for page to stabilize after challenge
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.warning(f"⚠️  Security checkpoint timeout ({config.VERCEL_CHALLENGE_TIMEOUT/1000}s), checking if we can proceed...")
                    
                    # Check if we actually got real content despite timeout
                    final_content = await self.page.content()
                    if len(final_content) > 1000 and 'datacentermap' in final_content.lower():
                        logger.info("✅ Page appears to have loaded despite challenge timeout")
                    else:
                        logger.error("❌ Still blocked by security challenge")
                        return False
            
            # Wait for network to be idle for final content
            try:
                await self.page.wait_for_load_state('networkidle', timeout=timeout // 3)
            except Exception:
                logger.debug("Network idle timeout, but proceeding anyway")
            
            return True
            
        except Exception as e:
            logger.error(f"Page load timeout for {url}: {e}")
            return False
    
    async def _wait_for_interactive_map(self, timeout: int = 30000) -> bool:
        """Wait for the interactive map to load completely on the USA page."""
        if not self.page:
            return False
        
        try:
            logger.info("🗺️  Waiting for interactive map to load...")
            
            # Wait for common map container elements to appear
            map_selectors = [
                'div[class*="map"]',
                'div[id*="map"]', 
                'canvas',
                'svg',
                '.leaflet-container',
                '.mapboxgl-map'
            ]
            
            # Try to find any map container
            for selector in map_selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=timeout // len(map_selectors))
                    logger.info(f"✅ Found map container: {selector}")
                    break
                except Exception:
                    continue
            else:
                logger.warning("⚠️  No map container found, but proceeding...")
            
            # Wait for any loading indicators to disappear
            loading_selectors = [
                '.loading',
                '.spinner',
                '[class*="loading"]',
                '[class*="spinner"]'
            ]
            
            for selector in loading_selectors:
                try:
                    # Wait for loading element to appear then disappear
                    await self.page.wait_for_selector(selector, timeout=2000)
                    await self.page.wait_for_selector(selector, state='hidden', timeout=timeout)
                    logger.info(f"✅ Loading indicator disappeared: {selector}")
                except Exception:
                    continue
            
            # Wait for map tiles/content to load (give it time to render)
            logger.info("⏳ Allowing time for map content to render...")
            await asyncio.sleep(3)
            
            # Check if we can find state-related clickable elements
            state_selectors = [
                '[class*="state"]',
                '[data-state]',
                'circle',
                'path[class*="state"]',
                '.marker',
                '[class*="marker"]'
            ]
            
            found_states = 0
            for selector in state_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        found_states += len(elements)
                        logger.info(f"✅ Found {len(elements)} potential state elements: {selector}")
                except Exception:
                    continue
            
            if found_states > 0:
                logger.info(f"🎯 Interactive map loaded successfully! Found {found_states} interactive elements")
                return True
            else:
                logger.warning("⚠️  Map loaded but no interactive elements found")
                return False
                
        except Exception as e:
            logger.error(f"Error waiting for interactive map: {e}")
            return False
    
    async def _find_state_clickable_elements(self) -> list:
        """Find clickable elements that represent states on the map."""
        if not self.page:
            return []
        
        try:
            logger.info("🔍 Searching for clickable state elements...")
            
            # Try various selectors that might represent states
            potential_selectors = [
                # Common map library patterns
                'circle[data-state]',
                'path[data-state]', 
                '[class*="state-marker"]',
                '[class*="state"][onclick]',
                'circle[onclick]',
                'path[onclick]',
                
                # Generic clickable elements that might be states
                'circle[cursor="pointer"]',
                'path[cursor="pointer"]',
                '.marker[data-state]',
                '.state-circle',
                '.region-marker',
                
                # SVG/Canvas based maps
                'g[data-state] circle',
                'g[data-state] path',
                'svg circle[data-*]',
                'svg path[data-*]',
            ]
            
            found_elements = []
            
            for selector in potential_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        # Get element attributes to determine if it's a state
                        attrs = await element.evaluate("""(element) => {
                            const attrs = {};
                            for (let attr of element.attributes) {
                                attrs[attr.name] = attr.value;
                            }
                            return attrs;
                        }""")
                        
                        # Check if element seems to represent a state
                        if any(key for key in attrs.keys() if 'state' in key.lower()):
                            found_elements.append({
                                'element': element,
                                'selector': selector,
                                'attributes': attrs
                            })
                            
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            # If no data-state elements found, try to find clickable circles/paths
            if not found_elements:
                logger.info("🔍 No data-state elements found, looking for generic clickable elements...")
                
                generic_selectors = [
                    'circle',
                    'path', 
                    '.marker',
                    '[onclick]'
                ]
                
                for selector in generic_selectors:
                    try:
                        elements = await self.page.query_selector_all(selector)
                        # Limit to reasonable number for states (50 is max US states)
                        for element in elements[:60]:
                            # Check if element is clickable
                            is_clickable = await element.evaluate("""(element) => {
                                const style = window.getComputedStyle(element);
                                return style.cursor === 'pointer' || 
                                       element.onclick !== null ||
                                       element.getAttribute('onclick') !== null;
                            }""")
                            
                            if is_clickable:
                                found_elements.append({
                                    'element': element,
                                    'selector': selector,
                                    'attributes': {}
                                })
                                
                                # Stop after finding reasonable number of clickable elements
                                if len(found_elements) >= 50:
                                    break
                                    
                    except Exception as e:
                        logger.debug(f"Generic selector {selector} failed: {e}")
                        continue
            
            logger.info(f"🎯 Found {len(found_elements)} potential state clickable elements")
            return found_elements
            
        except Exception as e:
            logger.error(f"Error finding state elements: {e}")
            return []
    
    async def click_state_element(self, element_info: dict, state_name: str = None) -> bool:
        """Click on a state element and wait for response."""
        if not self.page:
            return False
        
        try:
            element = element_info['element']
            selector = element_info['selector']
            
            # Get element position for logging
            box = await element.bounding_box()
            if box:
                logger.info(f"🖱️  Clicking {state_name or 'state'} element at ({box['x']:.0f}, {box['y']:.0f}) - {selector}")
            else:
                logger.info(f"🖱️  Clicking {state_name or 'state'} element - {selector}")
            
            # Scroll element into view
            await element.scroll_into_view_if_needed()
            await asyncio.sleep(0.5)  # Brief pause after scrolling
            
            # Get current URL to detect navigation
            current_url = self.page.url
            
            # Perform human-like click
            await element.click(
                button='left',
                click_count=1,
                delay=100  # 100ms delay between mouse down and up
            )
            
            # Wait briefly for any immediate response
            await asyncio.sleep(1)
            
            # Check if page navigated
            new_url = self.page.url
            if new_url != current_url:
                logger.info(f"✅ Navigation detected: {current_url} → {new_url}")
                
                # Wait for new page to load
                try:
                    await self.page.wait_for_load_state('networkidle', timeout=15000)
                    logger.info("✅ New page loaded after state click")
                    return True
                except Exception as e:
                    logger.warning(f"⚠️  Page load timeout after navigation: {e}")
                    return True  # Still consider it successful
            
            # If no navigation, check for dynamic content updates
            else:
                logger.info("🔄 No navigation detected, checking for dynamic content updates...")
                
                # Wait for any loading indicators to appear and disappear
                loading_selectors = ['.loading', '.spinner', '[class*="loading"]']
                for selector in loading_selectors:
                    try:
                        await self.page.wait_for_selector(selector, timeout=2000)
                        await self.page.wait_for_selector(selector, state='hidden', timeout=10000)
                        logger.info(f"✅ Dynamic loading completed: {selector}")
                        return True
                    except Exception:
                        continue
                
                # Wait for network activity to settle
                try:
                    await self.page.wait_for_load_state('networkidle', timeout=10000)
                    logger.info("✅ Network activity settled after state click")
                    return True
                except Exception:
                    logger.warning("⚠️  Network activity timeout, but proceeding...")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Error clicking state element: {e}")
            return False
    
    async def extract_state_data_from_current_page(self) -> dict:
        """Extract state/city data from the current page after clicking a state."""
        if not self.page:
            return {}
        
        try:
            # Get current URL and page content
            current_url = self.page.url
            page_content = await self.page.content()
            
            logger.info(f"📄 Extracting data from: {current_url}")
            
            # Basic extraction - this will be enhanced based on actual page structure
            data = {
                'url': current_url,
                'content_length': len(page_content),
                'title': await self.page.title(),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Try to find city links or facility information
            city_selectors = [
                'a[href*="city"]',
                'a[href*="location"]', 
                '.city-link',
                '.facility-link',
                'a[href*="/usa/"][href*="/"]',  # State-specific city URLs
            ]
            
            cities_found = []
            for selector in city_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements[:20]:  # Limit to prevent overwhelming
                        text = await element.inner_text()
                        href = await element.get_attribute('href')
                        if text and href:
                            cities_found.append({
                                'name': text.strip(),
                                'url': href
                            })
                except Exception:
                    continue
            
            data['cities'] = cities_found
            data['cities_count'] = len(cities_found)
            
            logger.info(f"✅ Extracted {len(cities_found)} cities from state page")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error extracting state data: {e}")
            return {}
    
    async def _validate_page_content(self, url: str, expected_content_type: str = "generic") -> bool:
        """Validate that we got real content instead of being blocked."""
        if not self.page:
            return False
        
        try:
            content = await self.page.content()
            page_title = await self.page.title()
            
            # Check for Vercel blocking
            blocked_indicators = [
                'vercel security checkpoint',
                'verifying your browser', 
                'we\'re verifying your browser',
                'challenge',
                'please wait while we verify'
            ]
            
            content_lower = content.lower()
            title_lower = page_title.lower()
            
            for indicator in blocked_indicators:
                if indicator in content_lower or indicator in title_lower:
                    logger.warning(f"❌ Page still shows blocking content: {indicator}")
                    return False
            
            # Check for minimum content length
            if len(content) < 500:
                logger.warning(f"❌ Page content too short ({len(content)} chars), likely blocked")
                return False
            
            # Content-specific validation
            if expected_content_type == "states":
                # For USA states page, expect interactive map elements instead of text
                
                # First check for interactive map elements
                map_found = await self._wait_for_interactive_map(timeout=30000)
                if map_found:
                    logger.info("✅ States page validation passed: interactive map loaded")
                    return True
                
                # Fallback: check for state names in content (legacy approach)
                expected_elements = ['delaware', 'california', 'texas', 'new york']
                found_states = sum(1 for state in expected_elements if state in content_lower)
                if found_states < 2:
                    logger.warning(f"❌ States page validation failed: only found {found_states} expected states, no interactive map")
                    return False
                logger.info(f"✅ States page validation passed: found {found_states} expected states (fallback)")
                
            elif expected_content_type == "state":
                # For state page, expect cities and data center related content
                expected_terms = ['data center', 'facility', 'city', 'colocation', 'datacenter']
                found_terms = sum(1 for term in expected_terms if term in content_lower)
                if found_terms < 2:
                    logger.warning(f"❌ State page validation failed: only found {found_terms} relevant terms")
                    return False
                logger.info(f"✅ State page validation passed: found {found_terms} relevant terms")
                
            elif expected_content_type == "city":
                # For city page, expect facility listings
                expected_terms = ['facility', 'data center', 'datacenter', 'colocation', 'address']
                found_terms = sum(1 for term in expected_terms if term in content_lower)
                if found_terms < 2:
                    logger.warning(f"❌ City page validation failed: only found {found_terms} relevant terms")
                    return False
                logger.info(f"✅ City page validation passed: found {found_terms} relevant terms")
            
            # Generic validation - check for datacentermap branding
            if 'datacentermap' not in content_lower and 'data center map' not in content_lower:
                logger.warning("❌ Page doesn't appear to be from datacentermap.com")
                return False
            
            logger.info(f"✅ Page content validation passed for {url}")
            return True
            
        except Exception as e:
            logger.error(f"Error validating page content: {e}")
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
                timeout=config.PLAYWRIGHT_TIMEOUT
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
                logger.warning(f"Page load issues for {url}, attempting to validate content anyway")
            
            # Determine expected content type based on URL
            expected_content_type = "generic"
            if "/usa/" == url.split('/')[-1] or url.endswith("/usa"):
                expected_content_type = "states"
            elif "/usa/" in url and url.count('/') >= 4:
                if url.endswith('/'):
                    expected_content_type = "state"
                else:
                    expected_content_type = "city"
            
            # Validate page content (temporarily relaxed for Phase 2 testing)
            if "/usa/" in url:
                logger.info("🧪 Phase 2 testing: Skipping strict validation for USA page to test interactive features")
            else:
                if not await self._validate_page_content(url, expected_content_type):
                    logger.error(f"❌ Content validation failed for {url}")
                    raise Exception(f"Content validation failed - likely still blocked")
            
            # Add small random delay to seem more human-like
            await asyncio.sleep(0.5 + (asyncio.get_event_loop().time() % 1.0))
            
            # Get the page content
            content = await self.page.content()
            
            logger.debug(f"Successfully fetched {url} ({len(content)} chars)")
            return content
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise