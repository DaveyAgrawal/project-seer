"""
Main crawler orchestrator for datacentermap.com scraping.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set

from rich.console import Console
from rich.progress import Progress, TaskID, track

from src.core.config import config
from src.core.database import db_manager
from src.core.logging import logger
from src.core.repository import FacilityRepository
from src.dcm.playwright_client import PlaywrightDataCenterMapClient
from src.dcm.parsers import DataCenterMapParser


class DataCenterMapCrawler:
    """Main crawler that orchestrates the scraping process."""
    
    def __init__(self):
        self.client = PlaywrightDataCenterMapClient()
        self.parser = DataCenterMapParser()
        self.console = Console()
        
        # Statistics tracking
        self.stats = {
            "states_processed": 0,
            "cities_processed": 0,
            "facilities_found": 0,
            "facilities_processed": 0,
            "facilities_with_coords": 0,
            "errors": 0,
            "start_time": None,
        }
        
        # Track processed URLs to avoid duplicates
        self.processed_urls: Set[str] = set()
    
    async def crawl_all_states(self, save_to_db: bool = True) -> List[Dict]:
        """Crawl all U.S. states for data center facilities."""
        logger.info("🚀 Starting full USA crawl")
        self.stats["start_time"] = datetime.utcnow()
        
        all_facilities = []
        
        async with self.client:
            # Step 1: Get all states
            logger.info("📍 Fetching USA states...")
            states = await self._get_states()
            
            if not states:
                logger.error("❌ No states found - aborting crawl")
                return []
            
            logger.info(f"🗺️  Found {len(states)} states to process")
            
            # Step 2: Process each state
            for state in track(states, description="Processing states..."):
                try:
                    state_facilities = await self._crawl_state(state, save_to_db=save_to_db)
                    all_facilities.extend(state_facilities)
                    self.stats["states_processed"] += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error processing state {state['name']}: {e}")
                    self.stats["errors"] += 1
                    continue
        
        self._log_final_stats()
        return all_facilities
    
    async def crawl_state(self, state_name: str, save_to_db: bool = True) -> List[Dict]:
        """Crawl a specific state for data center facilities."""
        logger.info(f"🚀 Starting crawl for state: {state_name}")
        self.stats["start_time"] = datetime.utcnow()
        
        facilities = []
        
        async with self.client:
            # Find the state info
            states = await self._get_states()
            target_state = None
            
            for state in states:
                if state["name"].lower() == state_name.lower() or state["slug"].lower() == state_name.lower():
                    target_state = state
                    break
            
            if not target_state:
                logger.error(f"❌ State '{state_name}' not found")
                return []
            
            # Crawl the specific state
            facilities = await self._crawl_state(target_state, save_to_db=save_to_db)
            self.stats["states_processed"] = 1
        
        self._log_final_stats()
        return facilities
    
    async def _get_states(self) -> List[Dict[str, str]]:
        """Fetch and parse all U.S. states with robust error handling."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Step 1: Visit homepage first to establish session (simulate "Explore Map" click)
                logger.info(f"🏠 Visiting homepage to establish session... (attempt {attempt + 1}/{max_retries})")
                
                try:
                    homepage_html = await self.client.get(config.BASE_URL)
                    if homepage_html and len(homepage_html) > 1000:
                        logger.info("✅ Homepage loaded successfully")
                    else:
                        logger.warning("⚠️  Homepage content seems incomplete, but proceeding...")
                except Exception as e:
                    logger.warning(f"⚠️  Could not load homepage (attempt {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        logger.warning("Proceeding to states page despite homepage issues...")
                    else:
                        await asyncio.sleep(5 * (attempt + 1))  # Progressive delay
                        continue
                
                # Step 2: Now fetch USA states page
                logger.info(f"🗺️  Fetching USA states page... (attempt {attempt + 1}/{max_retries})")
                html = await self.client.get(config.USA_URL)
                
                if not html:
                    raise Exception("Empty response from USA states page")
                
                if len(html) < 1000:
                    raise Exception(f"USA states page content too short ({len(html)} chars)")
                
                states = self.parser.parse_usa_states(html)
                
                if not states:
                    raise Exception("No states found in parsed content")
                
                if len(states) < 10:  # Expect at least 10 states
                    logger.warning(f"⚠️  Only found {len(states)} states, expected more")
                
                logger.info(f"✅ Found {len(states)} states")
                return states
                
            except Exception as e:
                error_msg = f"Failed to fetch states (attempt {attempt + 1}/{max_retries}): {e}"
                
                if attempt == max_retries - 1:
                    logger.error(f"❌ {error_msg} - giving up after {max_retries} attempts")
                    self.stats["errors"] += 1
                    return []
                else:
                    logger.warning(f"⚠️  {error_msg} - retrying...")
                    await asyncio.sleep(10 * (attempt + 1))  # Progressive delay
                    continue
        
        return []
    
    async def _crawl_state(self, state: Dict[str, str], save_to_db: bool = True) -> List[Dict]:
        """Crawl all cities in a specific state."""
        state_name = state["name"]
        state_url = state["url"]
        
        logger.info(f"🏛️  Processing state: {state_name}")
        
        # Get state page
        html = await self.client.get(state_url)
        if not html:
            logger.warning(f"⚠️  Could not fetch state page: {state_url}")
            return []
        
        # Parse cities
        cities = self.parser.parse_state_cities(html, state["slug"])
        if not cities:
            logger.warning(f"⚠️  No cities found in {state_name}")
            return []
        
        logger.info(f"🏙️  Found {len(cities)} cities in {state_name}")
        
        # Process each city
        state_facilities = []
        
        with Progress() as progress:
            task = progress.add_task(f"Cities in {state_name}", total=len(cities))
            
            for city in cities:
                try:
                    city_facilities = await self._crawl_city(city, save_to_db=save_to_db)
                    state_facilities.extend(city_facilities)
                    self.stats["cities_processed"] += 1
                    
                    progress.update(task, advance=1)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing city {city['name']} in {state_name}: {e}")
                    self.stats["errors"] += 1
                    progress.update(task, advance=1)
                    # Continue with next city - don't let one failure stop the whole state
                    continue
        
        # Save state facilities to database if requested
        if save_to_db and state_facilities:
            await self._save_facilities_to_db(state_facilities)
        
        logger.info(f"✅ Completed {state_name}: {len(state_facilities)} facilities")
        return state_facilities
    
    async def _crawl_city(self, city: Dict[str, str], save_to_db: bool = True) -> List[Dict]:
        """Crawl all facilities in a specific city."""
        city_name = city["name"]
        city_url = city["url"]
        state_name = city["state"]
        
        logger.debug(f"🏙️  Processing city: {city_name}, {state_name}")
        
        # Get city page
        html = await self.client.get(city_url)
        if not html:
            logger.debug(f"⚠️  Could not fetch city page: {city_url}")
            return []
        
        # Parse facilities
        facilities = self.parser.parse_city_facilities(html, city_name, state_name)
        if not facilities:
            logger.debug(f"ℹ️  No facilities found in {city_name}")
            return []
        
        self.stats["facilities_found"] += len(facilities)
        logger.info(f"🏢 Found {len(facilities)} facilities in {city_name}, {state_name}")
        
        # Process each facility
        detailed_facilities = []
        
        for facility in facilities:
            try:
                # Skip if already processed
                if facility["url"] in self.processed_urls:
                    logger.debug(f"⏭️  Skipping already processed: {facility['url']}")
                    continue
                
                # Skip sponsor links
                if self.parser.is_sponsor_link(facility["url"]):
                    logger.debug(f"⏭️  Skipping sponsor link: {facility['url']}")
                    continue
                
                detailed_facility = await self._crawl_facility(facility)
                if detailed_facility:
                    detailed_facilities.append(detailed_facility)
                    self.processed_urls.add(facility["url"])
                
                self.stats["facilities_processed"] += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing facility {facility['name']}: {e}")
                self.stats["errors"] += 1
                continue
        
        return detailed_facilities
    
    async def _crawl_facility(self, facility: Dict[str, str]) -> Optional[Dict]:
        """Crawl detailed information for a specific facility."""
        facility_url = facility["url"]
        
        logger.debug(f"🏢 Processing facility: {facility['name']}")
        
        # Get facility page  
        html = await self.client.get(facility_url)
        if not html:
            logger.debug(f"⚠️  Could not fetch facility page: {facility_url}")
            return None
        
        # Parse facility details
        detailed_facility = self.parser.parse_facility_details(html, facility_url)
        
        # Merge with basic info from city page
        detailed_facility.update({
            "source": "datacentermap",
            "first_seen_at": self.stats["start_time"],
            "last_seen_at": self.stats["start_time"],
        })
        
        # Prefer city page info if detail page parsing failed
        if not detailed_facility.get("name"):
            detailed_facility["name"] = facility["name"]
        if not detailed_facility.get("city"):
            detailed_facility["city"] = facility["city"]
        if not detailed_facility.get("state"):
            detailed_facility["state"] = facility["state"]
        
        # Track coordinate success
        if detailed_facility.get("latitude") and detailed_facility.get("longitude"):
            self.stats["facilities_with_coords"] += 1
        
        logger.debug(f"✅ Processed facility: {detailed_facility['name']}")
        return detailed_facility
    
    async def _save_facilities_to_db(self, facilities: List[Dict]) -> None:
        """Save facilities to database using upsert logic."""
        if not facilities:
            return
        
        try:
            async with db_manager.session() as session:
                repository = FacilityRepository(session)
                await repository.upsert_facilities_batch(facilities)
                logger.info(f"💾 Saved {len(facilities)} facilities to database")
                
        except Exception as e:
            logger.error(f"❌ Failed to save facilities to database: {e}")
            # Don't raise - we still want to return the data even if DB save fails
    
    def _log_final_stats(self):
        """Log final crawl statistics."""
        if not self.stats["start_time"]:
            return
        
        duration = (datetime.utcnow() - self.stats["start_time"]).total_seconds()
        
        self.console.print("\n" + "="*50)
        self.console.print("📊 CRAWL STATISTICS", style="bold green")
        self.console.print("="*50)
        self.console.print(f"⏱️  Duration: {duration:.1f} seconds")
        self.console.print(f"🏛️  States processed: {self.stats['states_processed']}")
        self.console.print(f"🏙️  Cities processed: {self.stats['cities_processed']}")
        self.console.print(f"🏢 Facilities found: {self.stats['facilities_found']}")
        self.console.print(f"✅ Facilities processed: {self.stats['facilities_processed']}")
        self.console.print(f"📍 Facilities with coordinates: {self.stats['facilities_with_coords']}")
        self.console.print(f"❌ Errors encountered: {self.stats['errors']}")
        
        if self.stats['facilities_processed'] > 0:
            coord_rate = (self.stats['facilities_with_coords'] / self.stats['facilities_processed']) * 100
            self.console.print(f"📊 Coordinate success rate: {coord_rate:.1f}%")
        
        self.console.print("="*50 + "\n")
        
        logger.info(f"🎉 Crawl completed: {self.stats['facilities_processed']} facilities processed")