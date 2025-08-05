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
            # Step 1: Get all states from hardcoded list
            logger.info("📍 Getting USA states...")
            states = self._get_usa_states()
            
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
            # Find the state info from hardcoded list
            states = self._get_usa_states()
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
    
    def _get_usa_states(self) -> List[Dict[str, str]]:
        """Return hardcoded list of all US states and DC for reliable crawling."""
        states = [
            {"name": "Alabama", "slug": "alabama", "url": f"{config.BASE_URL}/usa/alabama/"},
            {"name": "Alaska", "slug": "alaska", "url": f"{config.BASE_URL}/usa/alaska/"},
            {"name": "Arizona", "slug": "arizona", "url": f"{config.BASE_URL}/usa/arizona/"},
            {"name": "Arkansas", "slug": "arkansas", "url": f"{config.BASE_URL}/usa/arkansas/"},
            {"name": "California", "slug": "california", "url": f"{config.BASE_URL}/usa/california/"},
            {"name": "Colorado", "slug": "colorado", "url": f"{config.BASE_URL}/usa/colorado/"},
            {"name": "Connecticut", "slug": "connecticut", "url": f"{config.BASE_URL}/usa/connecticut/"},
            {"name": "Delaware", "slug": "delaware", "url": f"{config.BASE_URL}/usa/delaware/"},
            {"name": "Florida", "slug": "florida", "url": f"{config.BASE_URL}/usa/florida/"},
            {"name": "Georgia", "slug": "georgia", "url": f"{config.BASE_URL}/usa/georgia/"},
            {"name": "Hawaii", "slug": "hawaii", "url": f"{config.BASE_URL}/usa/hawaii/"},
            {"name": "Idaho", "slug": "idaho", "url": f"{config.BASE_URL}/usa/idaho/"},
            {"name": "Illinois", "slug": "illinois", "url": f"{config.BASE_URL}/usa/illinois/"},
            {"name": "Indiana", "slug": "indiana", "url": f"{config.BASE_URL}/usa/indiana/"},
            {"name": "Iowa", "slug": "iowa", "url": f"{config.BASE_URL}/usa/iowa/"},
            {"name": "Kansas", "slug": "kansas", "url": f"{config.BASE_URL}/usa/kansas/"},
            {"name": "Kentucky", "slug": "kentucky", "url": f"{config.BASE_URL}/usa/kentucky/"},
            {"name": "Louisiana", "slug": "louisiana", "url": f"{config.BASE_URL}/usa/louisiana/"},
            {"name": "Maine", "slug": "maine", "url": f"{config.BASE_URL}/usa/maine/"},
            {"name": "Maryland", "slug": "maryland", "url": f"{config.BASE_URL}/usa/maryland/"},
            {"name": "Massachusetts", "slug": "massachusetts", "url": f"{config.BASE_URL}/usa/massachusetts/"},
            {"name": "Michigan", "slug": "michigan", "url": f"{config.BASE_URL}/usa/michigan/"},
            {"name": "Minnesota", "slug": "minnesota", "url": f"{config.BASE_URL}/usa/minnesota/"},
            {"name": "Mississippi", "slug": "mississippi", "url": f"{config.BASE_URL}/usa/mississippi/"},
            {"name": "Missouri", "slug": "missouri", "url": f"{config.BASE_URL}/usa/missouri/"},
            {"name": "Montana", "slug": "montana", "url": f"{config.BASE_URL}/usa/montana/"},
            {"name": "Nebraska", "slug": "nebraska", "url": f"{config.BASE_URL}/usa/nebraska/"},
            {"name": "Nevada", "slug": "nevada", "url": f"{config.BASE_URL}/usa/nevada/"},
            {"name": "New Hampshire", "slug": "new-hampshire", "url": f"{config.BASE_URL}/usa/new-hampshire/"},
            {"name": "New Jersey", "slug": "new-jersey", "url": f"{config.BASE_URL}/usa/new-jersey/"},
            {"name": "New Mexico", "slug": "new-mexico", "url": f"{config.BASE_URL}/usa/new-mexico/"},
            {"name": "New York", "slug": "new-york", "url": f"{config.BASE_URL}/usa/new-york/"},
            {"name": "North Carolina", "slug": "north-carolina", "url": f"{config.BASE_URL}/usa/north-carolina/"},
            {"name": "North Dakota", "slug": "north-dakota", "url": f"{config.BASE_URL}/usa/north-dakota/"},
            {"name": "Ohio", "slug": "ohio", "url": f"{config.BASE_URL}/usa/ohio/"},
            {"name": "Oklahoma", "slug": "oklahoma", "url": f"{config.BASE_URL}/usa/oklahoma/"},
            {"name": "Oregon", "slug": "oregon", "url": f"{config.BASE_URL}/usa/oregon/"},
            {"name": "Pennsylvania", "slug": "pennsylvania", "url": f"{config.BASE_URL}/usa/pennsylvania/"},
            {"name": "Rhode Island", "slug": "rhode-island", "url": f"{config.BASE_URL}/usa/rhode-island/"},
            {"name": "South Carolina", "slug": "south-carolina", "url": f"{config.BASE_URL}/usa/south-carolina/"},
            {"name": "South Dakota", "slug": "south-dakota", "url": f"{config.BASE_URL}/usa/south-dakota/"},
            {"name": "Tennessee", "slug": "tennessee", "url": f"{config.BASE_URL}/usa/tennessee/"},
            {"name": "Texas", "slug": "texas", "url": f"{config.BASE_URL}/usa/texas/"},
            {"name": "Utah", "slug": "utah", "url": f"{config.BASE_URL}/usa/utah/"},
            {"name": "Vermont", "slug": "vermont", "url": f"{config.BASE_URL}/usa/vermont/"},
            {"name": "Virginia", "slug": "virginia", "url": f"{config.BASE_URL}/usa/virginia/"},
            {"name": "Washington", "slug": "washington", "url": f"{config.BASE_URL}/usa/washington/"},
            {"name": "West Virginia", "slug": "west-virginia", "url": f"{config.BASE_URL}/usa/west-virginia/"},
            {"name": "Wisconsin", "slug": "wisconsin", "url": f"{config.BASE_URL}/usa/wisconsin/"},
            {"name": "Wyoming", "slug": "wyoming", "url": f"{config.BASE_URL}/usa/wyoming/"},
            {"name": "District of Columbia", "slug": "district-of-columbia", "url": f"{config.BASE_URL}/usa/district-of-columbia/"},
        ]
        
        logger.info(f"📍 Using hardcoded list of {len(states)} US states and DC")
        return states
    
    async def _crawl_state(self, state: Dict[str, str], save_to_db: bool = True) -> List[Dict]:
        """Crawl all cities in a specific state using traditional parsing."""
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
                    city_facilities = await self._crawl_city(city, save_to_db=False)
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