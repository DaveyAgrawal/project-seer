#!/usr/bin/env python3
"""
Debug script to investigate Newark DE parsing.
"""

import asyncio
import sys
import os

# Add src to path (from debug folder)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.dcm.crawler import DataCenterMapCrawler
from src.dcm.parsers import DataCenterMapParser
from src.dcm.playwright_client import PlaywrightDataCenterMapClient


async def debug_newark():
    """Debug Newark DE parsing."""
    
    print("🔍 Starting Newark DE debug...")
    
    # Initialize components
    client = PlaywrightDataCenterMapClient()
    parser = DataCenterMapParser()
    
    newark_url = "https://www.datacentermap.com/usa/delaware/newark-de/"
    
    async with client:
        print(f"📡 Fetching Newark DE city page: {newark_url}")
        
        # Get Newark city page
        html = await client.get(newark_url)
        if not html:
            print("❌ Failed to fetch Newark DE city page")
            return
        
        print(f"✅ Fetched HTML content ({len(html)} characters)")
        
        # Save HTML to file for inspection (in debug folder)
        debug_file = os.path.join(os.path.dirname(__file__), "newark_de_city.html")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(html)
        print("💾 Saved HTML to debug/newark_de_city.html")
        
        # Parse facilities
        print(f"\n🏢 Parsing facilities in Newark DE...")
        facilities = parser.parse_city_facilities(html, "Newark DE", "Delaware")
        print(f"Found {len(facilities)} facilities:")
        for facility in facilities:
            print(f"  - {facility['name']}: {facility['url']}")
        
        if not facilities:
            print("❌ No facilities found! Let's inspect the Newark DE HTML structure...")
            
            from parsel import Selector
            city_selector = Selector(text=html)
            
            # Check for different types of links that might be facilities
            all_links = city_selector.css('a::attr(href)').getall()
            all_texts = city_selector.css('a::text').getall()
            
            print(f"\n🔗 All links on Newark DE page ({len(all_links)}):")
            for link, text in zip(all_links[:20], all_texts[:20]):
                if link and text and 'newark' in link.lower():
                    print(f"  - {text.strip()}: {link}")


if __name__ == "__main__":
    asyncio.run(debug_newark())