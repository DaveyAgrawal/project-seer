#!/usr/bin/env python3
"""
Debug script to investigate Delaware parsing issues.
"""

import asyncio
import sys
import os

# Add src to path (from debug folder)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.dcm.crawler import DataCenterMapCrawler
from src.dcm.parsers import DataCenterMapParser
from src.dcm.playwright_client import PlaywrightDataCenterMapClient


async def debug_delaware():
    """Debug Delaware parsing step by step."""
    
    print("🔍 Starting Delaware debug...")
    
    # Initialize components
    client = PlaywrightDataCenterMapClient()
    parser = DataCenterMapParser()
    
    delaware_url = "https://www.datacentermap.com/usa/delaware/"
    
    async with client:
        print(f"📡 Fetching Delaware state page: {delaware_url}")
        
        # Get Delaware state page
        html = await client.get(delaware_url)
        if not html:
            print("❌ Failed to fetch Delaware state page")
            return
        
        print(f"✅ Fetched HTML content ({len(html)} characters)")
        
        # Save HTML to file for inspection (in debug folder)
        debug_file = os.path.join(os.path.dirname(__file__), "delaware_state.html")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(html)
        print("💾 Saved HTML to debug/delaware_state.html")
        
        # Parse cities
        print("\n🏙️ Parsing cities...")
        cities = parser.parse_state_cities(html, "delaware")
        print(f"Found {len(cities)} cities:")
        for city in cities:
            print(f"  - {city['name']}: {city['url']}")
        
        if not cities:
            print("❌ No cities found! Let's inspect the HTML structure...")
            
            # Let's look for any links that might be cities
            from parsel import Selector
            selector = Selector(text=html)
            
            # Check all links
            all_links = selector.css('a::attr(href)').getall()
            all_texts = selector.css('a::text').getall()
            
            print(f"\n🔗 Found {len(all_links)} total links:")
            for link, text in zip(all_links[:20], all_texts[:20]):  # Show first 20
                if link and text:
                    print(f"  - {text.strip()}: {link}")
            
            # Look specifically for Delaware-related links
            delaware_links = [link for link in all_links if 'delaware' in link.lower()]
            print(f"\n🔍 Delaware-related links ({len(delaware_links)}):")
            for i, link in enumerate(delaware_links[:10]):
                corresponding_text = all_texts[all_links.index(link)] if link in all_links else "N/A"
                print(f"  {i+1}. {corresponding_text}: {link}")
        
        # Try to visit a city page if we found any
        if cities:
            city = cities[0]  # Take first city (likely Wilmington)
            print(f"\n🏙️ Fetching city page: {city['name']} ({city['url']})")
            
            city_html = await client.get(city['url'])
            if not city_html:
                print(f"❌ Failed to fetch city page: {city['url']}")
                return
            
            print(f"✅ Fetched city HTML content ({len(city_html)} characters)")
            
            # Save city HTML (in debug folder)
            city_filename = f"{city['name'].lower().replace(' ', '_')}_city.html"
            debug_city_file = os.path.join(os.path.dirname(__file__), city_filename)
            with open(debug_city_file, "w", encoding="utf-8") as f:
                f.write(city_html)
            print(f"💾 Saved city HTML to debug/{city_filename}")
            
            # Parse facilities
            print(f"\n🏢 Parsing facilities in {city['name']}...")
            facilities = parser.parse_city_facilities(city_html, city['name'], "Delaware")
            print(f"Found {len(facilities)} facilities:")
            for facility in facilities:
                print(f"  - {facility['name']}: {facility['url']}")
            
            if not facilities:
                print("❌ No facilities found! Let's inspect the city HTML structure...")
                
                from parsel import Selector
                city_selector = Selector(text=city_html)
                
                # Check for different types of links that might be facilities
                datacenter_links = city_selector.css('a[href*="datacenter"]').getall()
                facility_links = city_selector.css('a[href*="facility"]').getall()
                
                print(f"🔍 Datacenter links: {len(datacenter_links)}")
                print(f"🔍 Facility links: {len(facility_links)}")
                
                # Show all links from city page
                city_all_links = city_selector.css('a::attr(href)').getall()
                city_all_texts = city_selector.css('a::text').getall()
                
                print(f"\n🔗 All links on city page ({len(city_all_links)}):")
                for link, text in zip(city_all_links[:15], city_all_texts[:15]):
                    if link and text:
                        print(f"  - {text.strip()}: {link}")


if __name__ == "__main__":
    asyncio.run(debug_delaware())