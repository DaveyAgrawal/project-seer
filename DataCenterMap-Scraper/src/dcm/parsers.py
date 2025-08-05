"""
HTML parsers for extracting data from datacentermap.com pages.
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from parsel import Selector

from src.core.logging import logger


class DataCenterMapParser:
    """Parser for datacentermap.com HTML pages."""
    
    def __init__(self, base_url: str = "https://www.datacentermap.com"):
        self.base_url = base_url
    
    def parse_usa_states(self, html: str) -> List[Dict[str, str]]:
        """Parse USA page to extract state links and names."""
        selector = Selector(text=html)
        states = []
        
        # Look for state links in the main content
        state_links = selector.css('a[href*="/usa/"]::attr(href)').getall()
        state_names = selector.css('a[href*="/usa/"]::text').getall()
        
        for link, name in zip(state_links, state_names):
            if link and name and link.count('/') >= 2:  # Filter valid state URLs
                clean_name = name.strip()
                if clean_name and clean_name.lower() != "usa":
                    states.append({
                        "name": clean_name,
                        "url": urljoin(self.base_url, link),
                        "slug": link.split('/')[-2] if link.endswith('/') else link.split('/')[-1]
                    })
        
        logger.info(f"Found {len(states)} states")
        return states
    
    def parse_state_cities(self, html: str, state_name: str) -> List[Dict[str, str]]:
        """Parse state page to extract city links and names."""
        selector = Selector(text=html)
        cities = []
        
        # Look for city links within the state
        city_links = selector.css(f'a[href*="/usa/{state_name.lower()}/"]::attr(href)').getall()
        city_names = selector.css(f'a[href*="/usa/{state_name.lower()}/"]::text').getall()
        
        for link, name in zip(city_links, city_names):
            if link and name and link.count('/') >= 3:  # Filter valid city URLs
                clean_name = name.strip()
                if clean_name and clean_name.lower() != state_name.lower():
                    cities.append({
                        "name": clean_name,
                        "url": urljoin(self.base_url, link),
                        "state": state_name,
                        "slug": link.split('/')[-2] if link.endswith('/') else link.split('/')[-1]
                    })
        
        logger.info(f"Found {len(cities)} cities in {state_name}")
        return cities
    
    def parse_city_facilities_from_list(self, html: str, city_name: str, state_name: str) -> List[Dict[str, str]]:
        """Parse facility data directly from city page list view (preferred method)."""
        selector = Selector(text=html)
        facilities = []
        seen_facilities = set()  # Track unique facilities by name + address
        
        # Look for facility cards in the list view
        cards = selector.css('.ui.card')
        
        for card in cards:
            # Extract facility name from header
            name_elements = card.css('.header::text').getall()
            if not name_elements:
                continue
                
            facility_name = name_elements[0].strip()
            
            # Skip if this looks like an ad or non-facility card
            if not facility_name or 'advertisement' in facility_name.lower():
                continue
            
            # Extract all text content from the card
            text_content = [t.strip() for t in card.css('*::text').getall() if t.strip()]
            
            if len(text_content) < 3:  # Need at least name, company, address
                continue
            
            # Parse the structured data from the card
            # Typical format: [Name, Company, Street Address, Postal+City, ...]
            facility_data = {
                "name": facility_name,
                "city": city_name,
                "state": state_name,
                "country": "USA",
                "source": "datacentermap"
            }
            
            # Extract company/operator (usually second element)
            if len(text_content) >= 2:
                facility_data["operator"] = text_content[1]
            
            # Extract street address (usually third element)
            if len(text_content) >= 3:
                street_address = text_content[2]
                facility_data["address_street"] = street_address
            
            # Extract postal code and city (usually fourth element)
            if len(text_content) >= 4:
                postal_city = text_content[3]
                # Parse "19801 Wilmington" format
                parts = postal_city.split()
                if len(parts) >= 2:
                    facility_data["postal_code"] = parts[0]
                    facility_data["address_city"] = ' '.join(parts[1:])
            
            # Build full address
            address_parts = []
            if facility_data.get("address_street"):
                address_parts.append(facility_data["address_street"])
            if facility_data.get("address_city"):
                address_parts.append(facility_data["address_city"])
            if facility_data.get("state"):
                address_parts.append(facility_data["state"])
            if facility_data.get("postal_code"):
                address_parts.append(facility_data["postal_code"])
            
            facility_data["address_full"] = ", ".join(address_parts)
            
            # Try to extract facility URL if available
            facility_links = card.css('a[href*="/usa/"]::attr(href)').getall()
            if facility_links:
                # Filter out quote links
                facility_url = None
                for link in facility_links:
                    if '/quote/' not in link and '/request-quote' not in link:
                        facility_url = self.base_url + link if not link.startswith('http') else link
                        break
                
                if facility_url:
                    facility_data["source_url"] = facility_url
            
            # Create unique identifier for deduplication
            unique_key = f"{facility_name}|{facility_data.get('address_street', '')}"
            
            # Only add if we haven't seen this facility before
            if unique_key not in seen_facilities:
                seen_facilities.add(unique_key)
                facilities.append(facility_data)
        
        logger.info(f"Extracted {len(facilities)} facilities from list view in {city_name}, {state_name}")
        return facilities

    def parse_city_facilities(self, html: str, city_name: str, state_name: str) -> List[Dict[str, str]]:
        """Parse city page to extract facility links and names."""
        selector = Selector(text=html)
        facilities = []
        
        # DataCenterMap.com uses city-specific URLs for facilities
        # Pattern: /usa/{state}/{city}/{facility-slug}/
        # We need to look for these specific patterns based on the actual HTML structure
        
        # Method 1: Look for card links with facility-specific URLs (primary method)
        state_slug = state_name.lower().replace(' ', '-')
        # Handle special city name cases
        if city_name.lower() == "newark de":
            city_slug = "newark-de"
        else:
            city_slug = city_name.lower().replace(' ', '-')
        
        # Use a set to track URLs we've already found
        found_urls = set()
        
        # Look for facility links that follow the city pattern
        facility_selectors = [
            f'a[href*="/usa/{state_slug}/{city_slug}/"]',  # Primary pattern - should be sufficient for modern pages
        ]
        
        for css_selector in facility_selectors:
            links = selector.css(f'{css_selector}::attr(href)').getall()
            names = selector.css(f'{css_selector}::text').getall()
            
            for link, name in zip(links, names):
                if link and name:
                    clean_name = name.strip()
                    full_url = urljoin(self.base_url, link)
                    
                    # Skip generic links that don't point to specific facilities
                    if link.endswith('/datacenters/') or link.endswith('/facilities/') or link.endswith('/datacenter/') or link.endswith('/facility/'):
                        continue
                    
                    # Skip if it's just a breadcrumb or navigation link back to parent pages
                    if f'/usa/{state_slug}/' == link or f'/usa/{state_slug}/{city_slug}/' == link:
                        continue
                    
                    # Skip quote pages and other non-facility pages
                    if '/quote/' in link or '/request-quote' in link:
                        continue
                    
                    # Avoid duplicates by checking URL (primary key)
                    if full_url not in found_urls:
                        found_urls.add(full_url)
                        facilities.append({
                            "name": clean_name,
                            "url": full_url,
                            "city": city_name,
                            "state": state_name
                        })
        
        # If we still didn't find facilities with the primary method, try legacy patterns
        if not facilities:
            legacy_selectors = [
                'a[href*="/datacenters/"]',  # Legacy pattern
                'a[href*="/facilities/"]',   # Legacy pattern
                'a[href*="/datacenter/"]',   # Legacy pattern
                'a[href*="/facility/"]'      # Legacy pattern
            ]
            
            for css_selector in legacy_selectors:
                links = selector.css(f'{css_selector}::attr(href)').getall()
                names = selector.css(f'{css_selector}::text').getall()
                
                for link, name in zip(links, names):
                    if link and name:
                        clean_name = name.strip()
                        full_url = urljoin(self.base_url, link)
                        
                        # Skip generic links that don't point to specific facilities
                        if link.endswith('/datacenters/') or link.endswith('/facilities/') or link.endswith('/datacenter/') or link.endswith('/facility/'):
                            continue
                        
                        # Avoid duplicates by checking URL (primary key)
                        if full_url not in found_urls:
                            found_urls.add(full_url)
                            facilities.append({
                                "name": clean_name,
                                "url": full_url,
                                "city": city_name,
                                "state": state_name
                            })
        
        # Method 2: If we didn't find facilities with the primary method, look for card elements with headers
        if not facilities:
            # Look for UI card elements which contain facility information
            card_links = selector.css('a.ui.card::attr(href)').getall()
            card_headers = selector.css('a.ui.card .header::text').getall()
            
            for link, name in zip(card_links, card_headers):
                if link and name:
                    clean_name = name.strip()
                    full_url = urljoin(self.base_url, link)
                    
                    # Skip generic datacenter directory links
                    if link.endswith('/datacenters/') or link.endswith('/facilities/'):
                        continue
                    
                    # Only include links that appear to be facility-specific
                    if len(link.split('/')) >= 5:  # e.g., /usa/delaware/wilmington/facility-name/
                        facilities.append({
                            "name": clean_name,
                            "url": full_url,
                            "city": city_name,
                            "state": state_name
                        })
        
        logger.info(f"Found {len(facilities)} facilities in {city_name}, {state_name}")
        return facilities
    
    def parse_facility_details(self, html: str, facility_url: str) -> Dict[str, Optional[str]]:
        """Parse facility detail page to extract comprehensive information."""
        selector = Selector(text=html)
        
        # Initialize facility data
        facility = {
            "name": None,
            "address_full": None,
            "city": None,
            "state": None,
            "postal_code": None,
            "country": "USA",
            "latitude": None,
            "longitude": None,
            "source_url": facility_url
        }
        
        # Extract facility name
        name_selectors = [
            'h1::text',
            '.facility-name::text',
            '.datacenter-name::text',
            'title::text'
        ]
        
        for selector_str in name_selectors:
            name = selector.css(selector_str).get()
            if name:
                facility["name"] = name.strip()
                break
        
        # Extract address information
        address_selectors = [
            '.address::text',
            '.facility-address::text',
            '.location::text',
            '[class*="address"]::text'
        ]
        
        for selector_str in address_selectors:
            address = selector.css(selector_str).get()
            if address:
                facility["address_full"] = address.strip()
                break
        
        # Try to parse address components if full address exists
        if facility["address_full"]:
            parsed_address = self._parse_address(facility["address_full"])
            facility.update(parsed_address)
        
        # Extract coordinates from various sources
        coordinates = self._extract_coordinates(html, selector)
        if coordinates:
            facility["latitude"], facility["longitude"] = coordinates
        
        # If name not found, try to extract from URL or title
        if not facility["name"]:
            # Try page title
            title = selector.css('title::text').get()
            if title:
                facility["name"] = title.split('|')[0].strip()
            else:
                # Fallback to URL-based name
                path = urlparse(facility_url).path
                facility["name"] = path.split('/')[-1].replace('-', ' ').title()
        
        return facility
    
    def _parse_address(self, address: str) -> Dict[str, Optional[str]]:
        """Parse address string into components."""
        result = {
            "city": None,
            "state": None,
            "postal_code": None
        }
        
        # Common address patterns
        # Pattern 1: "City, ST 12345"
        pattern1 = re.search(r'([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', address)
        if pattern1:
            result["city"] = pattern1.group(1).strip()
            result["state"] = pattern1.group(2).strip()
            result["postal_code"] = pattern1.group(3).strip()
            return result
        
        # Pattern 2: "City, State 12345"
        pattern2 = re.search(r'([^,]+),\s*([A-Za-z\s]+)\s+(\d{5}(?:-\d{4})?)', address)
        if pattern2:
            result["city"] = pattern2.group(1).strip()
            result["state"] = pattern2.group(2).strip()
            result["postal_code"] = pattern2.group(3).strip()
            return result
        
        # Pattern 3: Try to extract just state abbreviation
        state_match = re.search(r'\b([A-Z]{2})\b', address)
        if state_match:
            result["state"] = state_match.group(1)
        
        # Pattern 4: Try to extract postal code
        zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address)
        if zip_match:
            result["postal_code"] = zip_match.group(1)
        
        return result
    
    def _extract_coordinates(self, html: str, selector: Selector) -> Optional[Tuple[float, float]]:
        """Extract latitude and longitude from various sources in the HTML."""
        
        # Method 1: Look for Google Maps embed or API calls
        gmaps_pattern = re.search(r'[@&](-?\d+\.\d+),(-?\d+\.\d+)', html)
        if gmaps_pattern:
            try:
                lat, lng = float(gmaps_pattern.group(1)), float(gmaps_pattern.group(2))
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    return lat, lng
            except ValueError:
                pass
        
        # Method 2: Look for data attributes
        lat_attr = selector.css('[data-lat]::attr(data-lat)').get()
        lng_attr = selector.css('[data-lng]::attr(data-lng)').get()
        
        if lat_attr and lng_attr:
            try:
                lat, lng = float(lat_attr), float(lng_attr)
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    return lat, lng
            except ValueError:
                pass
        
        # Method 3: Look in JavaScript variables
        js_coords = re.search(r'(?:lat|latitude)["\']?\s*[:=]\s*([+-]?\d+\.?\d*)', html, re.IGNORECASE)
        js_coords_lng = re.search(r'(?:lng|lon|longitude)["\']?\s*[:=]\s*([+-]?\d+\.?\d*)', html, re.IGNORECASE)
        
        if js_coords and js_coords_lng:
            try:
                lat, lng = float(js_coords.group(1)), float(js_coords_lng.group(1))
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    return lat, lng
            except ValueError:
                pass
        
        return None
    
    def is_sponsor_link(self, url: str) -> bool:
        """Check if a URL is a sponsor link that should be skipped."""
        sponsor_indicators = [
            '/sponsor/',
            '/ad/',
            '/advertisement/',
            'utm_source',
            'utm_medium',
            'utm_campaign',
            'partner',
            'affiliate'
        ]
        
        return any(indicator in url.lower() for indicator in sponsor_indicators)