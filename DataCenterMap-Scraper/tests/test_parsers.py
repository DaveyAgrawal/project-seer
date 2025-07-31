"""
Tests for HTML parsers.
"""

import pytest

from src.dcm.parsers import DataCenterMapParser
from tests.fixtures.sample_html import (
    USA_PAGE_HTML,
    DELAWARE_STATE_HTML,
    WILMINGTON_CITY_HTML,
    FACILITY_DETAIL_HTML,
    FACILITY_WITH_DATA_ATTRS_HTML,
    FACILITY_MINIMAL_HTML,
)


class TestDataCenterMapParser:
    """Test cases for DataCenterMapParser."""
    
    def setup_method(self):
        """Set up parser for each test."""
        self.parser = DataCenterMapParser()
    
    def test_parse_usa_states(self):
        """Test parsing state links from USA page."""
        states = self.parser.parse_usa_states(USA_PAGE_HTML)
        
        assert len(states) == 7
        
        # Check specific states
        delaware = next((s for s in states if s["name"] == "Delaware"), None)
        assert delaware is not None
        assert delaware["url"] == "https://www.datacentermap.com/usa/delaware/"
        assert delaware["slug"] == "delaware"
        
        california = next((s for s in states if s["name"] == "California"), None)
        assert california is not None
        assert california["slug"] == "california"
    
    def test_parse_state_cities(self):
        """Test parsing city links from state page."""
        cities = self.parser.parse_state_cities(DELAWARE_STATE_HTML, "delaware")
        
        assert len(cities) == 3
        
        # Check specific city
        wilmington = next((c for c in cities if c["name"] == "Wilmington"), None)
        assert wilmington is not None
        assert wilmington["url"] == "https://www.datacentermap.com/usa/delaware/wilmington/"
        assert wilmington["state"] == "delaware"
        assert wilmington["slug"] == "wilmington"
    
    def test_parse_city_facilities(self):
        """Test parsing facility links from city page."""
        facilities = self.parser.parse_city_facilities(WILMINGTON_CITY_HTML, "Wilmington", "Delaware")
        
        assert len(facilities) == 3
        
        # Check specific facility
        dc1 = next((f for f in facilities if "Delaware Data Center 1" in f["name"]), None)
        assert dc1 is not None
        assert dc1["url"] == "https://www.datacentermap.com/datacenters/delaware-data-center-1/"
        assert dc1["city"] == "Wilmington"
        assert dc1["state"] == "Delaware"
    
    def test_parse_facility_details_complete(self):
        """Test parsing complete facility details."""
        facility = self.parser.parse_facility_details(
            FACILITY_DETAIL_HTML, 
            "https://www.datacentermap.com/datacenters/delaware-data-center-1/"
        )
        
        assert facility["name"] == "Delaware Data Center 1"
        assert facility["address_full"] == "123 Technology Drive, Wilmington, DE 19801"
        assert facility["city"] == "Wilmington"
        assert facility["state"] == "DE"
        assert facility["postal_code"] == "19801"
        assert facility["country"] == "USA"
        assert facility["latitude"] == 39.7391
        assert facility["longitude"] == -75.5398
        assert facility["source_url"] == "https://www.datacentermap.com/datacenters/delaware-data-center-1/"
    
    def test_parse_facility_details_with_data_attrs(self):
        """Test parsing facility with data attributes for coordinates."""
        facility = self.parser.parse_facility_details(
            FACILITY_WITH_DATA_ATTRS_HTML,
            "https://www.datacentermap.com/facilities/tech-hub/"
        )
        
        assert facility["name"] == "Wilmington Tech Hub"
        assert facility["address_full"] == "456 Innovation Way, Newark, DE 19702"
        assert facility["city"] == "Newark"
        assert facility["state"] == "DE" 
        assert facility["postal_code"] == "19702"
        assert facility["latitude"] == 39.6837
        assert facility["longitude"] == -75.7497
    
    def test_parse_facility_details_minimal(self):
        """Test parsing minimal facility details."""
        facility = self.parser.parse_facility_details(
            FACILITY_MINIMAL_HTML,
            "https://www.datacentermap.com/datacenters/east-coast/"
        )
        
        assert facility["name"] == "East Coast Data Center"
        assert facility["country"] == "USA"
        assert facility["source_url"] == "https://www.datacentermap.com/datacenters/east-coast/"
        assert facility["latitude"] is None
        assert facility["longitude"] is None
    
    def test_parse_address_components(self):
        """Test address parsing into components."""
        # Test standard format
        result = self.parser._parse_address("123 Main St, Wilmington, DE 19801")
        assert result["city"] == "Wilmington"
        assert result["state"] == "DE"
        assert result["postal_code"] == "19801"
        
        # Test with full state name
        result = self.parser._parse_address("456 Tech Dr, Newark, Delaware 19702")
        assert result["city"] == "Newark"
        assert result["state"] == "Delaware"
        assert result["postal_code"] == "19702"
        
        # Test partial address
        result = self.parser._parse_address("789 Data Center Blvd, CA")
        assert result["state"] == "CA"
        assert result["city"] is None
        assert result["postal_code"] is None
    
    def test_extract_coordinates_from_javascript(self):
        """Test coordinate extraction from JavaScript variables."""
        html = """
        <script>
            var lat = 40.7128;
            var longitude = -74.0060;
        </script>
        """
        from parsel import Selector
        coords = self.parser._extract_coordinates(html, Selector(text=html))
        assert coords == (40.7128, -74.0060)
    
    def test_extract_coordinates_from_google_maps(self):
        """Test coordinate extraction from Google Maps URLs."""
        html = '<iframe src="https://maps.google.com/maps?q=39.7391,-75.5398&z=15"></iframe>'
        from parsel import Selector
        coords = self.parser._extract_coordinates(html, Selector(text=html))
        assert coords == (39.7391, -75.5398)
    
    def test_is_sponsor_link(self):
        """Test sponsor link detection."""
        assert self.parser.is_sponsor_link("https://example.com/sponsor/datacenter")
        assert self.parser.is_sponsor_link("https://example.com/datacenter?utm_source=partner")
        assert self.parser.is_sponsor_link("https://example.com/affiliate/link")
        assert not self.parser.is_sponsor_link("https://datacentermap.com/usa/delaware/")
        assert not self.parser.is_sponsor_link("https://datacentermap.com/datacenters/facility-1/")