"""
Tests for the crawler functionality.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.dcm.crawler import DataCenterMapCrawler
from tests.fixtures.sample_html import (
    USA_PAGE_HTML,
    DELAWARE_STATE_HTML, 
    WILMINGTON_CITY_HTML,
    FACILITY_DETAIL_HTML,
)


@pytest.fixture
def mock_http_client():
    """Mock HTTP client for testing."""
    client = AsyncMock()
    
    # Mock response mapping
    def mock_get(url):
        if url.endswith("/usa/"):
            return USA_PAGE_HTML
        elif url.endswith("/usa/delaware/"):
            return DELAWARE_STATE_HTML
        elif url.endswith("/usa/delaware/wilmington/"):
            return WILMINGTON_CITY_HTML
        elif "delaware-data-center-1" in url:
            return FACILITY_DETAIL_HTML
        else:
            return None
    
    client.get.side_effect = mock_get
    return client


@pytest.fixture
def mock_crawler(mock_http_client):
    """Create crawler with mocked HTTP client."""
    crawler = DataCenterMapCrawler()
    crawler.client = mock_http_client
    return crawler


class TestDataCenterMapCrawler:
    """Test cases for DataCenterMapCrawler."""
    
    async def test_get_states(self, mock_crawler):
        """Test fetching and parsing states."""
        states = await mock_crawler._get_states()
        
        assert len(states) == 7
        delaware = next((s for s in states if s["name"] == "Delaware"), None)
        assert delaware is not None
        assert delaware["slug"] == "delaware"
        assert delaware["url"] == "https://www.datacentermap.com/usa/delaware/"
    
    @patch('src.dcm.crawler.DataCenterMapCrawler._save_facilities_to_db')
    async def test_crawl_state_delaware(self, mock_save_db, mock_crawler):
        """Test crawling Delaware state with mocked responses."""
        mock_save_db.return_value = None  # Mock database save
        
        facilities = await mock_crawler._crawl_state(
            {"name": "Delaware", "url": "https://www.datacentermap.com/usa/delaware/", "slug": "delaware"},
            save_to_db=False  # Skip DB save for this test
        )
        
        assert len(facilities) >= 1
        
        # Check first facility
        facility = facilities[0]
        assert facility["name"] == "Delaware Data Center 1"
        assert facility["address_full"] == "123 Technology Drive, Wilmington, DE 19801"
        assert facility["city"] == "Wilmington"
        assert facility["state"] == "DE"
        assert facility["postal_code"] == "19801"
        assert facility["latitude"] == 39.7391
        assert facility["longitude"] == -75.5398
        assert facility["source"] == "datacentermap"
        assert "delaware-data-center-1" in facility["source_url"]
    
    @patch('src.dcm.crawler.DataCenterMapCrawler._save_facilities_to_db')
    async def test_crawl_city_wilmington(self, mock_save_db, mock_crawler):
        """Test crawling Wilmington city."""
        mock_save_db.return_value = None
        
        city_info = {
            "name": "Wilmington",
            "url": "https://www.datacentermap.com/usa/delaware/wilmington/",
            "state": "Delaware",
            "slug": "wilmington"
        }
        
        facilities = await mock_crawler._crawl_city(city_info, save_to_db=False)
        
        assert len(facilities) >= 1
        facility = facilities[0]
        assert facility["name"] == "Delaware Data Center 1"
        assert facility["city"] == "Wilmington"
        assert facility["state"] == "DE"
    
    async def test_crawl_facility_details(self, mock_crawler):
        """Test crawling individual facility details."""
        facility_info = {
            "name": "Delaware Data Center 1",
            "url": "https://www.datacentermap.com/datacenters/delaware-data-center-1/",
            "city": "Wilmington",
            "state": "Delaware"
        }
        
        detailed_facility = await mock_crawler._crawl_facility(facility_info)
        
        assert detailed_facility is not None
        assert detailed_facility["name"] == "Delaware Data Center 1"
        assert detailed_facility["address_full"] == "123 Technology Drive, Wilmington, DE 19801"
        assert detailed_facility["latitude"] == 39.7391
        assert detailed_facility["longitude"] == -75.5398
        assert detailed_facility["source"] == "datacentermap"
    
    @patch('src.dcm.crawler.DataCenterMapCrawler._save_facilities_to_db')
    async def test_crawl_state_by_name(self, mock_save_db, mock_crawler):
        """Test crawling by state name."""
        mock_save_db.return_value = None
        
        facilities = await mock_crawler.crawl_state("delaware", save_to_db=False)
        
        assert len(facilities) >= 1
        assert all(f.get("state") in ["DE", "Delaware"] for f in facilities)
    
    @patch('src.dcm.crawler.DataCenterMapCrawler._save_facilities_to_db')
    async def test_crawl_nonexistent_state(self, mock_save_db, mock_crawler):
        """Test crawling non-existent state."""
        mock_save_db.return_value = None
        
        facilities = await mock_crawler.crawl_state("nonexistent", save_to_db=False)
        
        assert len(facilities) == 0
    
    def test_processed_urls_tracking(self, mock_crawler):
        """Test that processed URLs are tracked to avoid duplicates."""
        url = "https://example.com/facility-1"
        
        assert url not in mock_crawler.processed_urls
        mock_crawler.processed_urls.add(url)
        assert url in mock_crawler.processed_urls
    
    def test_stats_tracking(self, mock_crawler):
        """Test that statistics are tracked properly."""
        assert mock_crawler.stats["states_processed"] == 0
        assert mock_crawler.stats["cities_processed"] == 0
        assert mock_crawler.stats["facilities_found"] == 0
        assert mock_crawler.stats["facilities_processed"] == 0
        assert mock_crawler.stats["facilities_with_coords"] == 0
        assert mock_crawler.stats["errors"] == 0
        
        # Stats should be updated during crawl
        mock_crawler.stats["facilities_found"] += 1
        assert mock_crawler.stats["facilities_found"] == 1
    
    async def test_error_handling_bad_state_page(self, mock_crawler):
        """Test error handling when state page fails to load."""
        # Mock client to return None for state page
        mock_crawler.client.get.return_value = None
        
        facilities = await mock_crawler._crawl_state(
            {"name": "BadState", "url": "https://example.com/bad", "slug": "bad"},
            save_to_db=False
        )
        
        assert len(facilities) == 0
    
    async def test_error_handling_bad_facility_page(self, mock_crawler):
        """Test error handling when facility page fails to load."""
        # Mock successful city page but failed facility page
        def mock_get(url):
            if "wilmington" in url:
                return WILMINGTON_CITY_HTML
            else:
                return None  # Facility pages fail
        
        mock_crawler.client.get.side_effect = mock_get
        
        city_info = {
            "name": "Wilmington",
            "url": "https://www.datacentermap.com/usa/delaware/wilmington/",
            "state": "Delaware",
            "slug": "wilmington"
        }
        
        facilities = await mock_crawler._crawl_city(city_info, save_to_db=False)
        
        # Should handle gracefully and return empty list
        assert len(facilities) == 0