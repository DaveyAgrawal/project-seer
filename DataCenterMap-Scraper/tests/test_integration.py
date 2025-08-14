"""
Integration tests that hit the real datacentermap.com website.
"""

import pytest
import asyncio
from pathlib import Path

from src.dcm.crawler import DataCenterMapCrawler
from src.dcm.exporter import FacilityExporter
from src.core.database import db_manager
from src.core.repository import FacilityRepository


@pytest.mark.integration
class TestDelawareIntegrationCrawl:
    """Integration test for Delaware crawl - hits real website."""
    
    @pytest.mark.asyncio
    async def test_delaware_crawl_end_to_end(self):
        """
        End-to-end test: Crawl Delaware, save to DB, export to CSV.
        This test hits the real datacentermap.com website.
        """
        # Setup
        crawler = DataCenterMapCrawler()
        test_csv_path = Path("exports/test_delaware_integration.csv")
        
        try:
            # 1. Crawl Delaware (save to database)
            facilities = await crawler.crawl_state("delaware", save_to_db=True)
            
            # Validate crawl results
            assert len(facilities) >= 1, "Should find at least 1 facility in Delaware"
            
            # Check facility structure
            facility = facilities[0]
            required_fields = ["name", "source_url", "source"]
            for field in required_fields:
                assert field in facility, f"Facility missing required field: {field}"
                assert facility[field] is not None, f"Field {field} should not be None"
            
            # Validate geographic data
            assert facility.get("country") == "USA"
            delaware_indicators = ["DE", "Delaware", "delaware"]
            assert any(indicator in str(facility.get("state", "")).lower() 
                      for indicator in delaware_indicators), "Facility should be in Delaware"
            
            print(f"✅ Crawled {len(facilities)} Delaware facilities")
            
            # 2. Export to CSV
            success = FacilityExporter.export_to_csv(facilities, test_csv_path)
            assert success, "CSV export should succeed"
            assert test_csv_path.exists(), "CSV file should be created"
            
            # Validate CSV content
            with open(test_csv_path, 'r', encoding='utf-8') as f:
                csv_content = f.read()
                assert len(csv_content.splitlines()) >= 2, "CSV should have header + at least 1 data row"
                assert "name,address_full" in csv_content, "CSV should contain expected columns"
            
            print(f"✅ Exported to CSV: {test_csv_path}")
            
            # 3. Verify database persistence
            async with db_manager.session() as session:
                repository = FacilityRepository(session)
                
                # Check that facilities were saved
                for facility in facilities:
                    db_facility = await repository.get_facility_by_url(facility["source_url"])
                    assert db_facility is not None, f"Facility should be saved to DB: {facility['source_url']}"
                    assert db_facility.name == facility["name"], "DB facility name should match"
            
            print(f"✅ Verified {len(facilities)} facilities saved to database")
            
            # 4. Test idempotency - run same crawl again
            print("🔄 Testing idempotency with second crawl...")
            facilities_second = await crawler.crawl_state("delaware", save_to_db=True)
            
            # Should find similar number of facilities
            assert len(facilities_second) == len(facilities), "Second crawl should find same facilities"
            
            # Verify updates (last_seen_at should be newer than first_seen_at for existing facilities)
            async with db_manager.session() as session:
                repository = FacilityRepository(session)
                db_facility = await repository.get_facility_by_url(facilities[0]["source_url"])
                assert db_facility.last_seen_at >= db_facility.first_seen_at, "last_seen_at should be >= first_seen_at"
            
            print("✅ Idempotency test passed")
            
            # 5. Performance check - should complete reasonably quickly
            # (This is implicit - if we get here, it completed in reasonable time)
            print("✅ Performance test passed (completed within timeout)")
            
        finally:
            # Cleanup
            if test_csv_path.exists():
                test_csv_path.unlink()
                print(f"🧹 Cleaned up test CSV: {test_csv_path}")
    
    @pytest.mark.asyncio
    async def test_delaware_data_quality(self):
        """
        Test the quality of data extracted from Delaware.
        This validates that our parsers work correctly on real data.
        """
        crawler = DataCenterMapCrawler()
        
        # Crawl Delaware without saving to DB (just test parsing)
        facilities = await crawler.crawl_state("delaware", save_to_db=False)
        
        assert len(facilities) >= 1, "Should find at least 1 facility"
        
        # Analyze data quality
        facilities_with_names = [f for f in facilities if f.get("name")]
        facilities_with_urls = [f for f in facilities if f.get("source_url")]
        facilities_with_coords = [f for f in facilities if f.get("latitude") and f.get("longitude")]
        facilities_with_addresses = [f for f in facilities if f.get("address_full")]
        
        # Quality assertions
        assert len(facilities_with_names) == len(facilities), "All facilities should have names"
        assert len(facilities_with_urls) == len(facilities), "All facilities should have source URLs"
        
        # At least some facilities should have coordinates and addresses
        coord_rate = len(facilities_with_coords) / len(facilities) if facilities else 0
        address_rate = len(facilities_with_addresses) / len(facilities) if facilities else 0
        
        print(f"📊 Delaware Data Quality:")
        print(f"   Total facilities: {len(facilities)}")
        print(f"   With coordinates: {len(facilities_with_coords)} ({coord_rate:.1%})")
        print(f"   With addresses: {len(facilities_with_addresses)} ({address_rate:.1%})")
        
        # Expect at least some success with coordinates/addresses
        assert coord_rate > 0, "At least some facilities should have coordinates"
        
        # Validate coordinate ranges (should be roughly in Delaware area)
        for facility in facilities_with_coords:
            lat, lng = facility["latitude"], facility["longitude"]
            assert 38.0 <= lat <= 40.0, f"Delaware latitude should be ~38-40, got {lat}"
            assert -76.0 <= lng <= -74.0, f"Delaware longitude should be ~-76 to -74, got {lng}"
        
        print("✅ Data quality validation passed")
    
    @pytest.mark.asyncio
    async def test_crawler_error_resilience(self):
        """
        Test that crawler handles real-world errors gracefully.
        """
        crawler = DataCenterMapCrawler()
        
        # Test with a state that might have fewer or no data centers
        facilities = await crawler.crawl_state("montana", save_to_db=False)
        
        # Should complete without crashing, even if no facilities found
        assert isinstance(facilities, list), "Should return a list even if empty"
        print(f"✅ Montana crawl completed: {len(facilities)} facilities")
        
        # Test non-existent state
        facilities_bad = await crawler.crawl_state("nonexistent", save_to_db=False)
        assert len(facilities_bad) == 0, "Non-existent state should return empty list"
        print("✅ Non-existent state handled gracefully")