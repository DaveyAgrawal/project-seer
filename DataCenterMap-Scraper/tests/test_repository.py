"""
Tests for database repository operations.
"""

import pytest
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import db_manager
from src.core.models import DataCenterFacility
from src.core.repository import FacilityRepository


@pytest.fixture
async def test_session():
    """Create a test database session."""
    # In a real test environment, you'd use a test database
    # For now, we'll use the main session but clean up after
    async with db_manager.session() as session:
        yield session
        # Clean up test data
        await session.execute(
            DataCenterFacility.__table__.delete().where(
                DataCenterFacility.source_url.like('%test-facility%')
            )
        )


@pytest.fixture
def sample_facility_data():
    """Sample facility data for testing."""
    return {
        "name": "Test Data Center",
        "address_full": "123 Test Street, Test City, TC 12345",
        "city": "Test City",
        "state": "TC",
        "postal_code": "12345",
        "country": "USA",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "source": "datacentermap",
        "source_url": "https://datacentermap.com/test-facility-1",
    }


class TestFacilityRepository:
    """Test cases for FacilityRepository."""
    
    async def test_upsert_new_facility(self, test_session: AsyncSession, sample_facility_data):
        """Test inserting a new facility."""
        repository = FacilityRepository(test_session)
        
        # Insert new facility
        facility = await repository.upsert_facility(sample_facility_data)
        
        assert facility.id is not None
        assert facility.name == "Test Data Center"
        assert facility.city == "Test City"
        assert facility.state == "TC"
        assert facility.latitude == 40.7128
        assert facility.longitude == -74.0060
        assert facility.source == "datacentermap"
        assert facility.source_url == "https://datacentermap.com/test-facility-1"
        assert facility.first_seen_at is not None
        assert facility.last_seen_at is not None
    
    async def test_upsert_existing_facility(self, test_session: AsyncSession, sample_facility_data):
        """Test updating an existing facility."""
        repository = FacilityRepository(test_session)
        
        # Insert facility first time
        facility1 = await repository.upsert_facility(sample_facility_data)
        original_first_seen = facility1.first_seen_at
        
        # Update the same facility (same source_url)
        updated_data = sample_facility_data.copy()
        updated_data["name"] = "Updated Test Data Center"
        updated_data["city"] = "Updated City"
        updated_data["latitude"] = 41.0000
        
        facility2 = await repository.upsert_facility(updated_data)
        
        # Should be same ID but updated fields
        assert facility2.id == facility1.id
        assert facility2.name == "Updated Test Data Center"
        assert facility2.city == "Updated City"
        assert facility2.latitude == 41.0000
        assert facility2.first_seen_at == original_first_seen  # Should not change
        assert facility2.last_seen_at > original_first_seen    # Should be updated
    
    async def test_upsert_facilities_batch(self, test_session: AsyncSession):
        """Test batch upsert of multiple facilities."""
        repository = FacilityRepository(test_session)
        
        facilities_data = [
            {
                "name": "Batch Facility 1",
                "source_url": "https://datacentermap.com/test-facility-batch-1",
                "city": "City1",
                "state": "ST1",
            },
            {
                "name": "Batch Facility 2", 
                "source_url": "https://datacentermap.com/test-facility-batch-2",
                "city": "City2",
                "state": "ST2",
            },
            {
                "name": "Batch Facility 3",
                "source_url": "https://datacentermap.com/test-facility-batch-3",
                "city": "City3",
                "state": "ST3",
                "latitude": 39.0,
                "longitude": -77.0,
            }
        ]
        
        facilities = await repository.upsert_facilities_batch(facilities_data)
        
        assert len(facilities) == 3
        assert facilities[0].name == "Batch Facility 1"
        assert facilities[0].city == "City1"
        assert facilities[1].name == "Batch Facility 2"
        assert facilities[2].latitude == 39.0
        assert facilities[2].longitude == -77.0
    
    async def test_get_facility_by_url(self, test_session: AsyncSession, sample_facility_data):
        """Test retrieving facility by source URL."""
        repository = FacilityRepository(test_session)
        
        # Insert facility
        await repository.upsert_facility(sample_facility_data)
        
        # Retrieve by URL
        facility = await repository.get_facility_by_url(sample_facility_data["source_url"])
        
        assert facility is not None
        assert facility.name == "Test Data Center"
        assert facility.source_url == sample_facility_data["source_url"]
        
        # Test non-existent URL
        non_existent = await repository.get_facility_by_url("https://example.com/nonexistent")
        assert non_existent is None
    
    async def test_get_facilities_by_state(self, test_session: AsyncSession):
        """Test retrieving facilities by state."""
        repository = FacilityRepository(test_session)
        
        # Insert facilities in different states
        facilities_data = [
            {
                "name": "Delaware Facility 1",
                "source_url": "https://datacentermap.com/test-facility-de-1",
                "state": "DE",
            },
            {
                "name": "Delaware Facility 2",
                "source_url": "https://datacentermap.com/test-facility-de-2", 
                "state": "DE",
            },
            {
                "name": "California Facility",
                "source_url": "https://datacentermap.com/test-facility-ca-1",
                "state": "CA",
            }
        ]
        
        await repository.upsert_facilities_batch(facilities_data)
        
        # Get Delaware facilities
        de_facilities = await repository.get_facilities_by_state("DE")
        assert len(de_facilities) == 2
        assert all(f.state == "DE" for f in de_facilities)
        
        # Get California facilities
        ca_facilities = await repository.get_facilities_by_state("CA")
        assert len(ca_facilities) == 1
        assert ca_facilities[0].state == "CA"
    
    async def test_get_facility_counts(self, test_session: AsyncSession):
        """Test facility count methods."""
        repository = FacilityRepository(test_session)
        
        # Get initial counts
        initial_total = await repository.get_facility_count()
        initial_with_coords = await repository.get_facilities_with_coordinates_count()
        
        # Insert facilities with and without coordinates
        facilities_data = [
            {
                "name": "With Coordinates",
                "source_url": "https://datacentermap.com/test-facility-coords",
                "latitude": 40.0,
                "longitude": -75.0,
            },
            {
                "name": "Without Coordinates", 
                "source_url": "https://datacentermap.com/test-facility-no-coords",
            }
        ]
        
        await repository.upsert_facilities_batch(facilities_data)
        
        # Check updated counts
        final_total = await repository.get_facility_count()
        final_with_coords = await repository.get_facilities_with_coordinates_count()
        
        assert final_total == initial_total + 2
        assert final_with_coords == initial_with_coords + 1
    
    async def test_get_state_summary(self, test_session: AsyncSession):
        """Test state summary generation."""
        repository = FacilityRepository(test_session)
        
        # Insert facilities in multiple states
        facilities_data = [
            {"name": "DE1", "source_url": "https://datacentermap.com/test-de-1", "state": "DE"},
            {"name": "DE2", "source_url": "https://datacentermap.com/test-de-2", "state": "DE"},
            {"name": "CA1", "source_url": "https://datacentermap.com/test-ca-1", "state": "CA"},
            {"name": "TX1", "source_url": "https://datacentermap.com/test-tx-1", "state": "TX"},
            {"name": "TX2", "source_url": "https://datacentermap.com/test-tx-2", "state": "TX"},
            {"name": "TX3", "source_url": "https://datacentermap.com/test-tx-3", "state": "TX"},
        ]
        
        await repository.upsert_facilities_batch(facilities_data)
        
        summary = await repository.get_state_summary()
        
        # Find our test states in the summary
        test_states = {item["state"]: item["count"] for item in summary 
                      if item["state"] in ["DE", "CA", "TX"]}
        
        assert test_states.get("DE", 0) >= 2  # At least our 2 test facilities
        assert test_states.get("CA", 0) >= 1  # At least our 1 test facility  
        assert test_states.get("TX", 0) >= 3  # At least our 3 test facilities