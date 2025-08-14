"""
Tests for CSV export functionality.
"""

import pytest
from pathlib import Path
from datetime import datetime
import csv
import tempfile

from src.dcm.exporter import FacilityExporter


@pytest.fixture
def sample_facilities():
    """Sample facility data for testing export."""
    return [
        {
            "id": 1,
            "name": "Test Data Center 1",
            "address_full": "123 Main St, Test City, TC 12345",
            "city": "Test City",
            "state": "TC",
            "postal_code": "12345",
            "country": "USA",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "source": "datacentermap",
            "source_url": "https://datacentermap.com/test-1",
            "first_seen_at": datetime(2024, 1, 1, 12, 0, 0),
            "last_seen_at": datetime(2024, 1, 2, 12, 0, 0),
        },
        {
            "id": 2,
            "name": "Test Data Center 2",
            "address_full": "456 Tech Ave, Another City, AC 67890",
            "city": "Another City",
            "state": "AC",
            "postal_code": "67890",
            "country": "USA",
            "latitude": 39.9526,
            "longitude": -75.1652,
            "source": "datacentermap",
            "source_url": "https://datacentermap.com/test-2",
            "first_seen_at": datetime(2024, 1, 1, 15, 0, 0),
            "last_seen_at": datetime(2024, 1, 2, 16, 0, 0),
        },
        {
            "id": 3,
            "name": "Test Data Center 3",
            "address_full": None,  # Test missing address
            "city": None,
            "state": "NC",
            "postal_code": None,
            "country": "USA",
            "latitude": None,  # Test missing coordinates
            "longitude": None,
            "source": "datacentermap",
            "source_url": "https://datacentermap.com/test-3",
            "first_seen_at": datetime(2024, 1, 3, 10, 0, 0),
            "last_seen_at": datetime(2024, 1, 3, 10, 0, 0),
        }
    ]


class TestFacilityExporter:
    """Test cases for FacilityExporter."""
    
    def test_export_to_csv_basic(self, sample_facilities):
        """Test basic CSV export functionality."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            # Export to CSV
            success = FacilityExporter.export_to_csv(sample_facilities, tmp_path)
            assert success
            assert tmp_path.exists()
            
            # Read and validate CSV content
            with open(tmp_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            assert len(rows) == 3
            
            # Check first row
            row1 = rows[0]
            assert row1["name"] == "Test Data Center 1"
            assert row1["city"] == "Test City"
            assert row1["state"] == "TC"
            assert row1["latitude"] == "40.7128"
            assert row1["longitude"] == "-74.006"
            assert row1["source"] == "datacentermap"
            assert "2024-01-01T12:00:00" in row1["first_seen_at"]
            
            # Check row with missing data
            row3 = rows[2]
            assert row3["name"] == "Test Data Center 3"
            assert row3["address_full"] == ""  # Should be empty, not None
            assert row3["latitude"] == ""
            assert row3["longitude"] == ""
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_export_to_csv_no_headers(self, sample_facilities):
        """Test CSV export without headers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            success = FacilityExporter.export_to_csv(
                sample_facilities, tmp_path, include_headers=False
            )
            assert success
            
            with open(tmp_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Should have 3 data rows, no header
            assert len(lines) == 3
            assert "name" not in lines[0]  # No header row
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_export_to_csv_empty_list(self):
        """Test CSV export with empty facility list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            success = FacilityExporter.export_to_csv([], tmp_path)
            assert not success  # Should return False for empty list
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_export_to_csv_directory_creation(self, sample_facilities):
        """Test that export creates parent directories."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            nested_path = Path(tmp_dir) / "subdir" / "nested" / "test.csv"
            
            success = FacilityExporter.export_to_csv(sample_facilities, nested_path)
            assert success
            assert nested_path.exists()
            assert nested_path.parent.exists()
    
    def test_export_summary_basic(self, sample_facilities):
        """Test summary generation."""
        summary = FacilityExporter.export_summary(sample_facilities)
        
        assert summary["total_facilities"] == 3
        assert summary["facilities_with_coordinates"] == 2  # First two have coords
        assert summary["facilities_with_full_address"] == 2  # First two have addresses
        assert summary["unique_states"] == 3  # TC, AC, NC
        assert summary["unique_cities"] == 2  # Two cities (one None)
        assert "coordinate_success_rate" in summary
        assert summary["coordinate_success_rate"] == 66.7  # 2/3 * 100, rounded to 1 decimal
        
        # Check state breakdown
        state_breakdown = summary["state_breakdown"]
        assert state_breakdown["TC"] == 1
        assert state_breakdown["AC"] == 1
        assert state_breakdown["NC"] == 1
    
    def test_export_summary_empty_list(self):
        """Test summary generation with empty list."""
        summary = FacilityExporter.export_summary([])
        assert summary == {}
    
    def test_export_summary_to_file(self, sample_facilities):
        """Test summary export to file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            summary = FacilityExporter.export_summary(sample_facilities, tmp_path)
            
            assert tmp_path.exists()
            
            # Read and validate summary file
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            assert "DataCenterMap Scraper - Export Summary" in content
            assert "Total facilities: 3" in content
            assert "Facilities with coordinates: 2" in content
            assert "State Breakdown:" in content
            assert "TC: 1" in content
            assert "AC: 1" in content
            assert "NC: 1" in content
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_csv_utf8_encoding(self):
        """Test CSV export handles UTF-8 characters correctly."""
        facilities_with_unicode = [
            {
                "id": 1,
                "name": "Datacenter München", # German umlaut
                "city": "São Paulo",          # Portuguese
                "state": "SP",
                "address_full": "123 Rua José, São Paulo, SP 01234-567",
                "source": "datacentermap",
                "source_url": "https://datacentermap.com/test-unicode",
                "first_seen_at": datetime(2024, 1, 1),
                "last_seen_at": datetime(2024, 1, 1),
            }
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            success = FacilityExporter.export_to_csv(facilities_with_unicode, tmp_path)
            assert success
            
            # Read back with UTF-8 encoding
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            assert "Datacenter München" in content
            assert "São Paulo" in content
            assert "José" in content
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()