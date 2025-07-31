"""
Pytest configuration and shared fixtures.
"""

import pytest
import asyncio
from pathlib import Path

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may be slow)"
    )


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the entire test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_exports_dir():
    """Ensure test exports directory exists."""
    exports_dir = Path("exports")
    exports_dir.mkdir(exist_ok=True)
    return exports_dir


@pytest.fixture(autouse=True)
def cleanup_test_files():
    """Clean up test files after each test."""
    yield
    
    # Clean up any test CSV files
    test_patterns = [
        "exports/test_*.csv",
        "exports/*_test.csv",
        "test_*.csv",
    ]
    
    for pattern in test_patterns:
        for file_path in Path(".").glob(pattern):
            try:
                file_path.unlink()
            except FileNotFoundError:
                pass