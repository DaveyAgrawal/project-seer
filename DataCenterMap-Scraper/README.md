# DataCenterMap Scraper

A production-ready web scraper for extracting U.S. data center facility information from datacentermap.com.

## Features

- **Comprehensive Coverage**: Crawls all 50 U.S. states + District of Columbia
- **Facility Data Extraction**: Name, address, city, state, latitude, longitude
- **Database Persistence**: PostgreSQL storage with automatic upsert logic
- **Rate-Limited Crawling**: 1 RPS default with exponential backoff retry
- **Playwright Integration**: Robust browser automation for dynamic content
- **CLI Interface**: State-specific and full crawling options
- **Docker Support**: Easy deployment with PostgreSQL container
- **CSV Export**: Data export functionality with summary statistics
- **Comprehensive Test Suite**: Unit and integration tests

## Quick Start

### 1. Prerequisites

- Python 3.9 or higher
- Docker (for PostgreSQL database)

### 2. Setup Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings (optional - defaults work for development)
vim .env
```

### 3. Install Dependencies

```bash
# Install the package and dependencies
pip3 install -e .

# Install Playwright browsers
python3 -m playwright install
```

### 4. Start Database

```bash
# Start PostgreSQL with Docker
docker-compose up -d postgres

# Initialize database tables
python3 -c "from src.app import app; app()" init-db
```

### 5. Run Crawler

```bash
# Test with a small state (recommended first run)
python3 -c "from src.app import app; app()" crawl --state delaware

# Test with a larger state
python3 -c "from src.app import app; app()" crawl --state california

# Crawl all U.S. states (long-running process)
python3 -c "from src.app import app; app()" crawl --all-states

# Export existing data from database
python3 -c "from src.app import app; app()" export --out exports/all_facilities.csv
```

## CLI Commands

### Crawling Commands

```bash
# Crawl specific state
python3 -c "from src.app import app; app()" crawl --state "delaware"
python3 -c "from src.app import app; app()" crawl --state "new york"

# Crawl all states (50+ states)
python3 -c "from src.app import app; app()" crawl --all-states

# Dry run (show what would be crawled)
python3 -c "from src.app import app; app()" crawl --state delaware --dry-run

# Crawl with CSV export
python3 -c "from src.app import app; app()" crawl --state delaware --out exports/delaware.csv
```

### Database Commands

```bash
# Initialize database tables
python3 -c "from src.app import app; app()" init-db

# Export data from database to CSV
python3 -c "from src.app import app; app()" export --out exports/all_facilities.csv

# Export with limit
python3 -c "from src.app import app; app()" export --out exports/sample.csv --limit 100
```

## Testing Individual States

The following states have been successfully tested:

```bash
# Small states (good for initial testing)
python3 -c "from src.app import app; app()" crawl --state delaware      # ~13 facilities
python3 -c "from src.app import app; app()" crawl --state wyoming       # ~13 facilities

# Medium states  
python3 -c "from src.app import app; app()" crawl --state california    # ~200+ facilities

# Large states (many facilities)
python3 -c "from src.app import app; app()" crawl --state "new york"    # ~130+ facilities
python3 -c "from src.app import app; app()" crawl --state texas         # ~300+ facilities
```

**Note**: State names with spaces must be quoted (e.g., `"new york"`, `"north carolina"`).

## Configuration

Key environment variables (see `.env.example`):

```bash
# Database Configuration
DATABASE_URL=postgresql+psycopg://projectseer:projectseer@localhost:5433/projectseer

# Scraper Configuration
USER_AGENT=ProjectSeerBot/0.1 (+projectseerai@gmail.com)
RATE_LIMIT_DELAY=1.0                    # Seconds between requests
MAX_RETRIES=3                           # Retry attempts for failed requests
REQUEST_TIMEOUT=30                      # HTTP request timeout

# Playwright Configuration
PLAYWRIGHT_TIMEOUT=120000               # Page load timeout (ms)
PLAYWRIGHT_HEADLESS=false               # Run browser in headless mode
VERCEL_CHALLENGE_TIMEOUT=90000          # Vercel protection timeout (ms)

# Export Settings
DEFAULT_EXPORT_DIR=exports              # Default CSV export directory
LOG_LEVEL=INFO                          # Logging level
```

## Architecture

```
src/
├── app.py                   # CLI application entry point
├── core/                    # Infrastructure components
│   ├── config.py           # Configuration management
│   ├── database.py         # Database connection and management
│   ├── logging.py          # Logging configuration
│   ├── models.py           # SQLAlchemy models
│   └── repository.py       # Database operations
└── dcm/                     # DataCenterMap-specific logic
    ├── crawler.py          # Main crawler orchestrator
    ├── parsers.py          # HTML parsing logic
    ├── playwright_client.py # Browser automation
    ├── http_client.py      # HTTP client (legacy)
    └── exporter.py         # CSV export functionality

tests/                       # Comprehensive test suite
├── fixtures/               # Test data and HTML fixtures
├── test_crawler.py        # Crawler tests
├── test_parsers.py        # Parser tests
├── test_integration.py    # End-to-end tests
└── ...

debug/                      # Debug files and state samples
exports/                    # CSV output directory
```

## Data Schema

The scraper extracts and stores the following facility information:

```python
{
    "name": "Facility Name",
    "address_full": "123 Main St, City, State 12345",
    "city": "City Name", 
    "state": "State Name",
    "latitude": 40.7128,
    "longitude": -74.0060,
    "source": "datacentermap",
    "source_url": "https://datacentermap.com/...",
    "first_seen_at": "2024-08-05T12:00:00Z",
    "last_seen_at": "2024-08-05T12:00:00Z"
}
```

## Development

### Setup Development Environment

```bash
# Install development dependencies
pip3 install -e ".[dev]"

# Install Playwright browsers
python3 -m playwright install

# Initialize database
python3 -c "from src.app import app; app()" init-db
```

### Running Tests

```bash
# Run unit tests (fast, using mocked data)
python3 -m pytest -v -m "not integration"

# Run all tests including integration tests (hits real website)
python3 -m pytest -v

# Run only integration tests (hits datacentermap.com)
python3 -m pytest -v -m integration

# Run tests with quiet output
python3 -m pytest -q
```

### Code Quality

```bash
# Format code
python3 -m black src/ tests/

# Check code style  
python3 -m ruff check src/ tests/

# Type checking
python3 -m mypy src/
```

### Python Version Requirements

This project requires **Python 3.9 or higher**. On systems with older Python versions:

- Use `python3` instead of `python`
- Use `pip3` instead of `pip`  
- Use `python3 -m pytest` instead of `pytest`

## Performance & Scale

- **Rate Limiting**: Default 1 RPS (configurable)
- **Concurrent Processing**: Async/await for I/O operations
- **Database**: PostgreSQL with connection pooling
- **Memory Efficient**: Streaming processing, no full dataset in memory
- **Error Handling**: Comprehensive retry logic and error recovery

## Deployment

### Docker Deployment

```bash
# Start database
docker-compose up -d postgres

# Build and run scraper (example)
docker build -t datacentermap-scraper .
docker run --network host datacentermap-scraper crawl --all-states
```

### Production Considerations

- Set `PLAYWRIGHT_HEADLESS=true` for server environments
- Configure appropriate `RATE_LIMIT_DELAY` for production loads
- Monitor database disk space (facilities table grows with crawls)
- Set up log rotation for production logging
- Consider running as scheduled job (cron) for regular updates

## Troubleshooting

### Common Issues

**Import Errors**: Ensure you're in the project root directory and have installed dependencies with `pip3 install -e .`

**Database Connection**: Verify PostgreSQL is running with `docker-compose ps`

**Playwright Issues**: Install browsers with `python3 -m playwright install`

**State Not Found**: Check state name spelling; use quotes for multi-word states

### Debug Mode

```bash
# Enable debug logging
LOG_LEVEL=DEBUG python3 -c "from src.app import app; app()" crawl --state delaware

# Save debug HTML files (check debug/ directory)
# Modify crawler.py to enable HTML debugging
```

## License

MIT License - see project files for details.