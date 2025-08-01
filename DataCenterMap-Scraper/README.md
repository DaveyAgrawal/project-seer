# DataCenterMap Scraper

A production-ready web scraper for extracting U.S. data center facility information from datacentermap.com.

## Features

- Extract facility name, address, latitude, longitude for U.S. data centers
- Persist data to PostgreSQL with timestamps
- Rate-limited crawling (1 RPS) with exponential backoff
- CLI interface with state-specific and full crawling options
- Docker support for easy deployment
- CSV export functionality
- Comprehensive test suite

## Quick Start

### 1. Setup Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings
vim .env
```

### 2. Start Database

```bash
# Start PostgreSQL with Docker
docker-compose up -d postgres

# Initialize database tables
python3 -m src.app init-db
```

### 3. Install Dependencies

```bash
pip3 install -e .
```

### 4. Run Crawler

```bash
# Crawl Delaware (quick test)
dcm crawl --state "delaware" --out exports/delaware.csv

# Crawl all U.S. states
dcm crawl --all-states

# Export existing data
dcm export --out exports/all_facilities.csv
```

## CLI Commands

- `dcm crawl --state "delaware" [--out PATH] [--dry-run]` - Crawl specific state
- `dcm crawl --all-states` - Crawl all U.S. states  
- `dcm export --out PATH` - Export data to CSV
- `dcm init-db` - Initialize database tables

## Configuration

Key environment variables (see `.env.example`):

- `DATABASE_URL` - PostgreSQL connection string
- `USER_AGENT` - Bot identification for requests
- `RATE_LIMIT_DELAY` - Delay between requests (default: 1.0s)
- `MAX_RETRIES` - Maximum retry attempts (default: 3)

## Development

### Python Version Requirements

This project requires Python 3.9 or higher. If you have an older system Python, you may need to use specific commands:

**macOS/Linux with system Python 3.9:**
- Use `python3` instead of `python`
- Use `pip3` instead of `pip`
- Use `python3 -m pytest` instead of `pytest`

### Setup Development Environment

```bash
# Install development dependencies
pip3 install -e ".[dev]"

# Initialize database
python3 -m src.app init-db
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

## Architecture

- `src/dcm/` - Core scraping logic (parsers, crawler)
- `src/core/` - Infrastructure (database, config, models)
- `tests/` - Test suite with fixtures
- `exports/` - CSV output directory