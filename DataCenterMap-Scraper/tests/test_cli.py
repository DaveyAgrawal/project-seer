"""
Tests for CLI functionality.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from pathlib import Path
from typer.testing import CliRunner
import tempfile

from src.app import app


@pytest.fixture
def cli_runner():
    """CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_db_manager():
    """Mock database manager."""
    with patch('src.app.db_manager') as mock:
        mock.session.return_value.__aenter__ = AsyncMock()
        mock.session.return_value.__aexit__ = AsyncMock()
        mock.create_tables = AsyncMock()
        yield mock


class TestCLI:
    """Test CLI commands."""
    
    def test_init_db_command(self, cli_runner, mock_db_manager):
        """Test init-db command."""
        with patch('src.core.config.config') as mock_config:
            mock_config.validate.return_value = None
            
            result = cli_runner.invoke(app, ["init-db"])
            
            assert result.exit_code == 0
            assert "Creating database tables" in result.output or "Database tables created" in result.output
    
    @patch('src.app.asyncio.run')
    def test_crawl_command_state(self, mock_asyncio_run, cli_runner):
        """Test crawl command with state parameter."""
        mock_asyncio_run.return_value = None
        
        result = cli_runner.invoke(app, ["crawl", "--state", "delaware"])
        
        assert result.exit_code == 0
        mock_asyncio_run.assert_called_once()
    
    @patch('src.app.asyncio.run')
    def test_crawl_command_all_states(self, mock_asyncio_run, cli_runner):
        """Test crawl command with all-states parameter."""
        mock_asyncio_run.return_value = None
        
        result = cli_runner.invoke(app, ["crawl", "--all-states"])
        
        assert result.exit_code == 0
        mock_asyncio_run.assert_called_once()
    
    def test_crawl_command_no_parameters(self, cli_runner):
        """Test crawl command without required parameters."""
        result = cli_runner.invoke(app, ["crawl"])
        
        assert result.exit_code == 1
        assert "Must specify either --state or --all-states" in result.output
    
    def test_crawl_command_both_parameters(self, cli_runner):
        """Test crawl command with conflicting parameters."""
        result = cli_runner.invoke(app, ["crawl", "--state", "delaware", "--all-states"])
        
        assert result.exit_code == 1
        assert "Cannot specify both --state and --all-states" in result.output
    
    @patch('src.app.asyncio.run')
    def test_crawl_command_dry_run(self, mock_asyncio_run, cli_runner):
        """Test crawl command with dry-run flag."""
        mock_asyncio_run.return_value = None
        
        result = cli_runner.invoke(app, ["crawl", "--state", "delaware", "--dry-run"])
        
        assert result.exit_code == 0
        mock_asyncio_run.assert_called_once()
    
    @patch('src.app.asyncio.run')
    def test_crawl_command_with_output(self, mock_asyncio_run, cli_runner):
        """Test crawl command with CSV output."""
        mock_asyncio_run.return_value = None
        
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = cli_runner.invoke(app, [
                "crawl", "--state", "delaware", "--out", tmp_path
            ])
            
            assert result.exit_code == 0
            mock_asyncio_run.assert_called_once()
            
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    
    @patch('src.app.asyncio.run')
    def test_export_command(self, mock_asyncio_run, cli_runner):
        """Test export command."""
        mock_asyncio_run.return_value = None
        
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = cli_runner.invoke(app, ["export", "--out", tmp_path])
            
            assert result.exit_code == 0
            mock_asyncio_run.assert_called_once()
            
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    
    @patch('src.app.asyncio.run')
    def test_export_command_with_limit(self, mock_asyncio_run, cli_runner):
        """Test export command with limit parameter."""
        mock_asyncio_run.return_value = None
        
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = cli_runner.invoke(app, ["export", "--out", tmp_path, "--limit", "100"])
            
            assert result.exit_code == 0
            mock_asyncio_run.assert_called_once()
            
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    
    def test_export_command_missing_output(self, cli_runner):
        """Test export command without required output parameter."""
        result = cli_runner.invoke(app, ["export"])
        
        assert result.exit_code == 2  # Typer exit code for missing required parameter
        assert "Missing option" in result.output or "required" in result.output.lower()


class TestCLIIntegration:
    """Integration tests for CLI commands."""
    
    @pytest.mark.integration
    def test_help_command(self, cli_runner):
        """Test that help command works."""
        result = cli_runner.invoke(app, ["--help"])
        
        assert result.exit_code == 0
        assert "DataCenterMap scraper CLI" in result.output
        assert "init-db" in result.output
        assert "crawl" in result.output
        assert "export" in result.output
    
    @pytest.mark.integration
    def test_crawl_help(self, cli_runner):
        """Test crawl command help."""
        result = cli_runner.invoke(app, ["crawl", "--help"])
        
        assert result.exit_code == 0
        assert "--state" in result.output
        assert "--all-states" in result.output
        assert "--out" in result.output
        assert "--dry-run" in result.output
    
    @pytest.mark.integration
    def test_export_help(self, cli_runner):
        """Test export command help."""
        result = cli_runner.invoke(app, ["export", "--help"])
        
        assert result.exit_code == 0
        assert "--out" in result.output
        assert "--limit" in result.output