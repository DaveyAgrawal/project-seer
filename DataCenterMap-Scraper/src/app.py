"""
Main CLI application entry point for DataCenterMap scraper.
"""

import typer

app = typer.Typer(
    name="dcm",
    help="DataCenterMap scraper CLI",
    add_completion=False,
)

def main():
    """Entry point for the CLI application."""
    app()