"""
Main CLI application entry point for DataCenterMap scraper.
"""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from src.core.config import config
from src.core.database import db_manager
from src.core.logging import logger

console = Console()
app = typer.Typer(
    name="dcm",
    help="DataCenterMap scraper CLI",
    add_completion=False,
)


@app.command()
def init_db():
    """Initialize database tables."""
    
    async def _init_db():
        try:
            config.validate()
            logger.info("Creating database tables...")
            await db_manager.create_tables()
            logger.info("✅ Database tables created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create database tables: {e}")
            raise typer.Exit(1)
    
    asyncio.run(_init_db())


@app.command()
def crawl(
    state: Optional[str] = typer.Option(None, "--state", help="State to crawl (e.g., 'delaware')"),
    all_states: bool = typer.Option(False, "--all-states", help="Crawl all U.S. states"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output CSV file path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be crawled without executing")
):
    """Crawl datacentermap.com for facility data."""
    
    if not state and not all_states:
        console.print("❌ Must specify either --state or --all-states", style="red")
        raise typer.Exit(1)
    
    if state and all_states:
        console.print("❌ Cannot specify both --state and --all-states", style="red")
        raise typer.Exit(1)
    
    async def _crawl():
        try:
            config.validate()
            
            # Import crawler here to avoid circular imports
            from src.dcm.crawler import DataCenterMapCrawler
            from src.dcm.exporter import FacilityExporter
            
            if dry_run:
                if state:
                    console.print(f"[DRY RUN] Would crawl state: {state}")
                else:
                    console.print("[DRY RUN] Would crawl all U.S. states")
                return
            
            # Initialize crawler
            crawler = DataCenterMapCrawler()
            
            # Execute crawl
            if state:
                logger.info(f"🚀 Starting crawl for state: {state}")
                facilities = await crawler.crawl_state(state)
            else:
                logger.info("🚀 Starting crawl for all U.S. states")
                facilities = await crawler.crawl_all_states()
            
            if not facilities:
                console.print("⚠️  No facilities found", style="yellow")
                return
            
            # Export to CSV if requested
            if out:
                success = FacilityExporter.export_to_csv(facilities, out)
                if success:
                    console.print(f"✅ Exported {len(facilities)} facilities to {out}", style="green")
                else:
                    console.print("❌ Export failed", style="red")
                    raise typer.Exit(1)
            
            # Generate summary
            FacilityExporter.export_summary(facilities)
            
            console.print(f"🎉 Crawl completed successfully! Found {len(facilities)} facilities", style="green")
                
        except Exception as e:
            logger.error(f"❌ Crawl failed: {e}")
            raise typer.Exit(1)
    
    asyncio.run(_crawl())


@app.command()
def export(
    out: Path = typer.Option(..., "--out", help="Output CSV file path"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of records")
):
    """Export facility data from database to CSV."""
    
    async def _export():
        try:
            config.validate()
            
            # Import here to avoid circular imports
            from src.dcm.exporter import FacilityExporter
            from src.core.models import DataCenterFacility
            from sqlalchemy import select
            
            logger.info(f"📤 Exporting data to {out}")
            
            # Query database for facilities
            async with db_manager.session() as session:
                query = select(DataCenterFacility)
                if limit:
                    query = query.limit(limit)
                
                result = await session.execute(query)
                facilities_db = result.scalars().all()
            
            if not facilities_db:
                console.print("⚠️  No facilities found in database", style="yellow")
                return
            
            # Convert to dictionaries
            facilities = [facility.to_dict() for facility in facilities_db]
            
            # Export to CSV
            success = FacilityExporter.export_to_csv(facilities, out)
            if success:
                console.print(f"✅ Exported {len(facilities)} facilities to {out}", style="green")
                
                # Generate summary
                FacilityExporter.export_summary(facilities)
            else:
                console.print("❌ Export failed", style="red")
                raise typer.Exit(1)
            
        except Exception as e:
            logger.error(f"❌ Export failed: {e}")
            raise typer.Exit(1)
    
    asyncio.run(_export())


def main():
    """Entry point for the CLI application."""
    app()