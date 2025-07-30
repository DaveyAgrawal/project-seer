"""
CSV export functionality for facility data.
"""

import csv
from pathlib import Path
from typing import List, Dict, Optional

from src.core.logging import logger


class FacilityExporter:
    """Handles exporting facility data to various formats."""
    
    @staticmethod
    def export_to_csv(facilities: List[Dict], output_path: Path, include_headers: bool = True) -> bool:
        """
        Export facilities to CSV format.
        
        Args:
            facilities: List of facility dictionaries
            output_path: Path to output CSV file
            include_headers: Whether to include column headers
            
        Returns:
            True if export successful, False otherwise
        """
        if not facilities:
            logger.warning("No facilities to export")
            return False
        
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Define CSV columns in logical order
            columns = [
                "id",
                "name",
                "address_full",
                "city", 
                "state",
                "postal_code",
                "country",
                "latitude",
                "longitude",
                "source",
                "source_url",
                "first_seen_at",
                "last_seen_at"
            ]
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=columns, extrasaction='ignore')
                
                if include_headers:
                    writer.writeheader()
                
                for facility in facilities:
                    # Convert datetime objects to ISO strings for CSV
                    row = facility.copy()
                    for date_field in ['first_seen_at', 'last_seen_at']:
                        if date_field in row and row[date_field]:
                            if hasattr(row[date_field], 'isoformat'):
                                row[date_field] = row[date_field].isoformat()
                    
                    writer.writerow(row)
            
            logger.info(f"✅ Exported {len(facilities)} facilities to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to export to CSV: {e}")
            return False
    
    @staticmethod
    def export_summary(facilities: List[Dict], output_path: Optional[Path] = None) -> Dict[str, int]:
        """
        Generate and optionally export a summary of the facility data.
        
        Args:
            facilities: List of facility dictionaries
            output_path: Optional path to save summary text file
            
        Returns:
            Dictionary with summary statistics
        """
        if not facilities:
            return {}
        
        # Calculate statistics
        summary = {
            "total_facilities": len(facilities),
            "facilities_with_coordinates": sum(1 for f in facilities 
                                             if f.get("latitude") and f.get("longitude")),
            "facilities_with_full_address": sum(1 for f in facilities 
                                              if f.get("address_full")),
            "unique_states": len(set(f.get("state") for f in facilities if f.get("state"))),
            "unique_cities": len(set(f"{f.get('city')}, {f.get('state')}" 
                                   for f in facilities 
                                   if f.get("city") and f.get("state"))),
        }
        
        # State breakdown
        state_counts = {}
        for facility in facilities:
            state = facility.get("state", "Unknown")
            state_counts[state] = state_counts.get(state, 0) + 1
        
        summary["state_breakdown"] = dict(sorted(state_counts.items()))
        
        # Coordinate success rate
        if summary["total_facilities"] > 0:
            coord_rate = (summary["facilities_with_coordinates"] / summary["total_facilities"]) * 100
            summary["coordinate_success_rate"] = round(coord_rate, 1)
        
        # Log summary
        logger.info("📊 Export Summary:")
        logger.info(f"   Total facilities: {summary['total_facilities']}")
        logger.info(f"   With coordinates: {summary['facilities_with_coordinates']}")
        logger.info(f"   With full address: {summary['facilities_with_full_address']}")
        logger.info(f"   Unique states: {summary['unique_states']}")
        logger.info(f"   Unique cities: {summary['unique_cities']}")
        
        if "coordinate_success_rate" in summary:
            logger.info(f"   Coordinate success rate: {summary['coordinate_success_rate']}%")
        
        # Export to file if requested
        if output_path:
            try:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write("DataCenterMap Scraper - Export Summary\n")
                    f.write("=" * 45 + "\n\n")
                    f.write(f"Total facilities: {summary['total_facilities']}\n")
                    f.write(f"Facilities with coordinates: {summary['facilities_with_coordinates']}\n")
                    f.write(f"Facilities with full address: {summary['facilities_with_full_address']}\n")
                    f.write(f"Unique states: {summary['unique_states']}\n")
                    f.write(f"Unique cities: {summary['unique_cities']}\n")
                    
                    if "coordinate_success_rate" in summary:
                        f.write(f"Coordinate success rate: {summary['coordinate_success_rate']}%\n")
                    
                    f.write("\nState Breakdown:\n")
                    f.write("-" * 20 + "\n")
                    for state, count in summary["state_breakdown"].items():
                        f.write(f"{state}: {count}\n")
                
                logger.info(f"✅ Summary exported to {output_path}")
                
            except Exception as e:
                logger.error(f"❌ Failed to export summary: {e}")
        
        return summary