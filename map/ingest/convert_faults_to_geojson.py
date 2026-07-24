#!/usr/bin/env python3
"""
Convert USGS Quaternary Faults shapefile to simplified GeoJSON for web display.
"""

import geopandas as gpd
import json
import os

# Paths
SHAPEFILE_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/temp/SHP/Qfaults_US_Database.shp'
OUTPUT_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/quaternary_faults.geojson'

# Geometry simplification tolerance (degrees, ~0.001 = ~100m at equator)
SIMPLIFY_TOLERANCE = 0.001


def get_slip_rate_class(rate):
    """Convert slip rate text to numeric class for styling."""
    if rate is None or rate == 'Unspecified' or rate == 'Insufficient data':
        return 0
    if 'Greater than 5' in str(rate):
        return 4  # Highest risk
    if 'Between 1.0 and 5.0' in str(rate):
        return 3
    if 'Between 0.2 and 1.0' in str(rate):
        return 2
    if 'Less than 0.2' in str(rate) or '0.2 +/-' in str(rate):
        return 1  # Lowest risk
    return 0


def main():
    print("Reading shapefile...")
    gdf = gpd.read_file(SHAPEFILE_PATH)
    print(f"  Loaded {len(gdf)} features")

    print("Simplifying geometry...")
    gdf['geometry'] = gdf['geometry'].simplify(tolerance=SIMPLIFY_TOLERANCE, preserve_topology=True)

    print("Adding slip_rate_class...")
    gdf['slip_rate_class'] = gdf['slip_rate'].apply(get_slip_rate_class)

    # Keep only needed columns
    cols_to_keep = ['fault_name', 'age', 'slip_rate', 'slip_rate_class', 'slip_sense', 'geometry']
    gdf_slim = gdf[cols_to_keep].copy()

    print("Converting to GeoJSON...")
    geojson = json.loads(gdf_slim.to_json())

    # Add metadata
    geojson['metadata'] = {
        'total_faults': len(gdf_slim),
        'source': 'USGS Quaternary Fault and Fold Database',
        'slip_rate_classes': {
            0: 'Unspecified',
            1: 'Less than 0.2 mm/yr',
            2: '0.2-1.0 mm/yr',
            3: '1.0-5.0 mm/yr',
            4: 'Greater than 5.0 mm/yr'
        }
    }

    print(f"Saving to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(geojson, f)

    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"Done! File size: {size_mb:.2f} MB, {len(geojson['features'])} features")


if __name__ == '__main__':
    main()
