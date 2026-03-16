#!/usr/bin/env python3
"""
Process USGS 3-Layer Geologic Model shapefile into GeoJSON for web map.
Creates simplified versions at different resolutions for performance.
"""

import json
import shapefile
from pyproj import Transformer
import sys
import os

# Input shapefile path
SHAPEFILE_PATH = "/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/3LYRModel_Shapefiles/ModelCells.shp"

# Output paths
OUTPUT_DIR = "/Users/devanagrawal/Desktop/project-seer/map/web/public/data"
OUTPUT_FULL = os.path.join(OUTPUT_DIR, "geology_full.geojson")
OUTPUT_SIMPLIFIED = os.path.join(OUTPUT_DIR, "geology_simplified.geojson")

# Note: The shapefile field names end in '_e' (elev) and '_al' (altitude)
# According to the XML metadata, these are already in METERS (LandSurf_elev_m, topBsmt_alt_m)
# No conversion needed - values are elevations in meters relative to sea level

# BSMT_TYPE descriptions (based on USGS documentation)
BSMT_TYPE_MAP = {
    "BSMT1": "Felsic igneous/metamorphic",
    "BSMT2": "Intermediate igneous/metamorphic", 
    "BSMT3": "Mafic igneous/metamorphic",
    "BSMT4": "Mixed/undifferentiated crystalline",
    "": "Unknown"
}

# BEDRX method descriptions
BEDRX_TYPE_MAP = {
    "BEDRX3": "Consolidated sedimentary",
    "BEDRX4": "Crystalline (igneous/metamorphic)",
    "BEDRX5": "Mixed sedimentary/crystalline",
    "BEDRX6": "Volcanic",
    "": "Unknown"
}

def get_qualitative_label(depth_to_basement_m, basement_type):
    """
    Determine qualitative favorability for CO₂-EGS targeting hot dry rock (basement).
    Shallower basement = more favorable (less drilling to reach HDR).
    Mafic basement = harder drilling, higher seismicity risk.
    """
    is_mafic = "mafic" in basement_type.lower() if basement_type else False
    
    # For HDR/EGS: we want shallow basement (less overburden to drill through)
    # But not TOO shallow (need some depth for temperature)
    # Typical drilling depths: 3-6km is feasible, >8km is very expensive
    if is_mafic:
        return "Unfavorable"  # Mafic = harder drilling, seismicity risk
    elif depth_to_basement_m < 3000:
        return "Favorable"    # Shallow basement - easy to reach HDR
    elif depth_to_basement_m < 6000:
        return "Moderate"     # Moderate depth - feasible drilling
    else:
        return "Unfavorable"  # Deep basement - expensive drilling

def process_shapefile(sample_rate=1, max_records=None):
    """
    Process the shapefile and return GeoJSON features.
    
    Args:
        sample_rate: Process every Nth record (1 = all, 4 = every 4th, etc.)
        max_records: Maximum number of records to process (None = all)
    """
    print(f"Reading shapefile: {SHAPEFILE_PATH}")
    sf = shapefile.Reader(SHAPEFILE_PATH)
    
    # Set up coordinate transformation from Albers to WGS84
    transformer = Transformer.from_crs("EPSG:5070", "EPSG:4326", always_xy=True)
    
    total_records = len(sf)
    print(f"Total records in shapefile: {total_records}")
    
    features = []
    processed = 0
    skipped_no_data = 0
    
    for i in range(0, total_records, sample_rate):
        if max_records and processed >= max_records:
            break
            
        try:
            rec = sf.record(i)
            shape = sf.shape(i)
            
            # Get raw values (in METERS, as elevations relative to sea level)
            # Field names: LandSurf_e = land surface elevation, topBsmt_al = top basement altitude
            land_surf_m = rec['LandSurf_e'] or 0
            top_bedrx_m = rec['topBedrx_a'] or 0
            top_bsmt_m = rec['topBsmt_al'] or 0
            bsmt_type_code = rec['BSMT_TYPE'] or ""
            bedrx_type_code = rec['BEDRX_Meth'] or ""
            
            # Skip cells with no meaningful data
            if land_surf_m == 0 and top_bedrx_m == 0 and top_bsmt_m == 0:
                skipped_no_data += 1
                continue
            
            # Calculate derived fields (values already in meters)
            # Sediment thickness: land surface to top of bedrock
            sediment_thickness_m = round(land_surf_m - top_bedrx_m)
            
            # Sedimentary rock thickness: top of bedrock to top of basement
            sedimentary_thickness_m = round(top_bedrx_m - top_bsmt_m)
            
            # Depth to basement: land surface to top of basement
            depth_to_basement_m = round(land_surf_m - top_bsmt_m)
            
            # Get descriptive types
            basement_type = BSMT_TYPE_MAP.get(bsmt_type_code, bsmt_type_code)
            bedrock_type = BEDRX_TYPE_MAP.get(bedrx_type_code, bedrx_type_code)
            
            # Calculate qualitative label based on depth to basement (for HDR targeting)
            qualitative = get_qualitative_label(depth_to_basement_m, basement_type)
            
            # Transform coordinates from Albers to WGS84
            # Handle POLYGON (5) and POLYGONZ (15) shape types
            if shape.shapeType in [shapefile.POLYGON, 15]:  # 15 = POLYGONZ
                points = shape.points
                # Transform center point only and create a simple square
                # This reduces file size significantly
                cx = (points[0][0] + points[2][0]) / 2
                cy = (points[0][1] + points[2][1]) / 2
                lon, lat = transformer.transform(cx, cy)
                
                # Create a simple point geometry (much smaller than polygons)
                geometry = {
                    "type": "Point",
                    "coordinates": [round(lon, 3), round(lat, 3)]
                }
            else:
                continue
            
            # Create feature with minimal properties to reduce file size
            feature = {
                "type": "Feature",
                "properties": {
                    "st": sedimentary_thickness_m,  # sedimentary thickness
                    "dt": depth_to_basement_m,      # depth to basement
                    "q": qualitative[0]             # F/M/U for Favorable/Moderate/Unfavorable
                },
                "geometry": geometry
            }
            
            features.append(feature)
            processed += 1
            
            if processed % 10000 == 0:
                print(f"  Processed {processed} features...")
                
        except Exception as e:
            print(f"  Error processing record {i}: {e}")
            continue
    
    print(f"Processed {processed} features, skipped {skipped_no_data} with no data")
    return features

def save_geojson(features, output_path):
    """Save features as GeoJSON file."""
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(geojson, f)
    
    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Saved {len(features)} features to {output_path} ({file_size:.1f} MB)")

def main():
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Process simplified version (every 12th cell = ~30km effective resolution)
    # Good balance between coverage and file size for web
    print("\n=== Creating simplified version (every 12th cell) ===")
    simplified_features = process_shapefile(sample_rate=12)
    save_geojson(simplified_features, OUTPUT_SIMPLIFIED)
    
    print("\nDone! Simplified geology data ready for web map.")
    print(f"Output: {OUTPUT_SIMPLIFIED}")

if __name__ == "__main__":
    main()
