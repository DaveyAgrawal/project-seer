#!/usr/bin/env python3
"""
Generate geothermal mesh cache files directly from the Stanford CSV,
bypassing the PostgreSQL database entirely.

This creates the mesh-{depth}m.json files that the web app uses.
"""

import pandas as pd
import numpy as np
import json
from tqdm import tqdm
import os

# Configuration
CSV_FILE = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv'
OUTPUT_DIR = '/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal'
DEPTHS_TO_GENERATE = [2000, 3000, 4000, 4500, 5000, 6000, 7000]  # meters

# Hexagon grid configuration (matching the existing mesh)
HEX_SIZE = 0.125  # degrees (approximately 10-15 km)
US_BOUNDS = {
    'west': -125,
    'east': -66,
    'south': 24,
    'north': 50
}

def web_mercator_to_wgs84(x, y):
    """Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)"""
    lng = (x / 20037508.34) * 180
    lat = (y / 20037508.34) * 180
    lat = 180 / np.pi * (2 * np.arctan(np.exp(lat * np.pi / 180)) - np.pi / 2)
    return lat, lng

def create_hexagon(center_lng, center_lat, size):
    """Create a hexagon polygon around a center point"""
    # Flat-topped hexagon vertices
    angles = [0, 60, 120, 180, 240, 300, 0]  # Close the polygon
    coords = []
    for angle in angles:
        rad = np.radians(angle)
        lng = center_lng + size * np.cos(rad)
        lat = center_lat + size * np.sin(rad) * 0.866  # Adjust for aspect ratio
        coords.append([lng, lat])
    return coords

def generate_hex_grid():
    """Generate a hexagon grid covering the US"""
    hexagons = []
    hex_id = 0
    
    # Calculate grid spacing
    lng_step = HEX_SIZE * 1.5
    lat_step = HEX_SIZE * 0.866 * 2
    
    lat = US_BOUNDS['south']
    row = 0
    while lat < US_BOUNDS['north']:
        lng_offset = (HEX_SIZE * 0.75) if (row % 2 == 1) else 0
        lng = US_BOUNDS['west'] + lng_offset
        
        while lng < US_BOUNDS['east']:
            hexagons.append({
                'id': hex_id,
                'center_lng': lng,
                'center_lat': lat,
                'coords': create_hexagon(lng, lat, HEX_SIZE)
            })
            hex_id += 1
            lng += lng_step
        
        lat += lat_step
        row += 1
    
    print(f"Generated {len(hexagons)} hexagons")
    return hexagons

def load_temperature_data(depth):
    """Load temperature data for a specific depth from CSV"""
    print(f"Loading temperature data for depth {depth}m...")
    
    # Read only the columns we need, filtering by depth
    chunks = []
    for chunk in tqdm(pd.read_csv(CSV_FILE, 
                                   usecols=['Northing', 'Easting', 'Depth', 'T'],
                                   chunksize=100000),
                      desc=f"Reading CSV for {depth}m"):
        # Filter to this depth
        depth_data = chunk[chunk['Depth'] == depth].copy()
        if len(depth_data) > 0:
            chunks.append(depth_data)
    
    if not chunks:
        print(f"No data found for depth {depth}m")
        return None
    
    df = pd.concat(chunks, ignore_index=True)
    print(f"Found {len(df)} points at depth {depth}m")
    
    # Convert coordinates from Web Mercator to WGS84
    print("Converting coordinates...")
    coords = df.apply(lambda row: web_mercator_to_wgs84(row['Easting'], row['Northing']), axis=1)
    df['lat'] = [c[0] for c in coords]
    df['lng'] = [c[1] for c in coords]
    
    # Convert temperature from Celsius to Fahrenheit
    df['temp_f'] = df['T'] * 9/5 + 32
    
    return df[['lat', 'lng', 'temp_f']]

def assign_points_to_hexagons(hexagons, points_df):
    """Assign temperature points to hexagons and calculate averages"""
    print("Assigning points to hexagons...")
    
    # Create a spatial index using binning
    # Round coordinates to hex grid cells
    points_df = points_df.copy()
    points_df['hex_lng'] = np.round(points_df['lng'] / (HEX_SIZE * 1.5)) * (HEX_SIZE * 1.5)
    points_df['hex_lat'] = np.round(points_df['lat'] / (HEX_SIZE * 0.866 * 2)) * (HEX_SIZE * 0.866 * 2)
    
    # Group by hex cell and calculate mean temperature
    grouped = points_df.groupby(['hex_lng', 'hex_lat']).agg({
        'temp_f': ['mean', 'count']
    }).reset_index()
    grouped.columns = ['hex_lng', 'hex_lat', 'avg_temp_f', 'point_count']
    
    # Create lookup dict
    temp_lookup = {}
    for _, row in grouped.iterrows():
        key = (round(row['hex_lng'], 3), round(row['hex_lat'], 3))
        temp_lookup[key] = (row['avg_temp_f'], int(row['point_count']))
    
    print(f"Created {len(temp_lookup)} temperature cells")
    
    # Assign to hexagons
    features = []
    assigned_count = 0
    
    for hex_data in tqdm(hexagons, desc="Building hexagon features"):
        key = (round(hex_data['center_lng'] / (HEX_SIZE * 1.5)) * (HEX_SIZE * 1.5),
               round(hex_data['center_lat'] / (HEX_SIZE * 0.866 * 2)) * (HEX_SIZE * 0.866 * 2))
        key = (round(key[0], 3), round(key[1], 3))
        
        if key in temp_lookup:
            avg_temp, point_count = temp_lookup[key]
            assigned_count += 1
        else:
            avg_temp = None
            point_count = 0
        
        feature = {
            "type": "Feature",
            "properties": {
                "avg_temperature_f": avg_temp,
                "point_count": point_count,
                "hex_id": f"hex_{hex_data['id']}"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [hex_data['coords']]
            },
            "id": hex_data['id']
        }
        features.append(feature)
    
    print(f"Assigned temperature data to {assigned_count}/{len(hexagons)} hexagons")
    return features

def generate_mesh_for_depth(depth, hexagons):
    """Generate mesh file for a specific depth"""
    print(f"\n{'='*60}")
    print(f"Generating mesh for {depth}m depth")
    print(f"{'='*60}")
    
    # Load temperature data
    points_df = load_temperature_data(depth)
    if points_df is None:
        print(f"Skipping {depth}m - no data")
        return
    
    # Assign to hexagons
    features = assign_points_to_hexagons(hexagons, points_df)
    
    # Create GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    # Save to file
    output_file = os.path.join(OUTPUT_DIR, f"mesh-{depth}m.json")
    print(f"Saving to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(geojson, f)
    
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    print(f"✅ Saved {output_file} ({file_size:.1f} MB)")

def main():
    print("🌡️ Stanford Thermal Model Mesh Generator")
    print("=" * 60)
    
    # Check if CSV exists
    if not os.path.exists(CSV_FILE):
        print(f"❌ CSV file not found: {CSV_FILE}")
        return
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate hexagon grid once
    print("\n📐 Generating hexagon grid...")
    hexagons = generate_hex_grid()
    
    # Generate mesh for each depth
    for depth in DEPTHS_TO_GENERATE:
        generate_mesh_for_depth(depth, hexagons)
    
    print("\n" + "=" * 60)
    print("✅ All mesh files generated!")
    print("=" * 60)

if __name__ == "__main__":
    main()
