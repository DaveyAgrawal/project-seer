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
from scipy.spatial import cKDTree
import os

# Configuration
CSV_FILE = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv'
OUTPUT_DIR = '/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal'
DEPTHS_TO_GENERATE = [1000, 2000, 3000, 4000, 5000, 6000, 7000]  # meters (STM native 1000m steps)

# Hexagon grid configuration. HEX_SIZE tuned so the hexagon count (~50k)
# matches the previously-deployed Turf 20-mile grid for a drop-in replacement.
HEX_SIZE = 0.11  # degrees (~7 miles)
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

def load_all_depths(depths):
    """Single pass over the (very large) CSV, returning {depth: DataFrame[lat,lng,temp_f]}.

    Reading the 5.7 GB CSV once and splitting by depth is far cheaper than
    scanning it once per depth.
    """
    print(f"Loading temperature data for depths {depths} (single CSV pass)...")
    depth_set = set(depths)
    parts = {d: [] for d in depths}

    for chunk in tqdm(pd.read_csv(CSV_FILE,
                                  usecols=['Northing', 'Easting', 'Depth', 'T'],
                                  chunksize=200000),
                      desc="Reading CSV"):
        sub = chunk[chunk['Depth'].isin(depth_set)]
        for d, grp in sub.groupby('Depth'):
            parts[int(d)].append(grp)

    result = {}
    for d in depths:
        if not parts[d]:
            print(f"No data found for depth {d}m")
            result[d] = None
            continue
        df = pd.concat(parts[d], ignore_index=True)
        # Vectorized Web Mercator -> WGS84 conversion
        lng = (df['Easting'].to_numpy() / 20037508.34) * 180.0
        lat_merc = (df['Northing'].to_numpy() / 20037508.34) * 180.0
        lat = 180.0 / np.pi * (2.0 * np.arctan(np.exp(lat_merc * np.pi / 180.0)) - np.pi / 2.0)
        out = pd.DataFrame({
            'lat': lat,
            'lng': lng,
            'temp_f': df['T'].to_numpy() * 9.0 / 5.0 + 32.0,
        })
        out = out.dropna(subset=['lat', 'lng', 'temp_f'])
        print(f"Found {len(out)} points at depth {d}m")
        result[d] = out
    return result

# Latitude compression factor used when laying out hexagon vertices. The same
# factor must be applied when measuring distances so the KD-tree "circle"
# matches the hexagon's actual footprint.
LAT_SCALE = 0.866


def build_hex_tree(hexagons):
    """Build a KD-tree over hexagon centers (in a lat-compressed plane)."""
    centers = np.array([[h['center_lng'], h['center_lat'] * LAT_SCALE] for h in hexagons])
    return cKDTree(centers), centers


def assign_points_to_hexagons(hexagons, points_df, tree):
    """Assign each temperature point to its NEAREST hexagon (Voronoi assignment).

    The previous implementation matched points to hexagons by independently
    rounding both to a coarse grid and requiring the keys to be equal. Because
    the hexagon grid is staggered (every other row is offset by half a step)
    while the point-binning grid is anchored at 0, whole columns of hexagons
    never coincided with a populated bin -> periodic vertical blank bands.

    Nearest-neighbor assignment removes the aliasing entirely: every point
    contributes to exactly one hexagon, and every hexagon that has any point
    within its footprint is filled. Only hexagons with genuinely no source
    coverage (true edges/gaps) remain null.
    """
    print("Assigning points to hexagons (nearest-neighbor)...")

    pts = np.column_stack([
        points_df['lng'].to_numpy(),
        points_df['lat'].to_numpy() * LAT_SCALE,
    ])

    # Max distance a point can be from a hex center and still belong to it.
    # Circumradius of a flat-topped hexagon of "size" HEX_SIZE is HEX_SIZE; add
    # a small margin so points near vertices are still captured.
    max_dist = HEX_SIZE * 1.05

    dist, idx = tree.query(pts, k=1, distance_upper_bound=max_dist)
    valid = np.isfinite(dist)
    vidx = idx[valid]
    vtemp = points_df['temp_f'].to_numpy()[valid]

    n_hex = len(hexagons)
    sums = np.zeros(n_hex)
    counts = np.zeros(n_hex, dtype=np.int64)
    mins = np.full(n_hex, np.inf)
    maxs = np.full(n_hex, -np.inf)
    np.add.at(sums, vidx, vtemp)
    np.add.at(counts, vidx, 1)
    np.minimum.at(mins, vidx, vtemp)
    np.maximum.at(maxs, vidx, vtemp)

    features = []
    assigned_count = 0
    for i, hex_data in enumerate(hexagons):
        if counts[i] > 0:
            avg_temp = round(float(sums[i] / counts[i]), 1)
            min_temp = round(float(mins[i]), 1)
            max_temp = round(float(maxs[i]), 1)
            point_count = int(counts[i])
            assigned_count += 1
        else:
            avg_temp = None
            min_temp = None
            max_temp = None
            point_count = 0

        features.append({
            "type": "Feature",
            "properties": {
                "avg_temperature_f": avg_temp,
                "min_temperature_f": min_temp,
                "max_temperature_f": max_temp,
                "point_count": point_count,
                "hex_id": f"hex_{hex_data['id']}"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [hex_data['coords']]
            },
            "id": hex_data['id']
        })

    print(f"Assigned temperature data to {assigned_count}/{len(hexagons)} hexagons")
    return features

def generate_mesh_for_depth(depth, hexagons, tree, points_df):
    """Generate mesh file for a specific depth"""
    print(f"\n{'='*60}")
    print(f"Generating mesh for {depth}m depth")
    print(f"{'='*60}")

    if points_df is None or len(points_df) == 0:
        print(f"Skipping {depth}m - no data")
        return

    # Assign to hexagons
    features = assign_points_to_hexagons(hexagons, points_df, tree)
    
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

    # Build the KD-tree of hexagon centers once (reused for every depth)
    print("🌳 Building hexagon KD-tree...")
    tree, _ = build_hex_tree(hexagons)

    # Read the large CSV exactly once, splitting rows across all target depths
    depth_data = load_all_depths(DEPTHS_TO_GENERATE)

    # Generate mesh for each depth
    for depth in DEPTHS_TO_GENERATE:
        generate_mesh_for_depth(depth, hexagons, tree, depth_data.get(depth))
    
    print("\n" + "=" * 60)
    print("✅ All mesh files generated!")
    print("=" * 60)

if __name__ == "__main__":
    main()
