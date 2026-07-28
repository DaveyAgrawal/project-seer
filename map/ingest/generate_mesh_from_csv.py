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
from pyproj import Transformer
import os

# Configuration
CSV_FILE = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv'
OUTPUT_DIR = '/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal'
DEPTHS_TO_GENERATE = [1000, 2000, 3000, 4000, 5000, 6000, 7000]  # meters (STM native 1000m steps)

# Hexagons are generated in an equal-area projection (EPSG:5070, NAD83 / Conus
# Albers) so every cell tiles PERFECTLY (adjacent cells share exact vertices --
# no overlaps, no gaps) and has an IDENTICAL, exact area. This is what makes
# downstream area / GW calculations valid. Vertices are converted to WGS84 only
# for storage/rendering; because a projection is a point map, shared vertices
# stay shared after conversion, so the tiling remains perfect in lon/lat too.
PROJ_CRS = "EPSG:5070"   # NAD83 / Conus Albers (meters, equal-area)
WGS84 = "EPSG:4326"

# Cell size: 5-mile circumradius -> regular-hexagon area ~= 65 sq miles, matching
# the area constant the web app uses for potential/GW estimates.
MILES_TO_M = 1609.344
CIRCUMRADIUS_MI = 5.0
CIRCUMRADIUS_M = CIRCUMRADIUS_MI * MILES_TO_M
HEX_AREA_SQMI = (3.0 * np.sqrt(3.0) / 2.0) * (CIRCUMRADIUS_MI ** 2)   # ~64.95
HEX_AREA_KM2 = HEX_AREA_SQMI * 2.5899881103

US_BOUNDS = {
    'west': -125,
    'east': -66,
    'south': 24,
    'north': 50
}

# Reusable coordinate transformers (always_xy => lon/lat order).
_TO_PROJ = Transformer.from_crs(WGS84, PROJ_CRS, always_xy=True)
_TO_WGS = Transformer.from_crs(PROJ_CRS, WGS84, always_xy=True)

def web_mercator_to_wgs84(x, y):
    """Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)"""
    lng = (x / 20037508.34) * 180
    lat = (y / 20037508.34) * 180
    lat = 180 / np.pi * (2 * np.arctan(np.exp(lat * np.pi / 180)) - np.pi / 2)
    return lat, lng

def generate_hex_grid():
    """Generate a perfectly-tiling pointy-topped hexagon grid.

    The grid is regular in EPSG:5070 meters (equal-area), so adjacent cells share
    exact vertices -> no overlaps, no gaps, and identical area everywhere. Cell
    corners/centers are then transformed to WGS84 lon/lat for storage.
    """
    R = CIRCUMRADIUS_M
    col_step = np.sqrt(3.0) * R      # horizontal spacing (pointy-topped)
    row_step = 1.5 * R               # vertical spacing

    # Projected bounding box covering the US_BOUNDS lon/lat rectangle. Albers is
    # curved, so sample the rectangle edges and pad by one cell.
    edge_lng = np.linspace(US_BOUNDS['west'], US_BOUNDS['east'], 50)
    edge_lat = np.linspace(US_BOUNDS['south'], US_BOUNDS['north'], 50)
    bl = np.concatenate([edge_lng, edge_lng,
                         np.full(50, US_BOUNDS['west']), np.full(50, US_BOUNDS['east'])])
    ba = np.concatenate([np.full(50, US_BOUNDS['south']), np.full(50, US_BOUNDS['north']),
                         edge_lat, edge_lat])
    bx, by = _TO_PROJ.transform(bl, ba)
    minx, maxx = bx.min() - R, bx.max() + R
    miny, maxy = by.min() - R, by.max() + R

    # Build the array of hexagon centers (projected meters).
    cxs, cys = [], []
    row = 0
    y = miny
    while y <= maxy:
        x_offset = (col_step / 2.0) if (row % 2 == 1) else 0.0
        x = minx + x_offset
        while x <= maxx:
            cxs.append(x)
            cys.append(y)
            x += col_step
        y += row_step
        row += 1
    cx = np.array(cxs)
    cy = np.array(cys)

    # Vertices for every hexagon, transformed to lon/lat in a single batch call.
    angles = np.radians([30, 90, 150, 210, 270, 330])
    vx = (cx[:, None] + R * np.cos(angles)[None, :]).ravel()
    vy = (cy[:, None] + R * np.sin(angles)[None, :]).ravel()
    vlng, vlat = _TO_WGS.transform(vx, vy)
    vlng = vlng.reshape(-1, 6)
    vlat = vlat.reshape(-1, 6)
    clng, clat = _TO_WGS.transform(cx, cy)

    hexagons = []
    for i in range(len(cx)):
        coords = [[float(vlng[i, k]), float(vlat[i, k])] for k in range(6)]
        coords.append(coords[0])   # close ring
        hexagons.append({
            'id': i,
            'cx': float(cx[i]),
            'cy': float(cy[i]),
            'center_lng': float(clng[i]),
            'center_lat': float(clat[i]),
            'coords': coords,
        })

    print(f"Generated {len(hexagons)} hexagons "
          f"({CIRCUMRADIUS_MI}-mile circumradius, {HEX_AREA_SQMI:.1f} sq mi each)")
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

def build_hex_tree(hexagons):
    """Build a KD-tree over hexagon centers in projected meters (isotropic)."""
    centers = np.array([[h['cx'], h['cy']] for h in hexagons])
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

    px, py = _TO_PROJ.transform(points_df['lng'].to_numpy(), points_df['lat'].to_numpy())
    pts = np.column_stack([px, py])

    # Max distance a point can be from a hex center and still belong to it. In a
    # regular hex tiling every point is within one circumradius of the nearest
    # center, so a tiny margin over CIRCUMRADIUS_M captures everything exactly.
    max_dist = CIRCUMRADIUS_M * 1.05

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

    # Only hexagons that actually received STM points are emitted. Because every
    # cell is identical and non-overlapping, dropping empty (ocean / off-domain)
    # cells keeps files small without affecting area math or rendering.
    area_sqmi = round(HEX_AREA_SQMI, 3)
    area_km2 = round(HEX_AREA_KM2, 3)
    features = []
    for i, hex_data in enumerate(hexagons):
        if counts[i] == 0:
            continue
        features.append({
            "type": "Feature",
            "properties": {
                "avg_temperature_f": round(float(sums[i] / counts[i]), 1),
                "min_temperature_f": round(float(mins[i]), 1),
                "max_temperature_f": round(float(maxs[i]), 1),
                "point_count": int(counts[i]),
                "area_sq_miles": area_sqmi,
                "area_km2": area_km2,
                "hex_id": f"hex_{hex_data['id']}"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [hex_data['coords']]
            },
            "id": hex_data['id']
        })

    print(f"Assigned temperature data to {len(features)}/{len(hexagons)} hexagons")
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
