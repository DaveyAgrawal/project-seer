#!/usr/bin/env python3
"""
Join water stress (SUI) data to SEER hex mesh using real HUC12 polygons.

This script:
1. Loads the WBD HUC12 polygons from the national geodatabase
2. Aggregates SUI time-series to mean per HUC12
3. Joins SUI to HUC12 polygons
4. For each hex in SEER's mesh, assigns the SUI of the containing HUC12
5. Updates the hex cache with sui and sui_class attributes

SUI Direction: Higher SUI = MORE water stress (worse for site selection)
- SUI 0.0 = Very low/none stress (good)
- SUI 1.0 = Severe stress (bad)
"""

import json
import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, shape
import numpy as np

# Paths
WBD_GDB_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/wbd/WBD_National_GDB.gdb'
WBD_ZIP_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/wbd/WBD_National.zip'
SUI_PARQUET_PATH = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/water_budget_sui_ensemble.parquet'
HEX_CACHE_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal'
OUTPUT_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/sui_by_hex.json'


def load_and_aggregate_sui():
    """Load SUI parquet and compute mean SUI per HUC12."""
    print("Loading SUI parquet data...")
    df = pd.read_parquet(SUI_PARQUET_PATH, columns=['huc', 'SUI', 'SUIclass'])
    
    print(f"  Total rows: {len(df):,}")
    print(f"  Unique HUC12s: {df['huc'].nunique():,}")
    
    # Aggregate to mean SUI per HUC12
    print("Aggregating to mean SUI per HUC12...")
    agg = df.groupby('huc').agg({
        'SUI': 'mean'
    }).reset_index()
    
    # Assign SUI class based on mean value
    def get_sui_class(sui):
        if sui < 0.1:
            return 'Very low/none'
        elif sui < 0.2:
            return 'Low'
        elif sui < 0.4:
            return 'Moderate'
        elif sui < 0.7:
            return 'High'
        else:
            return 'Severe'
    
    agg['sui_class'] = agg['SUI'].apply(get_sui_class)
    agg = agg.rename(columns={'SUI': 'sui'})
    
    # Ensure HUC12 is string with leading zeros
    agg['huc'] = agg['huc'].astype(str).str.zfill(12)
    
    print(f"  Aggregated to {len(agg):,} HUC12s")
    print(f"  SUI range: {agg['sui'].min():.3f} - {agg['sui'].max():.3f}")
    print(f"  SUI class distribution:")
    print(agg['sui_class'].value_counts())
    
    return agg.set_index('huc')


def load_huc12_polygons():
    """Load HUC12 polygons from WBD geodatabase."""
    print("\nLoading HUC12 polygons from WBD geodatabase...")
    
    # Check if GDB exists (need to unzip first)
    gdb_dir = WBD_GDB_PATH
    zip_path = gdb_dir.replace('.gdb', '.zip')
    
    if not os.path.exists(gdb_dir):
        if os.path.exists(zip_path):
            print(f"  Extracting {zip_path}...")
            import zipfile
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(os.path.dirname(gdb_dir))
        else:
            print(f"  ERROR: Neither {gdb_dir} nor {zip_path} found!")
            sys.exit(1)
    
    # List layers in GDB
    import fiona
    layers = fiona.listlayers(gdb_dir)
    print(f"  Available layers: {layers}")
    
    # Find HUC12 layer
    huc12_layer = None
    for layer in layers:
        if 'HUC12' in layer.upper() or 'WBDHU12' in layer.upper():
            huc12_layer = layer
            break
    
    if not huc12_layer:
        print(f"  ERROR: No HUC12 layer found in {layers}")
        sys.exit(1)
    
    print(f"  Loading layer: {huc12_layer}")
    gdf = gpd.read_file(gdb_dir, layer=huc12_layer)
    
    print(f"  Loaded {len(gdf):,} HUC12 polygons")
    print(f"  CRS: {gdf.crs}")
    print(f"  Columns: {gdf.columns.tolist()}")
    
    # Reproject to WGS84 if needed
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print("  Reprojecting to EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)
    
    # Find HUC12 ID column
    huc_col = None
    for col in ['huc12', 'HUC12', 'HUC_12']:
        if col in gdf.columns:
            huc_col = col
            break
    
    if not huc_col:
        print(f"  Available columns: {gdf.columns.tolist()}")
        # Try to find any column that looks like HUC
        for col in gdf.columns:
            if 'huc' in col.lower():
                huc_col = col
                break
    
    if not huc_col:
        print("  ERROR: Could not find HUC12 ID column")
        sys.exit(1)
    
    print(f"  Using HUC12 column: {huc_col}")
    
    # Ensure HUC12 is string with leading zeros
    gdf['huc12'] = gdf[huc_col].astype(str).str.zfill(12)
    
    # Keep only needed columns
    gdf = gdf[['huc12', 'geometry']]
    
    return gdf


def load_hex_mesh():
    """Load existing hex mesh from cache."""
    print("\nLoading hex mesh from cache...")
    
    # Find the most recent mesh file
    mesh_files = []
    for f in os.listdir(HEX_CACHE_PATH):
        if f.startswith('mesh-') and f.endswith('.json'):
            mesh_files.append(os.path.join(HEX_CACHE_PATH, f))
    
    if not mesh_files:
        print("  ERROR: No mesh cache files found!")
        sys.exit(1)
    
    # Use the first one (they should all have same hex geometry)
    mesh_path = mesh_files[0]
    print(f"  Loading: {mesh_path}")
    
    with open(mesh_path) as f:
        mesh_data = json.load(f)
    
    print(f"  Loaded {len(mesh_data.get('features', [])):,} hexagons")
    
    return mesh_data


def join_sui_to_hex(hex_mesh, huc12_gdf, sui_df):
    """Join SUI to hexes via point-in-polygon with HUC12."""
    print("\nJoining SUI to hexes...")
    
    features = hex_mesh.get('features', [])
    total = len(features)
    
    # Build spatial index for HUC12 polygons
    print("  Building spatial index for HUC12 polygons...")
    huc12_gdf = huc12_gdf.set_index('huc12')
    sindex = huc12_gdf.sindex
    
    # Track stats
    matched = 0
    no_huc = 0
    no_sui = 0
    
    # Process each hex
    print(f"  Processing {total:,} hexes...")
    for i, feature in enumerate(features):
        if i % 1000 == 0:
            print(f"    {i:,}/{total:,} ({100*i/total:.1f}%)")
        
        # Get hex centroid
        geom = shape(feature['geometry'])
        centroid = geom.centroid
        point = Point(centroid.x, centroid.y)
        
        # Find intersecting HUC12 polygons
        possible_matches_idx = list(sindex.intersection(point.bounds))
        
        huc12_id = None
        for idx in possible_matches_idx:
            huc12 = huc12_gdf.index[idx]
            if huc12_gdf.iloc[idx].geometry.contains(point):
                huc12_id = huc12
                break
        
        # Assign SUI
        if huc12_id and huc12_id in sui_df.index:
            sui_row = sui_df.loc[huc12_id]
            feature['properties']['sui'] = round(sui_row['sui'], 4)
            feature['properties']['sui_class'] = sui_row['sui_class']
            feature['properties']['huc12'] = huc12_id
            matched += 1
        elif huc12_id:
            # HUC12 found but no SUI data
            feature['properties']['sui'] = None
            feature['properties']['sui_class'] = None
            feature['properties']['huc12'] = huc12_id
            no_sui += 1
        else:
            # No HUC12 contains this hex (likely offshore or outside CONUS)
            feature['properties']['sui'] = None
            feature['properties']['sui_class'] = None
            feature['properties']['huc12'] = None
            no_huc += 1
    
    print(f"\n  Results:")
    print(f"    Matched with SUI: {matched:,} ({100*matched/total:.1f}%)")
    print(f"    HUC12 but no SUI: {no_sui:,}")
    print(f"    No HUC12 (offshore/outside): {no_huc:,}")
    
    return hex_mesh


def verify_coverage(hex_mesh):
    """Verify SUI coverage in key regions."""
    print("\nVerifying coverage in key regions...")
    
    features = hex_mesh.get('features', [])
    
    # Test regions (name, approx lng, approx lat)
    test_regions = [
        ('Wyoming', -107.5, 43.0),
        ('Montana', -110.0, 47.0),
        ('North Dakota', -100.5, 47.5),
        ('Pacific NW (WA)', -120.5, 47.5),
        ('West Texas (Permian)', -102.0, 31.8),
        ('Eastern US (Virginia)', -78.5, 37.5),
    ]
    
    for name, test_lng, test_lat in test_regions:
        # Find nearest hex
        nearest = None
        min_dist = float('inf')
        
        for feature in features:
            geom = shape(feature['geometry'])
            centroid = geom.centroid
            dist = ((centroid.x - test_lng)**2 + (centroid.y - test_lat)**2)**0.5
            if dist < min_dist:
                min_dist = dist
                nearest = feature
        
        if nearest:
            sui = nearest['properties'].get('sui')
            sui_class = nearest['properties'].get('sui_class')
            print(f"  {name}: SUI={sui}, class={sui_class}")
        else:
            print(f"  {name}: No hex found")


def save_results(hex_mesh):
    """Save updated hex mesh and SUI lookup."""
    print("\nSaving results...")
    
    # Save as standalone SUI-by-hex lookup (smaller file)
    sui_lookup = {}
    for feature in hex_mesh.get('features', []):
        hex_id = feature.get('id') or feature['properties'].get('id')
        if hex_id and feature['properties'].get('sui') is not None:
            sui_lookup[str(hex_id)] = {
                'sui': feature['properties']['sui'],
                'sui_class': feature['properties']['sui_class'],
                'huc12': feature['properties'].get('huc12')
            }
    
    with open(OUTPUT_PATH, 'w') as f:
        json.dump({
            'metadata': {
                'total_hexes': len(sui_lookup),
                'source': 'USGS Water Budget SUI joined to HUC12 polygons',
                'sui_direction': 'Higher SUI = more water stress (worse)',
                'sui_classes': {
                    'Very low/none': 'SUI < 0.1',
                    'Low': '0.1 <= SUI < 0.2',
                    'Moderate': '0.2 <= SUI < 0.4',
                    'High': '0.4 <= SUI < 0.7',
                    'Severe': 'SUI >= 0.7'
                }
            },
            'data': sui_lookup
        }, f)
    
    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"  Saved {OUTPUT_PATH} ({size_mb:.2f} MB)")
    print(f"  Contains SUI for {len(sui_lookup):,} hexes")
    
    # Also update the hex cache files with SUI
    print("\n  Updating hex cache files with SUI...")
    for f in os.listdir(HEX_CACHE_PATH):
        if f.startswith('mesh-') and f.endswith('.json'):
            cache_path = os.path.join(HEX_CACHE_PATH, f)
            print(f"    Updating {f}...")
            
            with open(cache_path) as cf:
                cache_data = json.load(cf)
            
            # Add SUI to each feature
            for feature in cache_data.get('features', []):
                hex_id = str(feature.get('id') or feature['properties'].get('id', ''))
                if hex_id in sui_lookup:
                    feature['properties']['sui'] = sui_lookup[hex_id]['sui']
                    feature['properties']['sui_class'] = sui_lookup[hex_id]['sui_class']
            
            with open(cache_path, 'w') as cf:
                json.dump(cache_data, cf)
    
    print("  Done!")


def main():
    print("=" * 60)
    print("Joining Water Stress (SUI) to SEER Hex Mesh")
    print("=" * 60)
    print("\nSUI Direction: Higher SUI = MORE water stress (worse)")
    print("  - SUI 0.0 = Very low/none stress (good for site)")
    print("  - SUI 1.0 = Severe stress (bad for site)")
    print()
    
    # Step 1: Load and aggregate SUI
    sui_df = load_and_aggregate_sui()
    
    # Step 2: Load HUC12 polygons
    huc12_gdf = load_huc12_polygons()
    
    # Step 3: Load hex mesh
    hex_mesh = load_hex_mesh()
    
    # Step 4: Join SUI to hexes
    hex_mesh = join_sui_to_hex(hex_mesh, huc12_gdf, sui_df)
    
    # Step 5: Verify coverage
    verify_coverage(hex_mesh)
    
    # Step 6: Save results
    save_results(hex_mesh)
    
    print("\n" + "=" * 60)
    print("Complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
