#!/usr/bin/env python3
"""
Tier 1 CO₂-EGS Hub Screening Export
====================================
Generates an Excel workbook with pre-joined, analysis-ready data for identifying
viable CO₂ dual-well geothermal hubs in the contiguous U.S.

Author: Project Seer
Date: April 2026
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from shapely.geometry import Point, LineString, Polygon, shape
from shapely.ops import nearest_points
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# PARAMETERS - Modify these to change analysis thresholds
# =============================================================================
PARAMS = {
    'grid_depths_m': [4000, 5000],
    'co2_egs_temp_threshold_f': 302,         # 150°C - minimum for CO₂-EGS viability
    'conventional_temp_threshold_f': 392,    # 200°C - Fervo/water-EGS threshold
    'max_basement_depth_m': 5000,            # Geology suitability threshold
    'tier1_emitter_sectors': ['Petroleum and Natural Gas Systems'],
    'include_ethanol': True,
    'include_ngp': True,
    'include_natural_co2': True,
    'pipeline_proximity_km': 50,             # Flag sources within this distance
    'hub_search_radius_km': 100,             # For downstream analysis reference
}

# =============================================================================
# FILE PATHS
# =============================================================================
BASE_DIR = Path('/Users/devanagrawal/Desktop/project-seer')  # project-seer root
DATA_DIR = BASE_DIR / 'map' / 'web' / 'public' / 'data'
CACHE_DIR = BASE_DIR / 'map' / 'web' / 'public' / 'cache' / 'geothermal'
MORE_DATA_DIR = BASE_DIR / 'DataCenterMap-Scraper' / 'more.data'
OUTPUT_FILE = MORE_DATA_DIR / 'tier1_hub_screening.xlsx'

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def haversine_km(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance in kilometers between two points."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return 6371 * c  # Earth radius in km

def get_polygon_centroid(geometry):
    """Extract centroid from a polygon geometry."""
    if geometry['type'] == 'Polygon':
        coords = geometry['coordinates'][0]
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return sum(lons) / len(lons), sum(lats) / len(lats)
    return None, None

def f_to_c(temp_f):
    """Convert Fahrenheit to Celsius."""
    if temp_f is None:
        return None
    return round((temp_f - 32) * 5 / 9, 1)

def load_json(filepath):
    """Load a JSON/GeoJSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_ethanol_plants():
    """Load ethanol plants with emissions data."""
    print("📍 Loading ethanol plants...")
    filepath = DATA_DIR / 'ethanol_plants_with_emissions.geojson'
    data = load_json(filepath)
    
    records = []
    for feat in data['features']:
        props = feat['properties']
        coords = feat['geometry']['coordinates']
        records.append({
            'source_id': f"ETH_{props.get('OBJECTID', '')}",
            'source_type': 'ethanol',
            'name': props.get('Site', 'Unknown'),
            'operator': props.get('Company', 'Unknown'),
            'state': props.get('State', ''),
            'lat': coords[1] if len(coords) >= 2 else props.get('Latitude'),
            'lng': coords[0] if len(coords) >= 2 else props.get('Longitude'),
            'annual_tonnage_co2': props.get('emissions_mt_co2e'),
            'capacity_mmgal': props.get('Cap_Mmgal'),
            'co2_purity_pct': 99.0,  # Ethanol fermentation CO2 is very pure
            'confidence': 'High',
            'tier': 1,
            'emissions_source': props.get('emissions_source', '')
        })
    
    df = pd.DataFrame(records)
    print(f"   ✅ Loaded {len(df)} ethanol plants")
    return df

def load_ngp_facilities():
    """Load natural gas processing facilities from emitters."""
    print("📍 Loading NGP facilities from emitters...")
    filepath = DATA_DIR / 'emitters.geojson'
    data = load_json(filepath)
    
    records = []
    for feat in data['features']:
        props = feat['properties']
        sector = props.get('industry_type_sectors', '')
        
        # Filter for Tier 1: Petroleum and Natural Gas Systems
        if sector and ('Petroleum' in sector or 'Natural Gas' in sector):
            coords = feat['geometry']['coordinates']
            records.append({
                'source_id': f"NGP_{props.get('facility_id', props.get('id', ''))}",
                'source_type': 'NGP',
                'name': props.get('facility_name', 'Unknown'),
                'operator': 'N/A',  # Not in emitters data
                'state': props.get('state', ''),
                'lat': coords[1] if len(coords) >= 2 else props.get('latitude'),
                'lng': coords[0] if len(coords) >= 2 else props.get('longitude'),
                'annual_tonnage_co2': props.get('total_emissions_2023'),
                'capacity_mmgal': None,
                'co2_purity_pct': None,  # Varies by facility
                'confidence': 'High',
                'tier': 1,
                'emissions_source': 'EPA FLIGHT 2023',
                'city': props.get('city', ''),
                'county': props.get('county', ''),
                'naics_code': props.get('primary_naics_code', ''),
                'subparts': props.get('industry_type_subparts', '')
            })
    
    df = pd.DataFrame(records)
    print(f"   ✅ Loaded {len(df)} NGP facilities")
    return df

def load_natural_co2_reservoirs():
    """Load natural CO2 reservoir data from Excel."""
    print("📍 Loading natural CO2 reservoirs...")
    filepath = MORE_DATA_DIR / 'natural_co2_reservoirs_CONUS.xlsx'
    
    df = pd.read_excel(filepath)
    
    # Standardize column names
    records = []
    for _, row in df.iterrows():
        records.append({
            'source_id': f"NAT_{row.get('Field Name', '').replace(' ', '_')}",
            'source_type': 'natural_reservoir',
            'name': row.get('Field Name', 'Unknown'),
            'operator': 'Natural Source',
            'state': row.get('State', ''),
            'lat': row.get('Approx Lat'),
            'lng': row.get('Approx Lon'),
            'annual_tonnage_co2': None,  # Production rate not specified
            'capacity_mmgal': None,
            'co2_purity_pct': row.get('CO2 Purity (%)'),
            'confidence': row.get('Confidence', 'TENTATIVE'),
            'tier': 1,
            'emissions_source': 'Hand-compiled',
            'reservoir_formation': row.get('Reservoir Formation', ''),
            'status': row.get('Status', ''),
            'notes': row.get('Notes', ''),
            'primary_source': row.get('Primary Source', '')
        })
    
    result_df = pd.DataFrame(records)
    print(f"   ✅ Loaded {len(result_df)} natural CO2 reservoirs (TENTATIVE)")
    return result_df, df  # Return both processed and original

def load_co2_pipelines():
    """Load CO2 pipeline geometries."""
    print("🔗 Loading CO2 pipelines...")
    filepath = MORE_DATA_DIR / 'co2_pipelines.geojson'
    data = load_json(filepath)
    
    pipelines = []
    pipeline_records = []
    
    for feat in data['features']:
        geom = feat['geometry']
        props = feat['properties']
        
        if geom['type'] == 'LineString':
            coords = geom['coordinates']
            line = LineString(coords)
            pipelines.append(line)
            
            # Extract endpoints
            start = coords[0]
            end = coords[-1]
            length_m = props.get('SHAPE__Length', 0)
            
            pipeline_records.append({
                'pipeline_id': props.get('objectid', feat.get('id', '')),
                'length_km': round(length_m / 1000, 2) if length_m else None,
                'start_lng': start[0],
                'start_lat': start[1],
                'end_lng': end[0],
                'end_lat': end[1]
            })
    
    df = pd.DataFrame(pipeline_records)
    print(f"   ✅ Loaded {len(df)} pipeline segments")
    return df, pipelines

def calculate_pipeline_distance(lat, lng, pipelines):
    """Calculate distance to nearest CO2 pipeline in km."""
    if pd.isna(lat) or pd.isna(lng) or not pipelines:
        return None
    
    point = Point(lng, lat)
    min_dist = float('inf')
    
    for line in pipelines:
        try:
            # Get nearest point on line
            nearest = nearest_points(point, line)[1]
            dist = haversine_km(lng, lat, nearest.x, nearest.y)
            min_dist = min(min_dist, dist)
        except:
            continue
    
    return round(min_dist, 2) if min_dist != float('inf') else None

def load_geothermal_mesh(depth_m):
    """Load geothermal hexagon mesh for a specific depth."""
    print(f"🌡️ Loading geothermal mesh at {depth_m}m...")
    filepath = CACHE_DIR / f'mesh-{depth_m}m.json'
    
    if not filepath.exists():
        print(f"   ⚠️ Mesh file not found: {filepath}")
        return pd.DataFrame()
    
    data = load_json(filepath)
    
    records = []
    for feat in data['features']:
        props = feat['properties']
        geom = feat['geometry']
        
        # Get centroid
        lng, lat = get_polygon_centroid(geom)
        
        records.append({
            'hex_id': props.get('hex_id', feat.get('id', '')),
            'lat': lat,
            'lng': lng,
            'avg_temperature_f': props.get('avg_temperature_f'),
            'point_count': props.get('point_count', 0)
        })
    
    df = pd.DataFrame(records)
    print(f"   ✅ Loaded {len(df)} hexagons at {depth_m}m")
    return df

def load_geology():
    """Load geology data (depth to basement)."""
    print("🪨 Loading geology data...")
    filepath = DATA_DIR / 'geology_simplified.geojson'
    data = load_json(filepath)
    
    records = []
    for feat in data['features']:
        props = feat['properties']
        coords = feat['geometry']['coordinates']
        
        records.append({
            'lat': coords[1],
            'lng': coords[0],
            'sediment_thickness_m': props.get('st'),
            'depth_to_basement_m': props.get('dt'),
            'quality_flag': props.get('q')
        })
    
    df = pd.DataFrame(records)
    print(f"   ✅ Loaded {len(df)} geology points")
    return df

# =============================================================================
# SPATIAL JOIN FUNCTIONS
# =============================================================================

def find_nearest_geology(hex_lat, hex_lng, geology_df):
    """Find nearest geology point to a hexagon centroid."""
    if pd.isna(hex_lat) or pd.isna(hex_lng):
        return None, None
    
    # Simple nearest neighbor (could optimize with spatial index for large datasets)
    distances = geology_df.apply(
        lambda row: haversine_km(hex_lng, hex_lat, row['lng'], row['lat']),
        axis=1
    )
    
    nearest_idx = distances.idxmin()
    nearest_row = geology_df.loc[nearest_idx]
    
    return nearest_row['depth_to_basement_m'], nearest_row['sediment_thickness_m']

def create_geothermal_geology_join(geo_4km, geo_5km, geology_df):
    """Create the key pre-joined geothermal + geology dataset using vectorized operations."""
    print("🔗 Creating geothermal-geology spatial join (optimized)...")
    
    # Use 5km mesh as base (more relevant for CO2-EGS)
    if geo_5km.empty:
        print("   ⚠️ No 5km mesh data available")
        return pd.DataFrame()
    
    # Sample geology heavily for speed (every 50th point)
    geology_sample = geology_df.iloc[::50].reset_index(drop=True)
    print(f"   Using {len(geology_sample)} geology sample points")
    
    # Build a simple grid-based lookup for geology
    geo_grid = {}
    for _, row in geology_sample.iterrows():
        # Round to 0.5 degree grid cells
        key = (round(row['lat'] * 2) / 2, round(row['lng'] * 2) / 2)
        if key not in geo_grid:
            geo_grid[key] = []
        geo_grid[key].append((row['depth_to_basement_m'], row['sediment_thickness_m']))
    
    def get_nearest_geology(lat, lng):
        if pd.isna(lat) or pd.isna(lng):
            return None, None
        key = (round(lat * 2) / 2, round(lng * 2) / 2)
        # Check this cell and neighbors
        for dlat in [0, 0.5, -0.5]:
            for dlng in [0, 0.5, -0.5]:
                check_key = (key[0] + dlat, key[1] + dlng)
                if check_key in geo_grid and geo_grid[check_key]:
                    # Return average of points in cell
                    depths = [p[0] for p in geo_grid[check_key] if p[0] is not None]
                    seds = [p[1] for p in geo_grid[check_key] if p[1] is not None]
                    return (np.mean(depths) if depths else None, np.mean(seds) if seds else None)
        return None, None
    
    # Merge 4km and 5km data first
    result_df = geo_5km.copy()
    result_df = result_df.rename(columns={'avg_temperature_f': 'temp_f_5km'})
    
    if not geo_4km.empty:
        geo_4km_subset = geo_4km[['hex_id', 'avg_temperature_f']].rename(columns={'avg_temperature_f': 'temp_f_4km'})
        result_df = result_df.merge(geo_4km_subset, on='hex_id', how='left')
    else:
        result_df['temp_f_4km'] = None
    
    # Apply geology lookup
    print("   Applying geology lookup...")
    geology_data = result_df.apply(lambda row: get_nearest_geology(row['lat'], row['lng']), axis=1)
    result_df['depth_to_basement_m'] = [g[0] for g in geology_data]
    result_df['sediment_thickness_m'] = [g[1] for g in geology_data]
    
    # Calculate derived columns
    result_df['temp_c_4km'] = result_df['temp_f_4km'].apply(f_to_c)
    result_df['temp_c_5km'] = result_df['temp_f_5km'].apply(f_to_c)
    result_df['co2egs_viable_4km'] = result_df['temp_f_4km'].notna() & (result_df['temp_f_4km'] >= PARAMS['co2_egs_temp_threshold_f'])
    result_df['co2egs_viable_5km'] = result_df['temp_f_5km'].notna() & (result_df['temp_f_5km'] >= PARAMS['co2_egs_temp_threshold_f'])
    result_df['conventional_viable_4km'] = result_df['temp_f_4km'].notna() & (result_df['temp_f_4km'] >= PARAMS['conventional_temp_threshold_f'])
    result_df['basement_shallow_enough'] = result_df['depth_to_basement_m'].notna() & (result_df['depth_to_basement_m'] <= PARAMS['max_basement_depth_m'])
    result_df['geology_suitable'] = result_df['co2egs_viable_5km'] & result_df['basement_shallow_enough']
    
    viable_count = result_df['geology_suitable'].sum()
    print(f"   ✅ Created join with {len(result_df)} hexagons, {viable_count} geology-suitable")
    return result_df

# =============================================================================
# README GENERATION
# =============================================================================

def create_readme():
    """Create README tab content."""
    readme_text = f"""
TIER 1 CO₂-EGS HUB SCREENING EXPORT
===================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Project Seer - CO₂-EGS Geothermal Resource Mapping Tool

PURPOSE
-------
This workbook contains pre-joined, analysis-ready data for identifying viable 
CO₂ dual-well geothermal hubs in the contiguous United States. It enables 
downstream analysis to answer: "How many viable CO₂-EGS hubs realistically exist?"

PARAMETERS USED
---------------
- CO₂-EGS Temperature Threshold: {PARAMS['co2_egs_temp_threshold_f']}°F ({f_to_c(PARAMS['co2_egs_temp_threshold_f'])}°C)
- Conventional EGS Threshold: {PARAMS['conventional_temp_threshold_f']}°F ({f_to_c(PARAMS['conventional_temp_threshold_f'])}°C)
- Max Depth to Basement: {PARAMS['max_basement_depth_m']}m
- Pipeline Proximity Flag: {PARAMS['pipeline_proximity_km']}km
- Hub Search Radius (reference): {PARAMS['hub_search_radius_km']}km

TAB DESCRIPTIONS
----------------

CO2_Sources_Tier1:
  One row per Tier 1 CO₂ source (ethanol plants, NGP facilities, natural reservoirs)
  - source_id: Unique identifier (ETH_xxx, NGP_xxx, NAT_xxx)
  - source_type: ethanol, NGP, or natural_reservoir
  - annual_tonnage_co2: Metric tons CO₂ per year (from EPA FLIGHT or estimated)
  - co2_purity_pct: CO₂ stream purity (99% for ethanol, varies for others)
  - distance_to_nearest_pipeline_km: Distance to nearest existing CO₂ pipeline

Natural_CO2_Reservoirs:
  ⚠️ TENTATIVE DATA - Hand-compiled, county-level coordinates need field verification
  Contains all original columns from source Excel plus pipeline distance

CO2_Pipelines:
  Existing CO₂ pipeline segments with endpoints
  - length_km: Pipeline segment length
  - start/end coordinates: Endpoints of LineString geometry

Geothermal_Grid:
  Hexagon grid (~10-15km spacing) with temperature at 4km and 5km depths
  - co2egs_viable_Xkm: TRUE if temperature ≥ {PARAMS['co2_egs_temp_threshold_f']}°F at that depth
  - conventional_viable_4km: TRUE if temperature ≥ {PARAMS['conventional_temp_threshold_f']}°F

Geology_Grid:
  Point grid with depth to basement and sediment thickness
  - basement_shallow_enough: TRUE if depth_to_basement ≤ {PARAMS['max_basement_depth_m']}m

Geothermal_Geology_Joined:
  ⭐ KEY PRE-JOIN - Each geothermal hexagon with nearest geology data
  - geology_suitable: TRUE if (temp ≥ {PARAMS['co2_egs_temp_threshold_f']}°F) AND (basement ≤ {PARAMS['max_basement_depth_m']}m)
  This is the primary screening layer for viable CO₂-EGS locations

DATA SOURCES
------------
- Ethanol Plants: EIA-819 (as of January 2024)
- Emissions: EPA FLIGHT 2023
- Natural CO₂ Reservoirs: Hand-compiled from Allis et al. 2001, Gilfillan et al. 2008
- CO₂ Pipelines: HIFLD/DOT pipeline data
- Geothermal: SMU Geothermal Laboratory temperature-at-depth models
- Geology: USGS depth to basement/sediment thickness

UNITS
-----
- Temperature: Fahrenheit (°F) and Celsius (°C)
- Distance: Kilometers (km)
- Depth: Meters (m)
- Emissions: Metric tons CO₂ equivalent per year
- Capacity: Million gallons per year (ethanol plants)

DATA QUALITY CAVEATS
--------------------
1. Natural CO₂ reservoirs are TENTATIVE - hand-compiled with county-level 
   coordinates that require field verification before use in final analysis.

2. CO₂ pipelines have geometry only - no operator, capacity, or flow direction 
   data available. Use for proximity screening only.

3. ~30% of ethanol plants are missing emissions data due to EPA FLIGHT gaps.
   Consider using capacity (Cap_Mmgal) as a proxy for emissions estimation.

4. Geology data includes only depth to basement - no permeability, seal quality,
   or other reservoir characterization data. Additional geological screening
   required for final site selection.

5. Geothermal temperatures are modeled estimates, not direct measurements.
   Uncertainty increases with depth and distance from control points.

DOWNSTREAM ANALYSIS SUGGESTIONS
-------------------------------
With this workbook, you can:
1. For each CO₂ source, find nearby hexagons where geology_suitable = TRUE
2. Prioritize sources near existing CO₂ pipelines (lower transport cost)
3. Cluster co-located sources into candidate "hubs"
4. Sensitivity-test by varying temperature and basement depth thresholds
5. Produce a ranked shortlist of candidate sites for detailed evaluation

CONTACT
-------
Project Seer - CO₂-EGS Geothermal Resource Mapping Tool
"""
    
    return pd.DataFrame({'README': [readme_text]})

# =============================================================================
# MAIN EXPORT FUNCTION
# =============================================================================

def main():
    print("=" * 60)
    print("TIER 1 CO₂-EGS HUB SCREENING EXPORT")
    print("=" * 60)
    print(f"Output: {OUTPUT_FILE}")
    print()
    
    # Load CO2 sources
    ethanol_df = load_ethanol_plants() if PARAMS['include_ethanol'] else pd.DataFrame()
    ngp_df = load_ngp_facilities() if PARAMS['include_ngp'] else pd.DataFrame()
    natural_df, natural_original = load_natural_co2_reservoirs() if PARAMS['include_natural_co2'] else (pd.DataFrame(), pd.DataFrame())
    
    # Load pipelines
    pipelines_df, pipeline_geoms = load_co2_pipelines()
    
    # Calculate pipeline distances for all sources
    print("📏 Calculating pipeline distances...")
    
    for df in [ethanol_df, ngp_df, natural_df]:
        if not df.empty:
            df['distance_to_nearest_pipeline_km'] = df.apply(
                lambda row: calculate_pipeline_distance(row['lat'], row['lng'], pipeline_geoms),
                axis=1
            )
            df['near_pipeline'] = df['distance_to_nearest_pipeline_km'] <= PARAMS['pipeline_proximity_km']
    
    # Combine all Tier 1 sources
    all_sources = pd.concat([ethanol_df, ngp_df, natural_df], ignore_index=True)
    print(f"📊 Total Tier 1 sources: {len(all_sources)}")
    
    # Load geothermal data
    geo_4km = load_geothermal_mesh(4000)
    geo_5km = load_geothermal_mesh(5000)
    
    # Load geology
    geology_df = load_geology()
    
    # Add basement_shallow_enough flag to geology
    geology_df['basement_shallow_enough'] = geology_df['depth_to_basement_m'] <= PARAMS['max_basement_depth_m']
    
    # Create geothermal grid export (combined 4km and 5km)
    print("🌡️ Creating geothermal grid export...")
    geothermal_grid = geo_5km.copy()
    geothermal_grid = geothermal_grid.rename(columns={'avg_temperature_f': 'temp_f_5km'})
    geothermal_grid['temp_c_5km'] = geothermal_grid['temp_f_5km'].apply(f_to_c)
    geothermal_grid['co2egs_viable_5km'] = geothermal_grid['temp_f_5km'] >= PARAMS['co2_egs_temp_threshold_f']
    
    # Merge 4km temperatures
    if not geo_4km.empty:
        geo_4km_subset = geo_4km[['hex_id', 'avg_temperature_f']].rename(columns={'avg_temperature_f': 'temp_f_4km'})
        geothermal_grid = geothermal_grid.merge(geo_4km_subset, on='hex_id', how='left')
        geothermal_grid['temp_c_4km'] = geothermal_grid['temp_f_4km'].apply(f_to_c)
        geothermal_grid['co2egs_viable_4km'] = geothermal_grid['temp_f_4km'] >= PARAMS['co2_egs_temp_threshold_f']
        geothermal_grid['conventional_viable_4km'] = geothermal_grid['temp_f_4km'] >= PARAMS['conventional_temp_threshold_f']
    
    # Create geothermal-geology join
    joined_df = create_geothermal_geology_join(geo_4km, geo_5km, geology_df)
    
    # Add pipeline distance to natural reservoirs original
    if not natural_original.empty:
        natural_original['distance_to_nearest_pipeline_km'] = natural_df['distance_to_nearest_pipeline_km'].values
        natural_original['DATA_QUALITY'] = 'TENTATIVE - verify coordinates'
    
    # Create README
    readme_df = create_readme()
    
    # Write Excel workbook
    print()
    print("📝 Writing Excel workbook...")
    
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # README first
        readme_df.to_excel(writer, sheet_name='README', index=False)
        
        # CO2 Sources
        if not all_sources.empty:
            # Select and order columns for export
            source_cols = ['source_id', 'source_type', 'name', 'operator', 'state', 'lat', 'lng',
                          'annual_tonnage_co2', 'co2_purity_pct', 'confidence', 'tier',
                          'distance_to_nearest_pipeline_km', 'near_pipeline']
            export_cols = [c for c in source_cols if c in all_sources.columns]
            all_sources[export_cols].to_excel(writer, sheet_name='CO2_Sources_Tier1', index=False)
        
        # Natural reservoirs (original format with flag)
        if not natural_original.empty:
            natural_original.to_excel(writer, sheet_name='Natural_CO2_Reservoirs', index=False)
        
        # Pipelines
        if not pipelines_df.empty:
            pipelines_df.to_excel(writer, sheet_name='CO2_Pipelines', index=False)
        
        # Geothermal grid
        if not geothermal_grid.empty:
            geo_cols = ['hex_id', 'lat', 'lng', 'temp_f_4km', 'temp_c_4km', 'temp_f_5km', 'temp_c_5km',
                       'co2egs_viable_4km', 'co2egs_viable_5km', 'conventional_viable_4km']
            export_geo_cols = [c for c in geo_cols if c in geothermal_grid.columns]
            geothermal_grid[export_geo_cols].to_excel(writer, sheet_name='Geothermal_Grid', index=False)
        
        # Geology grid
        if not geology_df.empty:
            geology_df.to_excel(writer, sheet_name='Geology_Grid', index=False)
        
        # Geothermal-Geology joined (the key pre-join)
        if not joined_df.empty:
            joined_df.to_excel(writer, sheet_name='Geothermal_Geology_Joined', index=False)
    
    print()
    print("=" * 60)
    print("✅ EXPORT COMPLETE")
    print("=" * 60)
    print(f"Output file: {OUTPUT_FILE}")
    print()
    print("Summary:")
    print(f"  - Tier 1 CO₂ sources: {len(all_sources)}")
    print(f"    - Ethanol plants: {len(ethanol_df)}")
    print(f"    - NGP facilities: {len(ngp_df)}")
    print(f"    - Natural reservoirs: {len(natural_df)} (TENTATIVE)")
    print(f"  - CO₂ pipeline segments: {len(pipelines_df)}")
    print(f"  - Geothermal hexagons: {len(geothermal_grid)}")
    print(f"  - Geology points: {len(geology_df)}")
    print(f"  - Geology-suitable hexagons: {joined_df['geology_suitable'].sum() if not joined_df.empty else 0}")
    print()

if __name__ == '__main__':
    main()
