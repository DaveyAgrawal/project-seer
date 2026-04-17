#!/usr/bin/env python3
"""
Tier 1 CO₂-EGS Hub Screening Export (Simplified)
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
import warnings
warnings.filterwarnings('ignore')

print("Starting export...")

# PARAMETERS
PARAMS = {
    'co2_egs_temp_threshold_f': 302,
    'conventional_temp_threshold_f': 392,
    'max_basement_depth_m': 5000,
}

# FILE PATHS
BASE_DIR = Path('/Users/devanagrawal/Desktop/project-seer')
DATA_DIR = BASE_DIR / 'map' / 'web' / 'public' / 'data'
CACHE_DIR = BASE_DIR / 'map' / 'web' / 'public' / 'cache' / 'geothermal'
MORE_DATA_DIR = BASE_DIR / 'DataCenterMap-Scraper' / 'more.data'
OUTPUT_FILE = MORE_DATA_DIR / 'tier1_hub_screening.xlsx'

def f_to_c(temp_f):
    if temp_f is None or pd.isna(temp_f):
        return None
    return round((temp_f - 32) * 5 / 9, 1)

def get_polygon_centroid(geometry):
    if geometry['type'] == 'Polygon':
        coords = geometry['coordinates'][0]
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return sum(lons) / len(lons), sum(lats) / len(lats)
    return None, None

# Load ethanol plants
print("Loading ethanol plants...")
with open(DATA_DIR / 'ethanol_plants_with_emissions.geojson') as f:
    ethanol_data = json.load(f)

ethanol_records = []
for feat in ethanol_data['features']:
    props = feat['properties']
    coords = feat['geometry']['coordinates']
    ethanol_records.append({
        'source_id': f"ETH_{props.get('OBJECTID', '')}",
        'source_type': 'ethanol',
        'name': props.get('Site', 'Unknown'),
        'operator': props.get('Company', 'Unknown'),
        'state': props.get('State', ''),
        'lat': coords[1],
        'lng': coords[0],
        'annual_tonnage_co2': props.get('emissions_mt_co2e'),
        'capacity_mmgal': props.get('Cap_Mmgal'),
        'co2_purity_pct': 99.0,
        'confidence': 'High',
        'tier': 1
    })
ethanol_df = pd.DataFrame(ethanol_records)
print(f"  Loaded {len(ethanol_df)} ethanol plants")

# Load ALL emitters (for CO2_Sources_All tab)
print("Loading ALL emitters...")
with open(DATA_DIR / 'emitters.geojson') as f:
    emitters_data = json.load(f)

all_emitter_records = []
ngp_records = []
for feat in emitters_data['features']:
    props = feat['properties']
    sector = props.get('industry_type_sectors', '') or ''
    coords = feat['geometry']['coordinates']
    
    # Determine tier based on sector
    is_tier1 = 'Petroleum' in sector or 'Natural Gas' in sector
    
    record = {
        'source_id': f"EMT_{props.get('facility_id', '')}",
        'source_type': sector if sector else 'Other',
        'name': props.get('facility_name', 'Unknown'),
        'operator': 'N/A',
        'state': props.get('state', ''),
        'city': props.get('city', ''),
        'county': props.get('county', ''),
        'lat': coords[1],
        'lng': coords[0],
        'annual_tonnage_co2': props.get('total_emissions_2023'),
        'naics_code': props.get('primary_naics_code', ''),
        'subparts': props.get('industry_type_subparts', ''),
        'tier': 1 if is_tier1 else 2,
        'confidence': 'High'
    }
    all_emitter_records.append(record)
    
    # Also add to NGP list if Tier 1
    if is_tier1:
        ngp_records.append({
            'source_id': f"NGP_{props.get('facility_id', '')}",
            'source_type': 'NGP',
            'name': props.get('facility_name', 'Unknown'),
            'operator': 'N/A',
            'state': props.get('state', ''),
            'lat': coords[1],
            'lng': coords[0],
            'annual_tonnage_co2': props.get('total_emissions_2023'),
            'capacity_mmgal': None,
            'co2_purity_pct': None,
            'confidence': 'High',
            'tier': 1
        })

all_emitters_df = pd.DataFrame(all_emitter_records)
ngp_df = pd.DataFrame(ngp_records)
print(f"  Loaded {len(all_emitters_df)} total emitters ({len(ngp_df)} NGP/Tier 1)")

# Load natural CO2 reservoirs
print("Loading natural CO2 reservoirs...")
natural_original = pd.read_excel(MORE_DATA_DIR / 'natural_co2_reservoirs_CONUS.xlsx')
natural_records = []
for _, row in natural_original.iterrows():
    natural_records.append({
        'source_id': f"NAT_{str(row.get('Field Name', '')).replace(' ', '_')}",
        'source_type': 'natural_reservoir',
        'name': row.get('Field Name', 'Unknown'),
        'operator': 'Natural Source',
        'state': row.get('State', ''),
        'lat': row.get('Approx Lat'),
        'lng': row.get('Approx Lon'),
        'annual_tonnage_co2': None,
        'capacity_mmgal': None,
        'co2_purity_pct': row.get('CO2 Purity (%)'),
        'confidence': 'TENTATIVE',
        'tier': 1
    })
natural_df = pd.DataFrame(natural_records)
natural_original['DATA_QUALITY'] = 'TENTATIVE - verify coordinates'
print(f"  Loaded {len(natural_df)} natural reservoirs")

# Load pipelines
print("Loading CO2 pipelines...")
with open(MORE_DATA_DIR / 'co2_pipelines.geojson') as f:
    pipeline_data = json.load(f)

pipeline_records = []
for feat in pipeline_data['features']:
    geom = feat['geometry']
    props = feat['properties']
    if geom['type'] == 'LineString':
        coords = geom['coordinates']
        pipeline_records.append({
            'pipeline_id': props.get('objectid', feat.get('id', '')),
            'length_km': round(props.get('SHAPE__Length', 0) / 1000, 2),
            'start_lng': coords[0][0],
            'start_lat': coords[0][1],
            'end_lng': coords[-1][0],
            'end_lat': coords[-1][1]
        })
pipelines_df = pd.DataFrame(pipeline_records)
print(f"  Loaded {len(pipelines_df)} pipeline segments")

# Load geothermal mesh (5km only for speed)
print("Loading geothermal mesh (5km)...")
with open(CACHE_DIR / 'mesh-5000m.json') as f:
    geo_5km_data = json.load(f)

geo_records = []
for feat in geo_5km_data['features']:
    props = feat['properties']
    lng, lat = get_polygon_centroid(feat['geometry'])
    temp_f = props.get('avg_temperature_f')
    geo_records.append({
        'hex_id': props.get('hex_id', ''),
        'lat': lat,
        'lng': lng,
        'temp_f_5km': temp_f,
        'temp_c_5km': f_to_c(temp_f),
        'co2egs_viable_5km': temp_f is not None and temp_f >= PARAMS['co2_egs_temp_threshold_f']
    })
geothermal_df = pd.DataFrame(geo_records)
print(f"  Loaded {len(geothermal_df)} hexagons")

# Load geology
print("Loading geology...")
with open(DATA_DIR / 'geology_simplified.geojson') as f:
    geology_data = json.load(f)

geology_records = []
for feat in geology_data['features']:
    props = feat['properties']
    coords = feat['geometry']['coordinates']
    dt = props.get('dt')
    geology_records.append({
        'lat': coords[1],
        'lng': coords[0],
        'sediment_thickness_m': props.get('st'),
        'depth_to_basement_m': dt,
        'basement_shallow_enough': dt is not None and dt <= PARAMS['max_basement_depth_m']
    })
geology_df = pd.DataFrame(geology_records)
print(f"  Loaded {len(geology_df)} geology points")

# Combine sources
all_sources = pd.concat([ethanol_df, ngp_df, natural_df], ignore_index=True)
print(f"Total Tier 1 sources: {len(all_sources)}")

# Create README
readme_text = f"""TIER 1 CO₂-EGS HUB SCREENING EXPORT
===================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

PARAMETERS:
- CO₂-EGS Temp Threshold: {PARAMS['co2_egs_temp_threshold_f']}°F (150°C)
- Conventional Threshold: {PARAMS['conventional_temp_threshold_f']}°F (200°C)
- Max Basement Depth: {PARAMS['max_basement_depth_m']}m

TABS:
- CO2_Sources_Tier1: All Tier 1 CO₂ sources (ethanol, NGP, natural)
- Natural_CO2_Reservoirs: TENTATIVE natural sources (verify coordinates)
- CO2_Pipelines: Existing pipeline segments
- Geothermal_Grid: Temperature at 5km depth with viability flags
- Geology_Grid: Depth to basement data

DATA CAVEATS:
1. Natural CO₂ reservoirs are TENTATIVE - hand-compiled
2. ~30% ethanol plants missing emissions data
3. Geology has depth only, no permeability data
"""

readme_df = pd.DataFrame({'README': [readme_text]})

# Write Excel
print("Writing Excel file...")
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    readme_df.to_excel(writer, sheet_name='README', index=False)
    all_emitters_df.to_excel(writer, sheet_name='CO2_Sources_All', index=False)
    all_sources.to_excel(writer, sheet_name='CO2_Sources_Tier1', index=False)
    natural_original.to_excel(writer, sheet_name='Natural_CO2_Reservoirs', index=False)
    pipelines_df.to_excel(writer, sheet_name='CO2_Pipelines', index=False)
    geothermal_df.to_excel(writer, sheet_name='Geothermal_Grid', index=False)
    geology_df.to_excel(writer, sheet_name='Geology_Grid', index=False)

print(f"\n✅ EXPORT COMPLETE: {OUTPUT_FILE}")
print(f"  - Tier 1 sources: {len(all_sources)}")
print(f"  - Pipelines: {len(pipelines_df)}")
print(f"  - Geothermal hexagons: {len(geothermal_df)}")
print(f"  - Geology points: {len(geology_df)}")
