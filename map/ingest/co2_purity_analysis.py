import json
import pandas as pd
import numpy as np
from collections import defaultdict
from math import radians, sin, cos, sqrt, atan2
import matplotlib.pyplot as plt

# Load data
print("Loading data...")
mesh = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
emitters = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson'))
ethanol = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson'))

# NAICS classification
NAICS_HIGH = {
    '486210': 'Natural Gas Processing', '211130': 'Natural Gas Processing', '211111': 'Natural Gas Processing',
    '211112': 'Natural Gas Processing', '211120': 'Natural Gas Processing', '213112': 'Natural Gas Processing',
    '325311': 'Ammonia/Fertilizer', '325312': 'Ammonia/Fertilizer',
    '325120': 'Industrial Gas', '325193': 'Ethanol', '311221': 'Ethanol'
}
NAICS_MEDIUM = {
    '324110': 'Petroleum Refining', '324199': 'Petroleum Refining',
    '327310': 'Cement', '327410': 'Lime', '327420': 'Minerals',
    '325110': 'Petrochemicals', '325199': 'Petrochemicals', '325211': 'Petrochemicals',
    '331110': 'Iron & Steel', '331511': 'Iron & Steel'
}
NAICS_LOW = {
    '221112': 'Fossil Power', '221118': 'Fossil Power',
    '562212': 'Landfills', '562213': 'Waste-to-Energy',
    '322110': 'Pulp & Paper', '322120': 'Pulp & Paper', '322130': 'Pulp & Paper'
}

def haversine(lon1, lat1, lon2, lat2):
    R = 6371  # km
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

# Get viable hexagons (>=150C at 5km, basement <5km)
print("Finding viable hexagons...")
viable_hexes = []
for idx, f in enumerate(mesh['features']):
    p = f['properties']
    t = p.get('avg_temperature_f')
    b = p.get('basement_depth_m')
    if t and (t-32)*5/9 >= 150 and (b is None or b < 5000):
        # Calculate centroid from polygon coords
        ring = f['geometry']['coordinates'][0]
        lon = sum(c[0] for c in ring) / len(ring)
        lat = sum(c[1] for c in ring) / len(ring)
        viable_hexes.append({'lon': lon, 'lat': lat, 'idx': idx})

print(f"Found {len(viable_hexes)} viable hexagons")

# Classify emitters and get coords
print("Classifying emitters...")
emitter_data = []
for f in emitters['features']:
    p = f['properties']
    naics = str(p.get('primary_naics_code', ''))
    emissions = p.get('total_emissions_2023', 0) or 0
    coords = f['geometry']['coordinates']
    
    if naics in NAICS_HIGH:
        purity = 'High'
        sector = NAICS_HIGH[naics]
    elif naics in NAICS_MEDIUM:
        purity = 'Medium'
        sector = NAICS_MEDIUM[naics]
    elif naics in NAICS_LOW:
        purity = 'Low'
        sector = NAICS_LOW[naics]
    else:
        purity = 'Other'
        sector = 'Other Industrial'
    
    emitter_data.append({
        'lon': coords[0], 'lat': coords[1],
        'emissions': emissions, 'purity': purity, 'sector': sector
    })

# Add ethanol plants
for f in ethanol['features']:
    p = f['properties']
    coords = f['geometry']['coordinates']
    emissions = p.get('co2_emissions_tpy', 0) or p.get('total_emissions_2023', 0) or 0
    emitter_data.append({
        'lon': coords[0], 'lat': coords[1],
        'emissions': emissions, 'purity': 'High', 'sector': 'Ethanol'
    })

print(f"Total emitters: {len(emitter_data)}")

# For each emitter, find hexes within 25km
print("Matching emitters to nearby hexagons (25km radius)...")
RADIUS_KM = 25

results_by_purity = defaultdict(lambda: {
    'facilities': 0, 'total_emissions': 0, 'hexes_nearby': set(), 'by_sector': defaultdict(lambda: {'facilities': 0, 'emissions': 0, 'hexes': set()})
})

for i, em in enumerate(emitter_data):
    if i % 500 == 0:
        print(f"  Processing emitter {i}/{len(emitter_data)}...")
    
    nearby_hexes = set()
    for h in viable_hexes:
        dist = haversine(em['lon'], em['lat'], h['lon'], h['lat'])
        if dist <= RADIUS_KM:
            nearby_hexes.add(h['idx'])
    
    purity = em['purity']
    sector = em['sector']
    
    results_by_purity[purity]['facilities'] += 1
    results_by_purity[purity]['total_emissions'] += em['emissions']
    results_by_purity[purity]['hexes_nearby'].update(nearby_hexes)
    results_by_purity[purity]['by_sector'][sector]['facilities'] += 1
    results_by_purity[purity]['by_sector'][sector]['emissions'] += em['emissions']
    results_by_purity[purity]['by_sector'][sector]['hexes'].update(nearby_hexes)

# Calculate MW potential (1 MW needs 5,550 tCO2/yr)
CO2_PER_MW = 5550

print("\n" + "="*80)
print("CO2 PURITY ANALYSIS - PROXIMITY TO CO2-EGS VIABLE LAND")
print("="*80)

# Summary table
summary_rows = []
for purity in ['High', 'Medium', 'Low', 'Other']:
    r = results_by_purity[purity]
    hexes = len(r['hexes_nearby'])
    emissions = r['total_emissions']
    mw_potential = emissions / CO2_PER_MW
    seq_potential = hexes * 100 * CO2_PER_MW / 1e6  # 100MW per hex
    
    summary_rows.append({
        'Purity': purity,
        'Facilities': r['facilities'],
        'Emissions_MtCO2': round(emissions / 1e6, 2),
        'Viable_Hexes_25km': hexes,
        'Area_km2': hexes * 65,
        'MW_from_CO2': round(mw_potential, 0),
        'Seq_Potential_MtCO2_yr': round(seq_potential, 2)
    })

df_summary = pd.DataFrame(summary_rows)
print("\n=== SUMMARY BY PURITY ===")
print(df_summary.to_string(index=False))

# Detailed by sector
detail_rows = []
for purity in ['High', 'Medium', 'Low']:
    for sector, data in results_by_purity[purity]['by_sector'].items():
        hexes = len(data['hexes'])
        emissions = data['emissions']
        detail_rows.append({
            'Purity': purity,
            'Sector': sector,
            'Facilities': data['facilities'],
            'Emissions_MtCO2': round(emissions / 1e6, 2),
            'Viable_Hexes_25km': hexes,
            'MW_from_CO2': round(emissions / CO2_PER_MW, 0),
            'Seq_Potential_MtCO2_yr': round(hexes * 100 * CO2_PER_MW / 1e6, 2)
        })

df_detail = pd.DataFrame(detail_rows)
df_detail = df_detail.sort_values(['Purity', 'Emissions_MtCO2'], ascending=[True, False])
print("\n=== DETAIL BY SECTOR ===")
print(df_detail.to_string(index=False))

# Save CSVs
df_summary.to_csv('/Users/devanagrawal/Desktop/project-seer/map/ingest/co2_purity_summary.csv', index=False)
df_detail.to_csv('/Users/devanagrawal/Desktop/project-seer/map/ingest/co2_purity_by_sector.csv', index=False)
print("\nCSVs saved!")

# Create bar chart
fig, ax = plt.subplots(figsize=(10, 6))

# Stacked bar by sector within purity
purity_order = ['High', 'Medium', 'Low']
colors = {'Ethanol': '#4CAF50', 'Natural Gas Processing': '#8BC34A', 'Ammonia/Fertilizer': '#CDDC39', 'Industrial Gas': '#C0CA33',
          'Petroleum Refining': '#FF9800', 'Cement': '#FFC107', 'Lime': '#FFD54F', 'Petrochemicals': '#FFAB40', 'Iron & Steel': '#FFE082', 'Minerals': '#FFE0B2',
          'Fossil Power': '#F44336', 'Landfills': '#795548', 'Waste-to-Energy': '#A1887F', 'Pulp & Paper': '#BCAAA4'}

x = np.arange(len(purity_order))
width = 0.6

bottom = np.zeros(3)
for sector in colors.keys():
    vals = []
    for purity in purity_order:
        sector_data = results_by_purity[purity]['by_sector'].get(sector, {'hexes': set()})
        vals.append(len(sector_data['hexes']) * 65 / 1000)  # Convert to 1000 km2
    if sum(vals) > 0:
        ax.bar(x, vals, width, label=sector, bottom=bottom, color=colors.get(sector, '#9E9E9E'))
        bottom += vals

ax.set_ylabel('Viable CO₂-EGS Land Area (1000 km²)')
ax.set_xlabel('CO₂ Purity Level')
ax.set_title('CO₂-EGS Viable Land Within 25km of Emission Sources\nby CO₂ Purity and Sector')
ax.set_xticks(x)
ax.set_xticklabels(['High Purity\n(>90% CO₂)', 'Medium Purity\n(15-50% CO₂)', 'Low Purity\n(<15% CO₂)'])
ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
plt.tight_layout()
plt.savefig('/Users/devanagrawal/Desktop/project-seer/map/ingest/co2_purity_chart.png', dpi=150, bbox_inches='tight')
print("Chart saved to co2_purity_chart.png")

# Key insights
print("\n=== KEY INSIGHTS ===")
high_hexes = len(results_by_purity['High']['hexes_nearby'])
med_hexes = len(results_by_purity['Medium']['hexes_nearby'])
low_hexes = len(results_by_purity['Low']['hexes_nearby'])
total_viable = len(viable_hexes)

print(f"High-purity sources near viable land: {high_hexes} hexes ({high_hexes*65:,} km²) = {100*high_hexes/total_viable:.1f}% of total viable")
print(f"Medium-purity sources near viable land: {med_hexes} hexes ({med_hexes*65:,} km²)")
print(f"Low-purity sources near viable land: {low_hexes} hexes ({low_hexes*65:,} km²)")
