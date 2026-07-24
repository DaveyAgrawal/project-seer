import json
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict
import csv

# Load data
mesh = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
emitters = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson'))
ethanol = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson'))
primacy_data = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/class_vi_primacy.json'))

# Download US states for state lookup
import requests
states_geo = requests.get("https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json").json()
from shapely.geometry import shape, Point

state_polys = {}
for f in states_geo['features']:
    name = f['properties'].get('name')
    if name:
        state_polys[name] = shape(f['geometry'])

def get_state(lon, lat):
    pt = Point(lon, lat)
    for name, poly in state_polys.items():
        if poly.contains(pt):
            return name
    return None

# Primacy lookup
primacy_lookup = {}
for abbr, data in primacy_data['data'].items():
    primacy_lookup[data['state']] = data['primacy_status']

NAICS_MAP = {
    '486210': ('High', 'Natural Gas Processing'), '211130': ('High', 'Natural Gas Processing'),
    '211111': ('High', 'Natural Gas Processing'), '211112': ('High', 'Natural Gas Processing'),
    '211120': ('High', 'Natural Gas Processing'), '213112': ('High', 'Natural Gas Processing'),
    '325311': ('High', 'Ammonia/Fertilizer'), '325312': ('High', 'Ammonia/Fertilizer'),
    '325120': ('High', 'Industrial Gas'), '325193': ('High', 'Ethanol'), '311221': ('High', 'Ethanol'),
    '324110': ('Medium', 'Petroleum Refining'), '324199': ('Medium', 'Petroleum Refining'),
    '327310': ('Medium', 'Cement'), '327410': ('Medium', 'Lime'), '327420': ('Medium', 'Minerals'),
    '325110': ('Medium', 'Petrochemicals'), '325199': ('Medium', 'Petrochemicals'), '325211': ('Medium', 'Petrochemicals'),
    '331110': ('Medium', 'Iron & Steel'), '331511': ('Medium', 'Iron & Steel'),
    '221112': ('Low', 'Fossil Power'), '221118': ('Low', 'Fossil Power'),
    '562212': ('Low', 'Landfills'), '562213': ('Low', 'Waste-to-Energy'),
    '322110': ('Low', 'Pulp & Paper'), '322120': ('Low', 'Pulp & Paper'), '322130': ('Low', 'Pulp & Paper'),
}

def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

# Build emitter list
all_emitters = []
for f in emitters['features']:
    p = f['properties']
    naics = str(p.get('primary_naics_code', ''))
    if naics in NAICS_MAP:
        purity, sector = NAICS_MAP[naics]
        all_emitters.append({
            'lon': f['geometry']['coordinates'][0],
            'lat': f['geometry']['coordinates'][1],
            'name': p.get('facility_name', 'Unknown'),
            'emissions': p.get('total_emissions_2023', 0) or 0,
            'purity': purity,
            'sector': sector
        })

for f in ethanol['features']:
    p = f['properties']
    all_emitters.append({
        'lon': f['geometry']['coordinates'][0],
        'lat': f['geometry']['coordinates'][1],
        'name': p.get('plant_name', p.get('facility_name', 'Ethanol Plant')),
        'emissions': p.get('co2_emissions_tpy', 0) or p.get('total_emissions_2023', 0) or 0,
        'purity': 'High',
        'sector': 'Ethanol'
    })

# CO2 requirements per plant size (tCO2/yr at 5550 tCO2/MW/yr)
CO2_5MW = 5 * 5550    # 27,750
CO2_10MW = 10 * 5550  # 55,500
CO2_50MW = 50 * 5550  # 277,500

# Analyze each viable hex
print("Analyzing viable hexagons...")
locations = []

for idx, f in enumerate(mesh['features']):
    p = f['properties']
    temp_f = p.get('avg_temperature_f')
    basement = p.get('basement_depth_m')
    
    if not temp_f:
        continue
    temp_c = (temp_f - 32) * 5/9
    if temp_c < 150 or (basement and basement >= 5000):
        continue
    
    # Get centroid
    ring = f['geometry']['coordinates'][0]
    lon = sum(c[0] for c in ring) / len(ring)
    lat = sum(c[1] for c in ring) / len(ring)
    
    # Find nearest emitters within 50km
    nearby = []
    for em in all_emitters:
        d = haversine(lon, lat, em['lon'], em['lat'])
        if d <= 50:
            nearby.append({**em, 'distance_km': round(d, 1)})
    
    # Sort by purity (High first) then distance
    purity_order = {'High': 0, 'Medium': 1, 'Low': 2}
    nearby.sort(key=lambda x: (purity_order.get(x['purity'], 3), x['distance_km']))
    
    # Calculate total CO2 within 25km (high purity only for best economics)
    co2_high_25km = sum(e['emissions'] for e in nearby if e['purity'] == 'High' and e['distance_km'] <= 25)
    co2_all_25km = sum(e['emissions'] for e in nearby if e['distance_km'] <= 25)
    co2_all_50km = sum(e['emissions'] for e in nearby)
    
    # Get state and primacy
    state = get_state(lon, lat)
    primacy = primacy_lookup.get(state, 'federal') if state else 'unknown'
    
    # Nearest high-purity emitter
    nearest_high = next((e for e in nearby if e['purity'] == 'High'), None)
    
    locations.append({
        'hex_idx': idx,
        'lat': round(lat, 4),
        'lon': round(lon, 4),
        'temp_c': round(temp_c, 1),
        'basement_m': basement,
        'state': state or 'Unknown',
        'primacy': primacy,
        'nearest_high_name': nearest_high['name'] if nearest_high else 'None',
        'nearest_high_sector': nearest_high['sector'] if nearest_high else 'None',
        'nearest_high_dist_km': nearest_high['distance_km'] if nearest_high else 999,
        'nearest_high_tpy': nearest_high['emissions'] if nearest_high else 0,
        'co2_high_25km_tpy': co2_high_25km,
        'co2_all_25km_tpy': co2_all_25km,
        'co2_all_50km_tpy': co2_all_50km,
        'emitters_50km': len(nearby),
        'meets_5mw': 'Yes' if co2_high_25km >= CO2_5MW else 'No',
        'meets_10mw': 'Yes' if co2_high_25km >= CO2_10MW else 'No',
        'meets_50mw': 'Yes' if co2_high_25km >= CO2_50MW else 'No',
    })

# Score and rank locations
# Score = temp + (primacy bonus) + (high purity CO2 availability) - (distance penalty)
for loc in locations:
    score = 0
    score += (loc['temp_c'] - 150) * 2  # Higher temp = better
    score += 20 if loc['primacy'] == 'primacy' else 0  # Primacy bonus
    score += min(loc['co2_high_25km_tpy'] / 10000, 50)  # CO2 availability (capped)
    score -= loc['nearest_high_dist_km'] * 0.5  # Distance penalty
    loc['score'] = round(score, 1)

# Sort by score
locations.sort(key=lambda x: x['score'], reverse=True)

# Save full CSV
with open('/Users/devanagrawal/Desktop/project-seer/map/ingest/pilot_locations.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=locations[0].keys())
    writer.writeheader()
    writer.writerows(locations)

print(f"\nSaved {len(locations)} locations to pilot_locations.csv")

# Print top 15
print("\n=== TOP 15 PILOT LOCATIONS ===")
print(f"{'Rank':<4} {'Lat':<8} {'Lon':<10} {'Temp°C':<7} {'State':<12} {'Primacy':<10} {'Nearest High-Purity':<30} {'Dist':<6} {'CO2 tpy':<10} {'5MW':<4} {'10MW':<4} {'50MW':<4}")
print("-" * 130)
for i, loc in enumerate(locations[:15], 1):
    print(f"{i:<4} {loc['lat']:<8} {loc['lon']:<10} {loc['temp_c']:<7} {loc['state']:<12} {loc['primacy']:<10} {loc['nearest_high_name'][:28]:<30} {loc['nearest_high_dist_km']:<6} {loc['co2_high_25km_tpy']:<10.0f} {loc['meets_5mw']:<4} {loc['meets_10mw']:<4} {loc['meets_50mw']:<4}")

# Summary stats
meets_5 = sum(1 for l in locations if l['meets_5mw'] == 'Yes')
meets_10 = sum(1 for l in locations if l['meets_10mw'] == 'Yes')
meets_50 = sum(1 for l in locations if l['meets_50mw'] == 'Yes')
print(f"\nSUMMARY: {meets_5} locations meet 5MW, {meets_10} meet 10MW, {meets_50} meet 50MW requirements")
