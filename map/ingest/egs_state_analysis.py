import json
import pandas as pd
from collections import defaultdict
from shapely.geometry import shape, Point
import requests

# Load data
d5 = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
d4 = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-4000m.json'))
primacy = pd.read_csv('/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/class_vi_primacy_status.csv')

# Download US states geojson
print("Loading US states boundaries...")
states_url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
states_geo = requests.get(states_url).json()

state_polys = {}
for f in states_geo['features']:
    name = f['properties'].get('name')
    if name:
        state_polys[name] = shape(f['geometry'])

def get_state(coords):
    pt = Point(coords[0], coords[1])
    for name, poly in state_polys.items():
        if poly.contains(pt):
            return name
    return None

# Count hexes by state
state_conv = defaultdict(int)
state_co2 = defaultdict(int)

for f in d4['features']:
    p = f['properties']
    t = p.get('avg_temperature_f')
    b = p.get('basement_depth_m')
    if t and (t-32)*5/9 >= 200 and (b is None or b < 4000):
        coords = f['geometry']['coordinates'][0][0]
        state = get_state(coords)
        if state:
            state_conv[state] += 1

for f in d5['features']:
    p = f['properties']
    t = p.get('avg_temperature_f')
    b = p.get('basement_depth_m')
    if t and (t-32)*5/9 >= 150 and (b is None or b < 5000):
        coords = f['geometry']['coordinates'][0][0]
        state = get_state(coords)
        if state:
            state_co2[state] += 1

# Build primacy lookup
primacy_lookup = dict(zip(primacy['state'], primacy['primacy_status']))

# Create table
rows = []
all_states = set(state_conv.keys()) | set(state_co2.keys())
for state in sorted(all_states):
    conv = state_conv.get(state, 0)
    co2 = state_co2.get(state, 0)
    status = primacy_lookup.get(state, 'federal')
    rows.append({
        'State': state,
        'Conv_Hexes': conv,
        'Conv_km2': conv * 65,
        'CO2_Hexes': co2,
        'CO2_km2': co2 * 65,
        'Plants_100MW': co2,
        'Seq_MtCO2_yr': round(co2 * 100 * 5550 / 1e6, 2),
        'Primacy': status
    })

df = pd.DataFrame(rows)
df.to_csv('/Users/devanagrawal/Desktop/project-seer/map/ingest/egs_state_breakdown.csv', index=False)

print("=== STATE BREAKDOWN ===")
print(df.to_string(index=False))

# Primacy summary
primacy_hex = sum(r['CO2_Hexes'] for r in rows if r['Primacy'] == 'primacy')
federal_hex = sum(r['CO2_Hexes'] for r in rows if r['Primacy'] != 'primacy')
total = primacy_hex + federal_hex

print("\n=== PRIMACY VS FEDERAL ===")
print(f"Primacy: {primacy_hex} hex = {primacy_hex*65:,} km2 ({100*primacy_hex/total:.1f}%)")
print(f"Federal: {federal_hex} hex = {federal_hex*65:,} km2 ({100*federal_hex/total:.1f}%)")

# Totals
total_seq = sum(r['Seq_MtCO2_yr'] for r in rows)
print(f"\n=== TOTAL CO2-EGS POTENTIAL ===")
print(f"Plants: {total:,} x 100MW = {total*100/1000:.0f} GW")
print(f"Sequestration: {total_seq:.1f} MtCO2/yr")
