#!/usr/bin/env python3
"""
CORRECT CO2-EGS Analysis with proper basement filtering.
Criteria: >=150C at 5km depth AND basement depth < 5km
"""
import json
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict
import csv

print("="*80)
print("CORRECT CO2-EGS ANALYSIS")
print("Criteria: >=150C at 5km AND basement < 5km")
print("="*80)

# Load data
print("\nLoading data...")
mesh_5km = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
mesh_4km = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-4000m.json'))
geology = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/geology_simplified.geojson'))
emitters = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson'))
ethanol = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson'))
primacy_data = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/class_vi_primacy.json'))
# Download US states if needed - use reverse geocoding instead
import urllib.request
try:
    states_geo = json.load(open('/tmp/us-states.geojson'))
except:
    print("  Downloading US states boundaries...")
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json',
        '/tmp/us-states.geojson'
    )
    states_geo = json.load(open('/tmp/us-states.geojson'))

print(f"  Mesh 5km features: {len(mesh_5km['features'])}")
print(f"  Mesh 4km features: {len(mesh_4km['features'])}")
print(f"  Geology points: {len(geology['features'])}")

# Constants
HEX_SQ_KM = 168.22  # CORRECT: 65 sq miles = 168 km²

def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

# Build geology spatial index (simple grid-based)
print("\nBuilding geology spatial index...")
geo_grid = defaultdict(list)
for f in geology['features']:
    coords = f['geometry']['coordinates']
    if f['geometry']['type'] == 'Point':
        lon, lat = coords[0], coords[1]
    else:
        # Polygon - get centroid
        ring = coords[0] if f['geometry']['type'] == 'Polygon' else coords[0][0]
        lon = sum(c[0] for c in ring) / len(ring)
        lat = sum(c[1] for c in ring) / len(ring)
    
    dt = f['properties'].get('dt')
    if dt is not None:
        # Grid key at 0.5 degree resolution
        key = (round(lon * 2) / 2, round(lat * 2) / 2)
        geo_grid[key].append((lon, lat, dt))

print(f"  Geology grid cells: {len(geo_grid)}")

def get_basement_depth(hex_lon, hex_lat):
    """Find nearest geology point and return its depth-to-basement."""
    key = (round(hex_lon * 2) / 2, round(hex_lat * 2) / 2)
    
    # Search nearby grid cells
    candidates = []
    for dx in [-0.5, 0, 0.5]:
        for dy in [-0.5, 0, 0.5]:
            k = (key[0] + dx, key[1] + dy)
            candidates.extend(geo_grid.get(k, []))
    
    if not candidates:
        return None
    
    # Find nearest
    min_dist = float('inf')
    nearest_dt = None
    for glon, glat, dt in candidates:
        d = haversine(hex_lon, hex_lat, glon, glat)
        if d < min_dist:
            min_dist = d
            nearest_dt = dt
    
    # Only use if within 50km (reasonable interpolation distance)
    if min_dist < 50:
        return nearest_dt
    return None

# Process hexes with BOTH temperature AND basement criteria
print("\nProcessing hexes with temperature AND basement criteria...")
viable_co2egs = []  # >=150C at 5km AND basement <5km
viable_conv = []    # >=200C at 4km AND basement <4km
temp_only_150 = []  # >=150C at 5km (no basement filter)
temp_only_200_4km = []  # >=200C at 4km (no basement filter)

# CO2-EGS: Use 5km mesh, >=150C, basement <5km
for f in mesh_5km['features']:
    p = f['properties']
    temp_f = p.get('avg_temperature_f')
    if not temp_f:
        continue
    
    temp_c = (temp_f - 32) * 5/9
    
    # Get hex centroid
    ring = f['geometry']['coordinates'][0]
    lon = sum(c[0] for c in ring) / len(ring)
    lat = sum(c[1] for c in ring) / len(ring)
    
    # Get basement depth from geology
    basement_m = get_basement_depth(lon, lat)
    
    if temp_c >= 150:
        temp_only_150.append((lon, lat, temp_c, basement_m))
        
        # CO2-EGS: >=150C at 5km AND basement <5km
        if basement_m is not None and basement_m < 5000:
            viable_co2egs.append((lon, lat, temp_c, basement_m))

# Conventional EGS: Use 4km mesh, >=200C, basement <4km
for f in mesh_4km['features']:
    p = f['properties']
    temp_f = p.get('avg_temperature_f')
    if not temp_f:
        continue
    
    temp_c = (temp_f - 32) * 5/9
    
    if temp_c >= 200:
        ring = f['geometry']['coordinates'][0]
        lon = sum(c[0] for c in ring) / len(ring)
        lat = sum(c[1] for c in ring) / len(ring)
        
        temp_only_200_4km.append((lon, lat, temp_c))
        
        basement_m = get_basement_depth(lon, lat)
        # Conventional: >=200C at 4km AND basement <4km
        if basement_m is not None and basement_m < 4000:
            viable_conv.append((lon, lat, temp_c, basement_m))

print(f"\n=== RESULTS ===")
print(f"Hexes with >=150C at 5km (temp only, no basement filter): {len(temp_only_150)}")
print(f"Hexes with >=200C at 4km (temp only, no basement filter): {len(temp_only_200_4km)}")
print(f"Hexes with >=150C at 5km AND basement <5km (CO2-EGS): {len(viable_co2egs)}")
print(f"Hexes with >=200C at 4km AND basement <4km (Conventional): {len(viable_conv)}")

# Check how many have no geology data
no_geo = sum(1 for _, _, _, b in temp_only_150 if b is None)
print(f"Hexes >=150C with NO geology data: {no_geo}")

print(f"\n=== TABLE 8: National CO2-EGS Potential ===")
n = len(viable_co2egs)
print(f"Viable hexagons: {n:,}")
print(f"Area (km²): {n * HEX_SQ_KM:,.0f}")
print(f"Area (acres): {n * 64.95 * 640 / 1e6:.1f} million")
print(f"Capacity (GW): {n * 100 / 1000:,.0f}")
print(f"Sequestration (MtCO2/yr): ~{n * 100 * 5550 / 1e6:,.0f}")

print(f"\n=== TABLE 9: EGS Comparison ===")
print(f"Conventional EGS (>=200C, basement <4km): {len(viable_conv):,} hexes = {len(viable_conv) * HEX_SQ_KM:,.0f} km²")
print(f"CO2-EGS (>=150C, basement <5km): {len(viable_co2egs):,} hexes = {len(viable_co2egs) * HEX_SQ_KM:,.0f} km²")
if len(viable_conv) > 0:
    print(f"Expansion factor: {len(viable_co2egs) / len(viable_conv):.1f}x")

# State breakdown
print(f"\n=== Building state breakdown... ===")
from shapely.geometry import shape, Point

state_polys = {}
state_abbrev = {}
for f in states_geo['features']:
    name = f['properties'].get('name') or f['properties'].get('NAME')
    abbr = f['properties'].get('abbreviation') or f['properties'].get('STUSPS')
    if name:
        try:
            state_polys[name] = shape(f['geometry'])
            state_abbrev[name] = abbr
        except:
            pass

def get_state(lon, lat):
    pt = Point(lon, lat)
    for name, poly in state_polys.items():
        try:
            if poly.contains(pt):
                return name
        except:
            pass
    return None

# Get primacy status
primacy_lookup = {}
for abbr, info in primacy_data.get('data', {}).items():
    state_name = info.get('state')
    status = info.get('primacy_status', 'federal')
    if state_name:
        primacy_lookup[state_name] = status.capitalize()

# Count by state
state_co2egs = defaultdict(int)
state_conv = defaultdict(int)

for lon, lat, temp_c, basement in viable_co2egs:
    state = get_state(lon, lat)
    if state:
        state_co2egs[state] += 1

for lon, lat, temp_c, basement in viable_conv:
    state = get_state(lon, lat)
    if state:
        state_conv[state] += 1

print(f"\n=== TABLE 10: Top States by CO2-EGS Potential ===")
sorted_states = sorted(state_co2egs.items(), key=lambda x: -x[1])[:15]
print(f"{'State':<15} {'Hexes':>8} {'Area (km²)':>12} {'Capacity (GW)':>12} {'Seq (MtCO2)':>12} {'Primacy':>12}")
print("-" * 75)
for state, hexes in sorted_states:
    km2 = hexes * HEX_SQ_KM
    gw = hexes * 100 / 1000
    seq = hexes * 100 * 5550 / 1e6
    status = primacy_lookup.get(state, 'Federal')
    print(f"{state:<15} {hexes:>8,} {km2:>12,.0f} {gw:>12.1f} {seq:>12,.0f} {status:>12}")

# Primacy vs Federal
print(f"\n=== TABLE 11: Primacy vs Federal ===")
primacy_hex = sum(h for s, h in state_co2egs.items() if primacy_lookup.get(s, '').lower() == 'primacy')
federal_hex = sum(h for s, h in state_co2egs.items() if primacy_lookup.get(s, '').lower() in ['federal', ''])
pending_hex = sum(h for s, h in state_co2egs.items() if primacy_lookup.get(s, '').lower() == 'application')
total = primacy_hex + federal_hex + pending_hex

print(f"Primacy: {primacy_hex:,} hexes = {primacy_hex * HEX_SQ_KM:,.0f} km² ({100*primacy_hex/total if total else 0:.1f}%)")
print(f"Federal: {federal_hex:,} hexes = {federal_hex * HEX_SQ_KM:,.0f} km² ({100*federal_hex/total if total else 0:.1f}%)")
print(f"Pending: {pending_hex:,} hexes = {pending_hex * HEX_SQ_KM:,.0f} km² ({100*pending_hex/total if total else 0:.1f}%)")

# Save viable hex coordinates for distance analysis
viable_coords = [(lon, lat) for lon, lat, _, _ in viable_co2egs]

# Distance analysis
print(f"\n=== Distance Analysis ===")
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

all_em = []
for f in emitters['features']:
    naics = str(f['properties'].get('primary_naics_code', ''))
    if naics in NAICS_MAP:
        purity, sector = NAICS_MAP[naics]
        all_em.append((f['geometry']['coordinates'][0], f['geometry']['coordinates'][1], purity, sector))

for f in ethanol['features']:
    all_em.append((f['geometry']['coordinates'][0], f['geometry']['coordinates'][1], 'High', 'Ethanol'))

print(f"Total classified emitters: {len(all_em)}")

# Facilities by distance
bands = [(0, 5), (5, 15), (15, 25), (25, 50)]
fac_by_sector = defaultdict(lambda: {b: 0 for b in bands})
fac_total = defaultdict(int)

print("Calculating facility distances...")
for elon, elat, purity, sector in all_em:
    fac_total[sector] += 1
    min_dist = float('inf')
    for hlon, hlat in viable_coords:
        d = haversine(elon, elat, hlon, hlat)
        if d < min_dist:
            min_dist = d
    for lo, hi in bands:
        if lo <= min_dist < hi:
            fac_by_sector[sector][(lo, hi)] += 1
            break

print(f"\n=== TABLE 13: Facilities by Sector and Distance ===")
high_sectors = ['Natural Gas Processing', 'Ammonia/Fertilizer', 'Industrial Gas', 'Ethanol']
med_sectors = ['Petroleum Refining', 'Petrochemicals', 'Cement', 'Lime', 'Iron & Steel', 'Minerals']
low_sectors = ['Fossil Power', 'Landfills', 'Waste-to-Energy', 'Pulp & Paper']

print(f"{'Purity':<8} {'Sector':<25} {'0-5km':>8} {'5-15km':>8} {'15-25km':>8} {'25-50km':>8} {'TOTAL':>8}")
print("-" * 78)

for purity, sectors in [('High', high_sectors), ('Medium', med_sectors), ('Low', low_sectors)]:
    for sector in sectors:
        if sector not in fac_by_sector:
            continue
        d = fac_by_sector[sector]
        total = fac_total[sector]
        print(f"{purity:<8} {sector:<25} {d[(0,5)]:>8} {d[(5,15)]:>8} {d[(15,25)]:>8} {d[(25,50)]:>8} {total:>8}")
    
    # Subtotal
    t0 = sum(fac_by_sector.get(s, {}).get((0,5), 0) for s in sectors)
    t1 = sum(fac_by_sector.get(s, {}).get((5,15), 0) for s in sectors)
    t2 = sum(fac_by_sector.get(s, {}).get((15,25), 0) for s in sectors)
    t3 = sum(fac_by_sector.get(s, {}).get((25,50), 0) for s in sectors)
    tt = sum(fac_total.get(s, 0) for s in sectors)
    print(f"{purity:<8} {'SUBTOTAL':<25} {t0:>8} {t1:>8} {t2:>8} {t3:>8} {tt:>8}")
    print()

# Viable hexes by distance
print(f"\n=== TABLE 14: Viable Hexes by Distance and Purity ===")
hex_by_purity = {'High': {b: set() for b in bands}, 'Medium': {b: set() for b in bands}, 'Low': {b: set() for b in bands}}

for i, (hlon, hlat) in enumerate(viable_coords):
    for elon, elat, purity, sector in all_em:
        d = haversine(elon, elat, hlon, hlat)
        for lo, hi in bands:
            if lo <= d < hi:
                hex_by_purity[purity][(lo, hi)].add(i)
                break

print(f"{'Distance':<12} {'High Purity':<25} {'Medium Purity':<25} {'Low Purity':<25}")
print("-" * 90)
for b in bands:
    h = len(hex_by_purity['High'][b])
    m = len(hex_by_purity['Medium'][b])
    l = len(hex_by_purity['Low'][b])
    print(f"{b[0]}-{b[1]} km      {h:>5} hex ({h*HEX_SQ_KM:>8,.0f} km²)   {m:>5} hex ({m*HEX_SQ_KM:>8,.0f} km²)   {l:>5} hex ({l*HEX_SQ_KM:>8,.0f} km²)")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
