import json
from math import radians, sin, cos, sqrt, atan2
import csv

# Load data
mesh = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
emitters = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson'))
ethanol = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson'))

NAICS_HIGH = {'486210','211130','211111','211112','211120','213112','325311','325312','325120','325193','311221'}

def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

# Build high-purity emitter list
high_emitters = []
for f in emitters['features']:
    naics = str(f['properties'].get('primary_naics_code', ''))
    if naics in NAICS_HIGH:
        high_emitters.append((f['geometry']['coordinates'][0], f['geometry']['coordinates'][1]))

for f in ethanol['features']:
    high_emitters.append((f['geometry']['coordinates'][0], f['geometry']['coordinates'][1]))

print(f"High-purity emitters: {len(high_emitters)}")

# Get all viable hexes with temperature
viable = []
for f in mesh['features']:
    p = f['properties']
    temp_f = p.get('avg_temperature_f')
    basement = p.get('basement_depth_m')
    if temp_f and (temp_f-32)*5/9 >= 150 and (basement is None or basement < 5000):
        ring = f['geometry']['coordinates'][0]
        lon = sum(c[0] for c in ring) / len(ring)
        lat = sum(c[1] for c in ring) / len(ring)
        temp_c = (temp_f - 32) * 5/9
        viable.append({'lon': lon, 'lat': lat, 'temp_c': temp_c, 'basement': basement})

print(f"Viable hexes: {len(viable)}")

# Calculate top decile temperature threshold
temps = sorted([v['temp_c'] for v in viable], reverse=True)
top_decile_threshold = temps[len(temps)//10]
print(f"Top decile temp threshold: {top_decile_threshold:.1f}°C")

# Find orphaned resources: top-decile heat, NO high-purity CO2 within 50km
orphaned = []
top_decile_total = 0
orphaned_count = 0

for v in viable:
    if v['temp_c'] < top_decile_threshold:
        continue
    top_decile_total += 1
    
    # Check if any high-purity emitter within 50km
    has_co2 = False
    for elon, elat in high_emitters:
        if haversine(v['lon'], v['lat'], elon, elat) <= 50:
            has_co2 = True
            break
    
    if not has_co2:
        orphaned_count += 1
        orphaned.append(v)

print(f"\n=== ORPHANED RESOURCE ANALYSIS ===")
print(f"Top-decile hexes (≥{top_decile_threshold:.1f}°C): {top_decile_total}")
print(f"Orphaned (no high-purity CO2 within 50km): {orphaned_count}")
print(f"Orphaned percentage: {100*orphaned_count/top_decile_total:.1f}%")
print(f"Orphaned area: {orphaned_count * 65:,} km²")
print(f"Orphaned capacity: {orphaned_count * 100 / 1000:.1f} GW")

# Also calculate for ALL viable land
all_orphaned = 0
for v in viable:
    has_co2 = False
    for elon, elat in high_emitters:
        if haversine(v['lon'], v['lat'], elon, elat) <= 50:
            has_co2 = True
            break
    if not has_co2:
        all_orphaned += 1

print(f"\n=== ALL VIABLE LAND ===")
print(f"Total viable hexes: {len(viable)}")
print(f"No high-purity CO2 within 50km: {all_orphaned}")
print(f"Orphaned percentage: {100*all_orphaned/len(viable):.1f}%")
print(f"Orphaned area: {all_orphaned * 65:,} km²")
print(f"Orphaned capacity: {all_orphaned * 100 / 1000:.1f} GW")

# Save orphaned locations
orphaned.sort(key=lambda x: x['temp_c'], reverse=True)
with open('/Users/devanagrawal/Desktop/project-seer/map/ingest/orphaned_resources.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['lat', 'lon', 'temp_c', 'basement'])
    writer.writeheader()
    for o in orphaned:
        writer.writerow({'lat': round(o['lat'], 4), 'lon': round(o['lon'], 4), 'temp_c': round(o['temp_c'], 1), 'basement': o['basement']})

print(f"\nSaved orphaned_resources.csv")
