import json
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict

mesh = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
emitters = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson'))
ethanol = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson'))

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

viable = []
for f in mesh['features']:
    p = f['properties']
    t = p.get('avg_temperature_f')
    if t and (t-32)*5/9 >= 150:
        ring = f['geometry']['coordinates'][0]
        viable.append((sum(c[0] for c in ring)/len(ring), sum(c[1] for c in ring)/len(ring)))

all_em = []
for f in emitters['features']:
    naics = str(f['properties'].get('primary_naics_code',''))
    if naics in NAICS_MAP:
        purity, sector = NAICS_MAP[naics]
        all_em.append((f['geometry']['coordinates'][0], f['geometry']['coordinates'][1], purity, sector))

for f in ethanol['features']:
    all_em.append((f['geometry']['coordinates'][0], f['geometry']['coordinates'][1], 'High', 'Ethanol'))

bands = [(0,5), (5,15), (15,25), (25,50)]
sectors = set(s for _,_,_,s in all_em)
fac_by_sector = {s: {b: 0 for b in bands} for s in sectors}
fac_total_sector = {s: 0 for s in sectors}

for elon, elat, purity, sector in all_em:
    fac_total_sector[sector] += 1
    min_dist = 9999
    for j, (hlon, hlat) in enumerate(viable):
        d = haversine(elon, elat, hlon, hlat)
        if d < min_dist: min_dist = d
    for lo, hi in bands:
        if lo <= min_dist < hi:
            fac_by_sector[sector][(lo,hi)] += 1
            break

# Order sectors by purity
high_sectors = ['Natural Gas Processing', 'Ammonia/Fertilizer', 'Industrial Gas', 'Ethanol']
med_sectors = ['Petroleum Refining', 'Petrochemicals', 'Cement', 'Lime', 'Iron & Steel', 'Minerals']
low_sectors = ['Fossil Power', 'Landfills', 'Waste-to-Energy', 'Pulp & Paper']

print('=== FACILITIES BY SECTOR AND DISTANCE TO VIABLE CO2-EGS LAND ===')
print(f'{"Purity":<8} {"Sector":<25} {"0-5km":>8} {"5-15km":>8} {"15-25km":>8} {"25-50km":>8} {"TOTAL":>8}')
print('-'*78)

for purity, sector_list in [('High', high_sectors), ('Medium', med_sectors), ('Low', low_sectors)]:
    for sector in sector_list:
        if sector not in fac_by_sector: continue
        d = fac_by_sector[sector]
        total = fac_total_sector.get(sector, 0)
        print(f'{purity:<8} {sector:<25} {d[(0,5)]:>8} {d[(5,15)]:>8} {d[(15,25)]:>8} {d[(25,50)]:>8} {total:>8}')
    print()

# Subtotals by purity
print('-'*78)
for purity, sector_list in [('High', high_sectors), ('Medium', med_sectors), ('Low', low_sectors)]:
    t0 = sum(fac_by_sector.get(s,{}).get((0,5),0) for s in sector_list)
    t1 = sum(fac_by_sector.get(s,{}).get((5,15),0) for s in sector_list)
    t2 = sum(fac_by_sector.get(s,{}).get((15,25),0) for s in sector_list)
    t3 = sum(fac_by_sector.get(s,{}).get((25,50),0) for s in sector_list)
    tt = sum(fac_total_sector.get(s,0) for s in sector_list)
    print(f'{purity:<8} {"SUBTOTAL":<25} {t0:>8} {t1:>8} {t2:>8} {t3:>8} {tt:>8}')
