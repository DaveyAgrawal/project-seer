import json
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict

mesh = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'))
geology = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/geology_simplified.geojson'))
emitters = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson'))
ethanol = json.load(open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson'))

def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

geo_grid = defaultdict(list)
for f in geology['features']:
    c = f['geometry']['coordinates']
    lon, lat = (c[0], c[1]) if f['geometry']['type'] == 'Point' else (sum(x[0] for x in c[0])/len(c[0]), sum(x[1] for x in c[0])/len(c[0]))
    dt = f['properties'].get('dt')
    if dt: geo_grid[(round(lon*2)/2, round(lat*2)/2)].append((lon, lat, dt))

def get_basement(lon, lat):
    candidates = []
    for dx in [-0.5,0,0.5]:
        for dy in [-0.5,0,0.5]:
            candidates.extend(geo_grid.get((round(lon*2)/2+dx, round(lat*2)/2+dy), []))
    if not candidates: return None
    nearest = min(candidates, key=lambda x: haversine(lon,lat,x[0],x[1]))
    return nearest[2] if haversine(lon,lat,nearest[0],nearest[1]) < 50 else None

viable = []
for f in mesh['features']:
    t = f['properties'].get('avg_temperature_f')
    if t and (t-32)*5/9 >= 150:
        ring = f['geometry']['coordinates'][0]
        lon, lat = sum(c[0] for c in ring)/len(ring), sum(c[1] for c in ring)/len(ring)
        b = get_basement(lon, lat)
        if b and b < 5000: viable.append((lon, lat))

NAICS = {
    '486210':('High','NGP'),'211130':('High','NGP'),'211111':('High','NGP'),'211112':('High','NGP'),'211120':('High','NGP'),'213112':('High','NGP'),
    '325311':('High','Ammonia'),'325312':('High','Ammonia'),'325120':('High','IndGas'),'325193':('High','Ethanol'),'311221':('High','Ethanol'),
    '324110':('Medium','Refining'),'324199':('Medium','Refining'),'327310':('Medium','Cement'),'327410':('Medium','Lime'),'327420':('Medium','Minerals'),
    '325110':('Medium','Petrochem'),'325199':('Medium','Petrochem'),'325211':('Medium','Petrochem'),'331110':('Medium','Steel'),'331511':('Medium','Steel'),
    '221112':('Low','Power'),'221118':('Low','Power'),'562212':('Low','Landfill'),'562213':('Low','WtE'),'322110':('Low','Pulp'),'322120':('Low','Pulp'),'322130':('Low','Pulp'),
}

all_em = [(f['geometry']['coordinates'][0],f['geometry']['coordinates'][1],*NAICS[str(f['properties'].get('primary_naics_code',''))]) 
          for f in emitters['features'] if str(f['properties'].get('primary_naics_code','')) in NAICS]
all_em += [(f['geometry']['coordinates'][0],f['geometry']['coordinates'][1],'High','Ethanol') for f in ethanol['features']]

hex_by_sector = defaultdict(set)
for i,(hlon,hlat) in enumerate(viable):
    for elon,elat,pur,sec in all_em:
        if haversine(elon,elat,hlon,hlat) <= 25:
            hex_by_sector[sec].add(i)

print('Sector,Hexes,Area_1000km2')
for sec in ['NGP','Ammonia','IndGas','Ethanol','Refining','Petrochem','Cement','Steel','Lime','Minerals','Power','Landfill','Pulp','WtE']:
    if sec in hex_by_sector:
        n = len(hex_by_sector[sec])
        print(f'{sec},{n},{n*168.22/1000:.1f}')
