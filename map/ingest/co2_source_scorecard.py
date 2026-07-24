#!/usr/bin/env python3
"""
Master Site Scorecard -- CO2-source-oriented.

One row per CO2 point source (the binding constraint for CO2-EGS deployment).
For each source we measure OUTWARD to:
  - the nearest developable geothermal resource
    (>=150 C reached at <=5 km depth AND crystalline basement <5 km)
  - CO2 pipeline proximity (within 30 km flag)
  - nearest high-voltage transmission line

All distances are exact great-circle distances computed via a KD-tree built on
ECEF (earth-centered) unit-sphere coordinates; the chord distance returned by the
tree is converted back to a great-circle arc length. No planar/degree math is used
for distances, so results are correct anywhere in CONUS.

Sources:
  - EPA FLIGHT emitters (emitters.geojson), classified High/Medium by NAICS
  - Ethanol plants (ethanol_plants_with_emissions.geojson), deduped vs FLIGHT
  - Natural CO2 domes (natural_co2_reservoirs_CONUS.xlsx)

Output: co2_source_scorecard.csv (sorted by composite score desc)
"""
import json
import csv
import math
import os
import urllib.request
from collections import defaultdict

import numpy as np
from scipy.spatial import cKDTree
from shapely.geometry import shape, Point
from shapely.strtree import STRtree
import openpyxl

BASE = "/Users/devanagrawal/Desktop/project-seer/map/web/public"
CACHE = f"{BASE}/cache/geothermal"
DATA = f"{BASE}/data"
OUT = "/Users/devanagrawal/Desktop/project-seer/map/ingest/co2_source_scorecard.csv"

R_KM = 6371.0088  # mean Earth radius (km)
CO2_PER_MW_YR = 5550.0  # tCO2/MW/yr (midpoint of 5,500-5,600)

# ---------------------------------------------------------------------------
# Great-circle helpers (exact)
# ---------------------------------------------------------------------------

def lonlat_to_unit_xyz(lon, lat):
    """Convert arrays of lon/lat (degrees) to unit-sphere ECEF xyz."""
    lon = np.radians(np.asarray(lon, dtype=float))
    lat = np.radians(np.asarray(lat, dtype=float))
    cl = np.cos(lat)
    x = cl * np.cos(lon)
    y = cl * np.sin(lon)
    z = np.sin(lat)
    return np.column_stack([x, y, z])


def chord_to_arc_km(chord):
    """Convert a Euclidean chord length on the unit sphere to arc length (km)."""
    chord = np.clip(np.asarray(chord, dtype=float), 0.0, 2.0)
    return R_KM * 2.0 * np.arcsin(chord / 2.0)


def haversine_km(lon1, lat1, lon2, lat2):
    """Scalar great-circle distance for verification."""
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return R_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def densify_line(coords, step_km=2.0):
    """Yield densified (lon,lat) points along a lon/lat polyline so that a KD-tree
    of vertices approximates true point-to-line distance to within ~step_km/2."""
    pts = []
    for i in range(len(coords) - 1):
        lon1, lat1 = coords[i][0], coords[i][1]
        lon2, lat2 = coords[i + 1][0], coords[i + 1][1]
        pts.append((lon1, lat1))
        seg = haversine_km(lon1, lat1, lon2, lat2)
        n = int(seg // step_km)
        for k in range(1, n + 1):
            t = k / (n + 1)
            pts.append((lon1 + (lon2 - lon1) * t, lat1 + (lat2 - lat1) * t))
    if coords:
        pts.append((coords[-1][0], coords[-1][1]))
    return pts


# ---------------------------------------------------------------------------
# NAICS classification (High + Medium only per scope)
# ---------------------------------------------------------------------------
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
}


def plant_size_class(mw):
    if mw is None:
        return ''
    if mw >= 100:
        return '>=100 MW'
    if mw >= 50:
        return '50-100 MW'
    if mw >= 20:
        return '20-50 MW'
    if mw >= 10:
        return '10-20 MW'
    if mw >= 1:
        return '1-10 MW'
    return '<1 MW'


# ---------------------------------------------------------------------------
# 1. Load geothermal meshes and build the viable-resource set
# ---------------------------------------------------------------------------
print("Loading geothermal meshes...")
DEPTHS = [2000, 3000, 4500, 5000]  # skip 4000m (corrupt hex_ids)


def feature_centroid(f):
    g = f['geometry']
    ring = g['coordinates'][0] if g['type'] == 'Polygon' else g['coordinates'][0][0]
    return sum(c[0] for c in ring) / len(ring), sum(c[1] for c in ring) / len(ring)


def ckey(lon, lat):
    # centroid key -- all depth meshes share the same hex grid, so rounding the
    # centroid to ~100 m gives a robust join across depths (hex_id is unreliable:
    # many features are 'hex_undefined').
    return (round(lon, 3), round(lat, 3))


temp_by_depth = {}
for dp in DEPTHS:
    d = json.load(open(f"{CACHE}/mesh-{dp}m.json"))
    m = {}
    for f in d['features']:
        tf = f['properties'].get('avg_temperature_f')
        if tf is not None:
            lon, lat = feature_centroid(f)
            m[ckey(lon, lat)] = (tf - 32) * 5.0 / 9.0
    temp_by_depth[dp] = m
mesh5 = json.load(open(f"{CACHE}/mesh-5000m.json"))
print(f"  mesh-5000m features: {len(mesh5['features'])}")

# geology (depth to basement)
print("Loading geology (depth to basement)...")
geology = json.load(open(f"{DATA}/geology_simplified.geojson"))
geo_grid = defaultdict(list)
for f in geology['features']:
    g = f['geometry']
    if g['type'] == 'Point':
        lon, lat = g['coordinates'][0], g['coordinates'][1]
    else:
        ring = g['coordinates'][0] if g['type'] == 'Polygon' else g['coordinates'][0][0]
        lon = sum(c[0] for c in ring) / len(ring)
        lat = sum(c[1] for c in ring) / len(ring)
    dt = f['properties'].get('dt')
    if dt is not None:
        geo_grid[(round(lon * 2) / 2, round(lat * 2) / 2)].append((lon, lat, dt))


def basement_depth(lon, lat):
    key = (round(lon * 2) / 2, round(lat * 2) / 2)
    best_d, best_dt = float('inf'), None
    for dx in (-0.5, 0, 0.5):
        for dy in (-0.5, 0, 0.5):
            for glon, glat, dt in geo_grid.get((key[0] + dx, key[1] + dy), ()):  # noqa
                d = haversine_km(lon, lat, glon, glat)
                if d < best_d:
                    best_d, best_dt = d, dt
    return best_dt if best_d < 50 else None


def depth_to_150c(lon, lat):
    """Linear interpolation of the depth (m) at which 150 C is first reached.
    Uses an ambient surface anchor (0 m, 15 C) only if 150 C is already reached
    at the shallowest sampled depth."""
    k = ckey(lon, lat)
    profile = [(0.0, 15.0)]
    for dp in DEPTHS:
        t = temp_by_depth[dp].get(k)
        if t is not None:
            profile.append((float(dp), t))
    profile.sort()
    for i in range(1, len(profile)):
        d0, t0 = profile[i - 1]
        d1, t1 = profile[i]
        if t1 >= 150.0 >= t0:
            if t1 == t0:
                return round(d1)
            return round(d0 + (d1 - d0) * (150.0 - t0) / (t1 - t0))
    return None


print("Building viable-resource set (>=150 C @ <=5 km, basement <5 km)...")
viable = []  # dict per hex
for f in mesh5['features']:
    p = f['properties']
    tf = p.get('avg_temperature_f')
    if tf is None:
        continue
    temp_c = (tf - 32) * 5.0 / 9.0
    if temp_c < 150.0:
        continue
    ring = f['geometry']['coordinates'][0]
    lon = sum(c[0] for c in ring) / len(ring)
    lat = sum(c[1] for c in ring) / len(ring)
    bm = basement_depth(lon, lat)
    if bm is None or bm >= 5000:
        continue
    viable.append({
        'hex_id': p.get('hex_id'),
        'lon': lon, 'lat': lat,
        'temp_c': round(temp_c, 1),
        'depth_150c_m': depth_to_150c(lon, lat),
        'basement_m': round(bm),
        'sui': p.get('sui'),
        'sui_class': p.get('sui_class'),
    })
print(f"  viable hexes: {len(viable)}")

viable_xyz = lonlat_to_unit_xyz([v['lon'] for v in viable], [v['lat'] for v in viable])
viable_tree = cKDTree(viable_xyz)

# ---------------------------------------------------------------------------
# 2. Assign state + primacy and lithology to each viable hex
# ---------------------------------------------------------------------------
print("Assigning state + primacy to viable hexes...")
states_path = "/tmp/us-states.geojson"
if not os.path.exists(states_path):
    print("  downloading US state boundaries...")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json",
        states_path,
    )
states_geo = json.load(open(states_path))
state_polys, state_names = [], []
for f in states_geo['features']:
    nm = f['properties'].get('name')
    if nm:
        state_polys.append(shape(f['geometry']))
        state_names.append(nm)
state_tree = STRtree(state_polys)

primacy_raw = json.load(open(f"{DATA}/class_vi_primacy.json"))['data']
primacy_lookup = {v['state']: v['primacy_status'] for v in primacy_raw.values()}


def state_of(lon, lat):
    pt = Point(lon, lat)
    for idx in state_tree.query(pt):
        if state_polys[idx].contains(pt):
            return state_names[idx]
    return None


print("Loading lithology polygons...")
litho = json.load(open(f"{DATA}/bedrock_lithology.geojson"))
litho_polys, litho_cat, litho_rock = [], [], []
for f in litho['features']:
    try:
        litho_polys.append(shape(f['geometry']))
    except Exception:
        continue
    litho_cat.append(f['properties'].get('category', ''))
    litho_rock.append(f['properties'].get('rock_type', ''))
litho_tree = STRtree(litho_polys)


def lithology_of(lon, lat):
    pt = Point(lon, lat)
    for idx in litho_tree.query(pt):
        if litho_polys[idx].contains(pt):
            return litho_cat[idx], litho_rock[idx]
    return '', ''


for i, v in enumerate(viable):
    st = state_of(v['lon'], v['lat'])
    v['state'] = st or 'Unknown'
    v['primacy'] = primacy_lookup.get(st, 'federal') if st else 'unknown'
    v['litho_cat'], v['litho_rock'] = lithology_of(v['lon'], v['lat'])
    if (i + 1) % 2000 == 0:
        print(f"  ...{i + 1}/{len(viable)} hexes attributed")

# ---------------------------------------------------------------------------
# 3. Build pipeline & transmission KD-trees (densified vertices)
# ---------------------------------------------------------------------------
print("Building CO2 pipeline KD-tree...")
pipes = json.load(open(f"{DATA}/co2_pipelines.geojson"))
pipe_pts = []
for f in pipes['features']:
    g = f['geometry']
    if g['type'] == 'LineString':
        pipe_pts.extend(densify_line(g['coordinates'], 2.0))
    elif g['type'] == 'MultiLineString':
        for ls in g['coordinates']:
            pipe_pts.extend(densify_line(ls, 2.0))
pipe_xyz = lonlat_to_unit_xyz([p[0] for p in pipe_pts], [p[1] for p in pipe_pts])
pipe_tree = cKDTree(pipe_xyz)
print(f"  pipeline vertices (densified): {len(pipe_pts)}")

print("Building transmission KD-tree...")
trans = json.load(open(f"{DATA}/transmission_lines.geojson"))
tr_pts, tr_kv, tr_vc = [], [], []
for f in trans['features']:
    g = f['geometry']
    kv = f['properties'].get('kv')
    vc = f['properties'].get('volt_class', '')
    lines = [g['coordinates']] if g['type'] == 'LineString' else (g['coordinates'] if g['type'] == 'MultiLineString' else [])
    for ls in lines:
        dp = densify_line(ls, 3.0)
        tr_pts.extend(dp)
        tr_kv.extend([kv] * len(dp))
        tr_vc.extend([vc] * len(dp))
tr_xyz = lonlat_to_unit_xyz([p[0] for p in tr_pts], [p[1] for p in tr_pts])
tr_tree = cKDTree(tr_xyz)
tr_kv = np.array([k if k is not None else np.nan for k in tr_kv], dtype=float)
print(f"  transmission vertices (densified): {len(tr_pts)}")

# ---------------------------------------------------------------------------
# 4. Assemble the CO2 source list
# ---------------------------------------------------------------------------
print("Assembling CO2 sources...")
sources = []  # dict per source

emitters = json.load(open(f"{DATA}/emitters.geojson"))
flight_ethanol = []  # (lon,lat) for dedupe vs EIA ethanol
for f in emitters['features']:
    p = f['properties']
    naics = str(p.get('primary_naics_code', ''))
    if naics not in NAICS_MAP:
        continue
    purity, sector = NAICS_MAP[naics]
    lon, lat = f['geometry']['coordinates'][0], f['geometry']['coordinates'][1]
    em = p.get('total_emissions_2023')
    sources.append({
        'name': p.get('facility_name', 'Unknown'),
        'type': 'FLIGHT',
        'sector': sector, 'purity': purity, 'naics': naics,
        'lon': lon, 'lat': lat,
        'src_state': p.get('state', ''),
        'emissions': em, 'emissions_est': False,
        'co2_purity_pct': '',
    })
    if sector == 'Ethanol':
        flight_ethanol.append((lon, lat))

# EIA ethanol plants (estimated CO2), deduped vs FLIGHT ethanol within 2 km
ETHANOL_T_PER_MMGAL = 2860.0  # ~2.86 kg CO2 per gal ethanol (fermentation stoich)
eth = json.load(open(f"{DATA}/ethanol_plants_with_emissions.geojson"))
added_eth = 0
for f in eth['features']:
    p = f['properties']
    lon = p.get('Longitude') or f['geometry']['coordinates'][0]
    lat = p.get('Latitude') or f['geometry']['coordinates'][1]
    if any(haversine_km(lon, lat, flon, flat) < 2.0 for flon, flat in flight_ethanol):
        continue  # already represented by a FLIGHT ethanol emitter
    cap = p.get('Cap_Mmgal')
    em = p.get('emissions_mt_co2e')
    est = False
    if em is None and cap is not None:
        em = round(cap * ETHANOL_T_PER_MMGAL)
        est = True
    sources.append({
        'name': p.get('Company', 'Ethanol Plant') + (f" ({p.get('Site')})" if p.get('Site') else ''),
        'type': 'Ethanol(EIA)',
        'sector': 'Ethanol', 'purity': 'High', 'naics': '325193',
        'lon': lon, 'lat': lat,
        'src_state': p.get('State', ''),
        'emissions': em, 'emissions_est': est,
        'co2_purity_pct': '',
    })
    added_eth += 1

# Natural CO2 domes (from xlsx); emissions N/A (geologic reservoirs)
wb = openpyxl.load_workbook(f"{DATA}/natural_co2_reservoirs_CONUS.xlsx")
ws = wb.active
rows = list(ws.iter_rows(values_only=True))
header = rows[0]
col = {h: i for i, h in enumerate(header)}
n_domes = 0
for r in rows[1:]:
    if r[col['Approx Lat']] is None or r[col['Approx Lon']] is None:
        continue
    sources.append({
        'name': r[col['Field Name']],
        'type': 'Natural Dome',
        'sector': 'Natural CO2 Dome', 'purity': 'High', 'naics': '',
        'lon': float(r[col['Approx Lon']]), 'lat': float(r[col['Approx Lat']]),
        'src_state': r[col['State']] or '',
        'emissions': None, 'emissions_est': False,
        'co2_purity_pct': r[col['CO2 Purity (%)']],
    })
    n_domes += 1

print(f"  FLIGHT (High+Medium): {sum(1 for s in sources if s['type']=='FLIGHT')}")
print(f"  Ethanol (EIA, deduped): {added_eth}")
print(f"  Natural domes: {n_domes}")
print(f"  TOTAL sources: {len(sources)}")

# ---------------------------------------------------------------------------
# 5. Compute distances (exact great-circle via KD-trees)
# ---------------------------------------------------------------------------
print("Computing distances...")
src_xyz = lonlat_to_unit_xyz([s['lon'] for s in sources], [s['lat'] for s in sources])

# nearest viable resource
vd_chord, vd_idx = viable_tree.query(src_xyz, k=1)
vd_km = chord_to_arc_km(vd_chord)

# nearest CO2 pipeline
pd_chord, _ = pipe_tree.query(src_xyz, k=1)
pd_km = chord_to_arc_km(pd_chord)

# nearest transmission
td_chord, td_idx = tr_tree.query(src_xyz, k=1)
td_km = chord_to_arc_km(td_chord)

# ---------------------------------------------------------------------------
# 6. Compose rows + composite score
# ---------------------------------------------------------------------------
PURITY_W = {'High': 30.0, 'Medium': 12.0}
PRIMACY_W = {'primacy': 25.0, 'application': 10.0, 'federal': 0.0, 'unknown': 0.0}

rows_out = []
for i, s in enumerate(sources):
    v = viable[vd_idx[i]]
    dist_v = round(float(vd_km[i]), 2)
    dist_p = round(float(pd_km[i]), 2)
    dist_t = round(float(td_km[i]), 2)

    em = s['emissions']
    mw = round(em / CO2_PER_MW_YR, 1) if em else None

    # composite score (transparent, higher = better first-project candidate)
    score = 0.0
    score += PURITY_W.get(s['purity'], 0.0)
    if mw:
        score += min(mw / 2.0, 40.0)            # volume (MW), capped
    score += max(0.0, 40.0 - dist_v)             # proximity to hot rock (0 at >=40 km)
    score += (v['temp_c'] - 150.0) * 0.15        # site temperature bonus
    score += PRIMACY_W.get(v['primacy'], 0.0)    # site regulatory readiness
    if dist_p <= 30.0:
        score += 10.0                            # existing pipeline access
    rows_out.append({
        'source_name': s['name'],
        'source_type': s['type'],
        'sector': s['sector'],
        'purity_tier': s['purity'],
        'source_co2_purity_pct': s['co2_purity_pct'],
        'naics': s['naics'],
        'emissions_tpy_2023': round(em) if em else '',
        'emissions_estimated': s['emissions_est'],
        'mw_potential': mw if mw is not None else '',
        'plant_size_class': plant_size_class(mw),
        'source_lat': round(s['lat'], 5),
        'source_lon': round(s['lon'], 5),
        'source_state': s['src_state'],
        'dist_to_viable_km': dist_v,
        'viable_within_50km': dist_v <= 50.0,
        'site_lat': round(v['lat'], 5),
        'site_lon': round(v['lon'], 5),
        'site_state': v['state'],
        'site_class_vi_primacy': v['primacy'],
        'site_temp_c_at_5km': v['temp_c'],
        'site_depth_to_150c_m': v['depth_150c_m'] if v['depth_150c_m'] is not None else '',
        'site_basement_depth_m': v['basement_m'],
        'site_lithology_category': v['litho_cat'],
        'site_lithology_rock_type': v['litho_rock'],
        'site_water_sui': v['sui'],
        'site_water_sui_class': v['sui_class'],
        'within_30km_co2_pipeline': dist_p <= 30.0,
        'dist_to_co2_pipeline_km': dist_p,
        'nearest_transmission_km': dist_t,
        'transmission_kv': '' if np.isnan(tr_kv[td_idx[i]]) else int(tr_kv[td_idx[i]]),
        'transmission_volt_class': tr_vc[td_idx[i]],
        'composite_score': round(score, 1),
    })

rows_out.sort(key=lambda r: r['composite_score'], reverse=True)

with open(OUT, 'w', newline='') as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows_out[0].keys()))
    w.writeheader()
    w.writerows(rows_out)
print(f"\nSaved {len(rows_out)} rows -> {OUT}")

# ---------------------------------------------------------------------------
# 7. Summary + a self-check on distance correctness
# ---------------------------------------------------------------------------
print("\n" + "=" * 90)
print("SUMMARY")
print("=" * 90)
high = [r for r in rows_out if r['purity_tier'] == 'High']
high_50 = [r for r in high if r['viable_within_50km']]
high_25 = [r for r in high if r['dist_to_viable_km'] <= 25]
high_5 = [r for r in high if r['dist_to_viable_km'] <= 5]
print(f"High-purity sources: {len(high)}")
print(f"  with viable rock within 50 km: {len(high_50)}")
print(f"  within 25 km: {len(high_25)}")
print(f"  within 5 km (effective co-location): {len(high_5)}")

print("\nCo-located (<=5 km) high-purity by sector:")
byc = defaultdict(int)
for r in high_5:
    byc[r['sector']] += 1
for k, n in sorted(byc.items(), key=lambda x: -x[1]):
    print(f"  {k:<28} {n}")

print("\nTop 15 first-project candidates:")
print(f"{'Score':>6} {'Purity':<7} {'Sector':<22} {'MW':>7} {'d_rock':>7} {'Temp':>6} {'Primacy':<11} {'Pipe30':<7} Source")
for r in rows_out[:15]:
    print(f"{r['composite_score']:>6} {r['purity_tier']:<7} {r['sector'][:22]:<22} {str(r['mw_potential']):>7} "
          f"{r['dist_to_viable_km']:>7} {r['site_temp_c_at_5km']:>6} {r['site_class_vi_primacy']:<11} "
          f"{str(r['within_30km_co2_pipeline']):<7} {r['source_name'][:34]}")

# distance self-check: recompute a few with independent haversine to nearest viable
print("\nDistance self-check (KD-tree arc vs brute-force haversine, first 5 sources):")
vlon = np.array([v['lon'] for v in viable]); vlat = np.array([v['lat'] for v in viable])
for i in range(5):
    s = sources[i]
    brute = min(haversine_km(s['lon'], s['lat'], vlon[j], vlat[j]) for j in range(len(viable)))
    kd = float(vd_km[i])
    print(f"  {s['name'][:30]:<30} kd={kd:8.3f} km  brute={brute:8.3f} km  diff={abs(kd-brute):.4f}")
print("\nDONE.")
