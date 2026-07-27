#!/usr/bin/env python3
"""
Diagnostic: characterize the blank vertical bands in the geothermal mesh.

Loads a generated mesh-{depth}m.json and analyzes where the null-temperature
hexagons are, to confirm whether blanks form periodic vertical (longitude)
bands and whether they correlate with hex-grid row parity.
"""
import json
import sys
from collections import defaultdict

MESH = sys.argv[1] if len(sys.argv) > 1 else \
    '/Users/devanagrawal/Desktop/project-seer/map/web/public/cache/geothermal/mesh-5000m.json'

with open(MESH) as f:
    gj = json.load(f)

feats = gj['features']
n = len(feats)
null_feats = [ft for ft in feats if ft['properties'].get('avg_temperature_f') is None]
filled = n - len(null_feats)
print(f"File: {MESH}")
print(f"Total hexagons : {n}")
print(f"Filled         : {filled} ({100*filled/n:.1f}%)")
print(f"Null/blank      : {len(null_feats)} ({100*len(null_feats)/n:.1f}%)")

# Bucket null-fraction by longitude column (0.1875 deg wide) to detect vertical bands
LNG_STEP = 0.1875
col_total = defaultdict(int)
col_null = defaultdict(int)
for ft in feats:
    # hexagon center longitude = first vertex angle 0 -> center + size, so use bbox center
    coords = ft['geometry']['coordinates'][0]
    lngs = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    cx = (min(lngs) + max(lngs)) / 2
    col = round(cx / LNG_STEP)
    col_total[col] += 1
    if ft['properties'].get('avg_temperature_f') is None:
        col_null[col] += 1

# Report longitude columns sorted, show null fraction to reveal banding
cols = sorted(col_total)
print("\nNull-fraction by longitude column (looking for alternating/periodic bands):")
print("col  center_lng  total  null  null%")
banded = []
for col in cols:
    t = col_total[col]
    nu = col_null[col]
    frac = nu / t if t else 0
    banded.append(frac)
    if t >= 3:  # skip sparse edge columns
        bar = '#' * int(frac * 40)
        print(f"{col:5d}  {col*LNG_STEP:9.3f}  {t:5d}  {nu:5d}  {100*frac:5.1f}  {bar}")

# Detect alternation: compare mean null-fraction of even vs odd columns
even = [col_null[c]/col_total[c] for c in cols if col % 2 == 0 and col_total[c] >= 3]
odd = [col_null[c]/col_total[c] for c in cols if col % 2 == 1 and col_total[c] >= 3]
if even and odd:
    me = sum(even)/len(even)
    mo = sum(odd)/len(odd)
    print(f"\nMean null-fraction: EVEN columns={me:.3f}  ODD columns={mo:.3f}  (large gap => aliasing bands)")
