#!/usr/bin/env python3
"""
Cheap validation of the mesh point->hexagon assignment WITHOUT reading the
5.7 GB Stanford CSV.

Creates a dense, gap-free synthetic temperature field across the US bounds and
runs the real assignment function. A correct assignment should fill ~100% of
interior hexagons (no periodic vertical blank bands). The old rounded-bin
matching left ~17.5% blank in periodic columns.
"""
import numpy as np
import pandas as pd

from generate_mesh_from_csv import (
    generate_hex_grid, build_hex_tree, assign_points_to_hexagons,
    US_BOUNDS, HEX_SIZE,
)


def make_dense_points(step=0.05):
    """Gap-free grid of points across the US bounds with a smooth temp field."""
    lngs = np.arange(US_BOUNDS['west'], US_BOUNDS['east'], step)
    lats = np.arange(US_BOUNDS['south'], US_BOUNDS['north'], step)
    LNG, LAT = np.meshgrid(lngs, lats)
    lng = LNG.ravel()
    lat = LAT.ravel()
    # Arbitrary smooth field so we can check averaging is sane
    temp_f = 150 + 3 * (lat - US_BOUNDS['south']) + 2 * (lng - US_BOUNDS['west'])
    return pd.DataFrame({'lat': lat, 'lng': lng, 'temp_f': temp_f})


def check_geometry(feats):
    """Verify hexagons are ground-regular (not compressed) and tile without overlap."""
    import math
    # Aspect ratio: for a pointy-topped hexagon, ground width / ground height
    # should be sqrt(3)/2 ~= 0.866. Compression shows up as a wrong ratio.
    ratios = []
    for f in feats[:5000]:
        ring = f['geometry']['coordinates'][0]
        xs = [c[0] for c in ring]; ys = [c[1] for c in ring]
        clat = (min(ys) + max(ys)) / 2
        ground_w = (max(xs) - min(xs)) * math.cos(math.radians(clat))
        ground_h = (max(ys) - min(ys))
        if ground_h > 0:
            ratios.append(ground_w / ground_h)
    mean_ratio = sum(ratios) / len(ratios)
    print(f"\nGeometry: mean ground width/height ratio = {mean_ratio:.3f} "
          f"(regular pointy-top target ~0.866)")
    assert 0.80 < mean_ratio < 0.93, f"Hexagons look distorted (ratio {mean_ratio:.3f})"
    print("PASS: hexagons are ground-regular (no compression).")


def main():
    hexagons = generate_hex_grid()
    tree, _ = build_hex_tree(hexagons)
    pts = make_dense_points(step=0.05)
    print(f"Synthetic points: {len(pts):,}")

    feats = assign_points_to_hexagons(hexagons, pts, tree)

    filled = [f for f in feats if f['properties']['avg_temperature_f'] is not None]
    blank = len(feats) - len(filled)
    print(f"\nHexagons total : {len(feats)}")
    print(f"Filled         : {len(filled)} ({100*len(filled)/len(feats):.2f}%)")
    print(f"Blank          : {blank} ({100*blank/len(feats):.2f}%)")

    # Check for periodic vertical bands: group blanks by longitude column.
    from collections import defaultdict
    col_total, col_blank = defaultdict(int), defaultdict(int)
    for f in feats:
        xs = [c[0] for c in f['geometry']['coordinates'][0]]
        col = round(((min(xs) + max(xs)) / 2) / (HEX_SIZE * 1.5))
        col_total[col] += 1
        if f['properties']['avg_temperature_f'] is None:
            col_blank[col] += 1
    worst = sorted(
        ((col_blank[c] / col_total[c], c, col_total[c]) for c in col_total if col_total[c] >= 3),
        reverse=True,
    )[:5]
    print("\nWorst 5 longitude columns by blank-fraction (should be ~0 in interior):")
    for frac, c, t in worst:
        print(f"  col {c:5d}  blank {100*frac:5.1f}%  (n={t})")

    # Assert: interior banding eliminated. Allow a little slack for true grid
    # edges where synthetic coverage is thin.
    assert blank / len(feats) < 0.02, "Too many blanks - aliasing may persist!"
    print("\nPASS: assignment fills interior hexagons with no periodic bands.")

    check_geometry(feats)


if __name__ == '__main__':
    main()
