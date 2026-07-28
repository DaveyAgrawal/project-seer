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

from pyproj import Geod

from generate_mesh_from_csv import (
    generate_hex_grid, build_hex_tree, assign_points_to_hexagons,
    US_BOUNDS, HEX_AREA_SQMI,
)

_GEOD = Geod(ellps="WGS84")


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


def check_equal_area(feats):
    """Every cell must have the same true (geodesic) area == the stored constant."""
    areas = []
    for f in feats[::37][:4000]:   # sample
        ring = f['geometry']['coordinates'][0]
        lngs = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        area_m2, _ = _GEOD.polygon_area_perimeter(lngs, lats)
        areas.append(abs(area_m2) / 2.589988e6)   # m^2 -> sq miles
    areas = np.array(areas)
    rel_spread = (areas.max() - areas.min()) / areas.mean()
    print(f"\nEqual-area: geodesic area mean={areas.mean():.2f} sq mi "
          f"(stored {HEX_AREA_SQMI:.2f}), spread={100*rel_spread:.2f}%")
    assert abs(areas.mean() - HEX_AREA_SQMI) / HEX_AREA_SQMI < 0.03, "Area drifts from stored value"
    assert rel_spread < 0.03, "Cell areas vary -> tiling not equal-area"
    stored = {f['properties']['area_sq_miles'] for f in feats}
    assert len(stored) == 1, f"Multiple stored areas: {stored}"
    print("PASS: all cells identical area (valid for area/GW calculations).")


def check_no_gaps(hexagons, feats):
    """Every interior hexagon (well inside the sampled region) must be filled.

    Detects aliasing/banding, which would show up as missing interior cells.
    """
    filled_ids = {f['id'] for f in feats}
    m = 0.6
    interior = [h for h in hexagons
                if US_BOUNDS['west'] + m < h['center_lng'] < US_BOUNDS['east'] - m
                and US_BOUNDS['south'] + m < h['center_lat'] < US_BOUNDS['north'] - m]
    missing = [h for h in interior if h['id'] not in filled_ids]
    frac = len(missing) / len(interior)
    print(f"\nCoverage: interior cells={len(interior)}, missing={len(missing)} "
          f"({100*frac:.2f}%) -> no periodic bands")
    assert frac < 0.02, "Interior cells missing -> aliasing/banding present!"
    print("PASS: full interior coverage, no periodic bands.")


def main():
    hexagons = generate_hex_grid()
    tree, _ = build_hex_tree(hexagons)
    pts = make_dense_points(step=0.05)
    print(f"Synthetic points: {len(pts):,}")

    feats = assign_points_to_hexagons(hexagons, pts, tree)
    print(f"\nHexagons generated : {len(hexagons)}")
    print(f"Cells with data    : {len(feats)}")

    check_no_gaps(hexagons, feats)
    check_equal_area(feats)


if __name__ == '__main__':
    main()
