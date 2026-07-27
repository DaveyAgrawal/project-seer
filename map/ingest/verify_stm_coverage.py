#!/usr/bin/env python3
"""
Prove the blank vertical bands are an ASSIGNMENT ARTIFACT, not missing STM data.

Reads the real Stanford Thermal Model CSV for ONE depth, converts coordinates,
and checks STM point coverage at the exact longitude columns that come up
100%-blank in the deployed mesh. If STM has plenty of points there, the blanks
are purely a binning/aliasing bug and the fix fills them with REAL STM values.
"""
import numpy as np
import pandas as pd

CSV = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv'
DEPTH = 5000
LNG_STEP = 0.1875

# Longitude columns that were 100% blank in the deployed mesh-5000m.json
BLANK_LNGS = [-120.562, -114.562, -117.0, -116.0]


def merc_to_lnglat(easting, northing):
    lng = (easting / 20037508.34) * 180.0
    lat_m = (northing / 20037508.34) * 180.0
    lat = 180.0 / np.pi * (2.0 * np.arctan(np.exp(lat_m * np.pi / 180.0)) - np.pi / 2.0)
    return lng, lat


def main():
    print(f"Scanning STM CSV for depth={DEPTH}m (real data, single pass)...")
    lngs = []
    temps = []
    rows = 0
    for chunk in pd.read_csv(CSV, usecols=['Easting', 'Northing', 'Depth', 'T'],
                             chunksize=500000):
        d = chunk[chunk['Depth'] == DEPTH]
        if len(d):
            lng, _ = merc_to_lnglat(d['Easting'].to_numpy(), d['Northing'].to_numpy())
            lngs.append(lng)
            temps.append(d['T'].to_numpy())
            rows += len(d)
    lng = np.concatenate(lngs)
    temp = np.concatenate(temps)
    print(f"\nSTM points at {DEPTH}m: {rows:,}")
    print(f"Longitude span: {lng.min():.2f} to {lng.max():.2f}")
    print(f"Temperature (C) span: {np.nanmin(temp):.1f} to {np.nanmax(temp):.1f}")

    # Coverage check at the previously-blank longitude columns
    print("\nSTM coverage at longitudes that were 100% BLANK in the deployed mesh:")
    print("  target_lng   STM_points_in_+/-0.09deg   mean_T(F)")
    for L in BLANK_LNGS:
        m = np.abs(lng - L) <= (LNG_STEP / 2)
        n = int(m.sum())
        meanF = (np.nanmean(temp[m]) * 9 / 5 + 32) if n else float('nan')
        flag = 'STM HAS DATA -> blank was an artifact' if n > 0 else 'genuinely empty in STM'
        print(f"  {L:9.3f}   {n:24,}   {meanF:8.1f}   {flag}")

    # Show STM longitude coverage is continuous (no periodic vertical gaps)
    cols = np.round(lng / LNG_STEP).astype(int)
    uniq, counts = np.unique(cols, return_counts=True)
    empty_interior = 0
    lo, hi = uniq.min() + 2, uniq.max() - 2
    present = set(uniq.tolist())
    for c in range(lo, hi + 1):
        if c not in present:
            empty_interior += 1
    print(f"\nInterior longitude columns with ZERO STM points: {empty_interior} "
          f"(out of {hi - lo + 1}) -> STM coverage is {'CONTINUOUS' if empty_interior == 0 else 'gappy'}")


if __name__ == '__main__':
    main()
