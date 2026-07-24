#!/usr/bin/env python3
"""Generate thesis-ready CO2 emitter table."""
import json
from collections import defaultdict

# Load data
with open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson') as f:
    emitters = json.load(f)

with open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/ethanol_plants_with_emissions.geojson') as f:
    ethanol = json.load(f)

# Simplified NAICS mapping for thesis categories
CATEGORY_MAP = {
    '325193': 'Ethanol Fermentation', '311221': 'Ethanol Fermentation',
    '325311': 'Ammonia / Fertilizer', '325312': 'Ammonia / Fertilizer',
    '325120': 'Industrial Gas Production',
    '486210': 'Natural Gas Processing', '211130': 'Natural Gas Processing',
    '211111': 'Natural Gas Processing', '211112': 'Natural Gas Processing',
    '211120': 'Natural Gas Processing', '213112': 'Natural Gas Processing',
    '324110': 'Petroleum Refining', '324199': 'Petroleum Refining',
    '325110': 'Petrochemicals', '325199': 'Petrochemicals', '325211': 'Petrochemicals',
    '327310': 'Cement Manufacturing', '327410': 'Lime Manufacturing',
    '331110': 'Iron & Steel', '331511': 'Iron & Steel',
    '221112': 'Fossil Power Generation', '221118': 'Fossil Power Generation',
    '562212': 'Landfills', '562213': 'Waste-to-Energy',
    '322120': 'Pulp & Paper', '322130': 'Pulp & Paper', '322110': 'Pulp & Paper',
}

PURITY_MAP = {
    'Ethanol Fermentation': 'High (>90%)',
    'Ammonia / Fertilizer': 'High (>90%)',
    'Industrial Gas Production': 'High (>90%)',
    'Natural Gas Processing': 'High (>90%)',
    'Petroleum Refining': 'Medium-High',
    'Petrochemicals': 'Medium',
    'Cement Manufacturing': 'Medium',
    'Lime Manufacturing': 'Medium-High',
    'Iron & Steel': 'Medium',
    'Fossil Power Generation': 'Low (<15%)',
    'Landfills': 'Low (biogenic)',
    'Waste-to-Energy': 'Low (<15%)',
    'Pulp & Paper': 'Low (biogenic)',
}

# Aggregate
cats = defaultdict(lambda: {'count': 0, 'emissions': 0})
for f in emitters['features']:
    p = f['properties']
    code = str(p.get('primary_naics_code', ''))
    emissions = p.get('total_emissions_2023', 0) or 0
    cat = CATEGORY_MAP.get(code, 'Other Industrial')
    cats[cat]['count'] += 1
    cats[cat]['emissions'] += emissions

# Print markdown table
print("## Table: CO₂ Point Source Emitters in SEER by Category\n")
print("| Category | Facilities | 2023 Emissions (Mt) | Avg/Facility (kt) | CO₂ Purity |")
print("|----------|------------|---------------------|-------------------|------------|")

for cat, d in sorted(cats.items(), key=lambda x: -x[1]['emissions']):
    if d['emissions'] < 1e6:  # Skip tiny categories
        continue
    avg = d['emissions'] / d['count'] / 1000
    purity = PURITY_MAP.get(cat, 'Variable')
    print(f"| {cat} | {d['count']:,} | {d['emissions']/1e6:.1f} | {avg:.0f} | {purity} |")

print("| Natural CO₂ Domes | 12 | N/A | N/A | ~100% |")
print()

# Summary stats
total = sum(d['emissions'] for d in cats.values())
high_purity = sum(d['emissions'] for c, d in cats.items() 
                  if PURITY_MAP.get(c, '').startswith('High'))
print(f"**Total facilities:** {sum(d['count'] for d in cats.values()):,}")
print(f"**Total 2023 emissions:** {total/1e6:.1f} Mt CO₂")
print(f"**High-purity sources:** {high_purity/1e6:.1f} Mt CO₂ ({100*high_purity/total:.1f}%)")
print()

# High purity breakdown
print("### High-Purity CO₂ Sources (>90% CO₂, minimal capture cost)")
print()
for cat in ['Natural Gas Processing', 'Ammonia / Fertilizer', 'Industrial Gas Production', 'Ethanol Fermentation']:
    if cat in cats:
        d = cats[cat]
        print(f"- **{cat}:** {d['count']} facilities, {d['emissions']/1e6:.1f} Mt")
print("- **Natural CO₂ Domes:** 12 reservoirs (geological)")
