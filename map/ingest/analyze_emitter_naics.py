#!/usr/bin/env python3
"""
Analyze EPA FLIGHT emitters by NAICS code to create a CO2-EGS optimized taxonomy.
Classifies emitters by CO2 stream type and purity rather than EPA sector.
"""

import json
from collections import defaultdict

# NAICS code to CO2 stream classification
# Format: NAICS -> (Industry Description, CO2 Stream Type, Category, Purity Level)
NAICS_CLASSIFICATION = {
    # === HIGH PURITY (>90% CO2) - Minimal capture cost ===
    '325193': ('Ethyl Alcohol (Ethanol) Manufacturing', 'Fermentation CO2', 'Ethanol Fermentation', 'HIGH'),
    '325311': ('Nitrogenous Fertilizer Manufacturing', 'Ammonia synthesis CO2', 'Ammonia / Fertilizer', 'HIGH'),
    '325312': ('Phosphatic Fertilizer Manufacturing', 'Process CO2', 'Ammonia / Fertilizer', 'HIGH'),
    '325120': ('Industrial Gas Manufacturing', 'Process CO2 / SMR', 'Industrial Gas Production', 'HIGH'),
    '486210': ('Pipeline Transportation of Natural Gas', 'Acid gas removal CO2', 'Natural Gas Processing', 'HIGH'),
    '211130': ('Natural Gas Extraction', 'Acid gas removal CO2', 'Natural Gas Processing', 'HIGH'),
    '211111': ('Crude Petroleum and Natural Gas Extraction', 'Acid gas removal CO2', 'Natural Gas Processing', 'HIGH'),
    '211112': ('Natural Gas Liquid Extraction', 'Acid gas removal CO2', 'Natural Gas Processing', 'HIGH'),
    '211120': ('Crude Petroleum Extraction', 'Acid gas removal CO2', 'Natural Gas Processing', 'HIGH'),
    '213112': ('Support Activities for Oil and Gas Operations', 'Acid gas removal CO2', 'Natural Gas Processing', 'HIGH'),
    
    # === MEDIUM-HIGH PURITY (50-90% CO2) ===
    '324110': ('Petroleum Refineries', 'SMR hydrogen / FCC regen', 'Petroleum Refining', 'MEDIUM-HIGH'),
    '324199': ('All Other Petroleum and Coal Products', 'Process CO2', 'Petroleum Refining', 'MEDIUM'),
    '325110': ('Petrochemical Manufacturing', 'Process CO2 / SMR', 'Petrochemicals', 'MEDIUM-HIGH'),
    '325199': ('All Other Basic Organic Chemical Mfg', 'Process CO2', 'Petrochemicals', 'MEDIUM'),
    '325211': ('Plastics Material and Resin Manufacturing', 'Process CO2', 'Petrochemicals', 'MEDIUM'),
    '325180': ('Other Basic Inorganic Chemical Mfg', 'Process CO2', 'Chemicals - Other', 'MEDIUM'),
    '325130': ('Synthetic Dye and Pigment Manufacturing', 'Process CO2', 'Chemicals - Other', 'MEDIUM'),
    '325194': ('Cyclic Crude, Intermediate, and Gum Mfg', 'Process CO2', 'Petrochemicals', 'MEDIUM'),
    '325212': ('Synthetic Rubber Manufacturing', 'Process CO2', 'Petrochemicals', 'MEDIUM'),
    '325998': ('All Other Miscellaneous Chemical Mfg', 'Process CO2', 'Chemicals - Other', 'MEDIUM'),
    
    # === MEDIUM PURITY (15-50% CO2) - Calcination/process ===
    '327310': ('Cement Manufacturing', 'Calcination + combustion', 'Cement Manufacturing', 'MEDIUM'),
    '327410': ('Lime Manufacturing', 'Calcination CO2', 'Lime Manufacturing', 'MEDIUM-HIGH'),
    '331110': ('Iron and Steel Mills and Ferroalloy Mfg', 'Blast furnace gas / coke', 'Iron & Steel', 'MEDIUM'),
    '331313': ('Alumina Refining and Primary Aluminum', 'Process CO2', 'Metals - Aluminum', 'MEDIUM'),
    '331314': ('Secondary Smelting of Aluminum', 'Combustion', 'Metals - Aluminum', 'LOW'),
    '331315': ('Aluminum Sheet, Plate, and Foil Mfg', 'Combustion', 'Metals - Aluminum', 'LOW'),
    '331492': ('Secondary Smelting and Alloying of Nonferrous', 'Combustion', 'Metals - Other', 'LOW'),
    '331511': ('Iron Foundries', 'Combustion / coke', 'Iron & Steel', 'MEDIUM'),
    '327420': ('Gypsum Product Manufacturing', 'Calcination', 'Minerals - Other', 'MEDIUM'),
    '327211': ('Flat Glass Manufacturing', 'Combustion', 'Glass Manufacturing', 'LOW'),
    '327213': ('Glass Container Manufacturing', 'Combustion', 'Glass Manufacturing', 'LOW'),
    '327993': ('Mineral Wool Manufacturing', 'Combustion', 'Minerals - Other', 'LOW'),
    '327999': ('All Other Miscite Nonmetallic Mineral Products', 'Combustion', 'Minerals - Other', 'LOW'),
    
    # === LOW PURITY (3-15% CO2) - Combustion flue gas ===
    '221112': ('Fossil Fuel Electric Power Generation', 'Combustion flue gas', 'Fossil Power Generation', 'LOW'),
    '221118': ('Other Electric Power Generation', 'Combustion flue gas', 'Fossil Power Generation', 'LOW'),
    '221210': ('Natural Gas Distribution', 'Combustion / leakage', 'Natural Gas Transmission', 'LOW'),
    '221330': ('Steam and Air-Conditioning Supply', 'Combustion flue gas', 'Industrial Heating', 'LOW'),
    
    # === WASTE STREAMS ===
    '562212': ('Solid Waste Landfill', 'Landfill gas (CH4/CO2)', 'Landfills', 'LOW-BIOGENIC'),
    '562213': ('Solid Waste Combustors and Incinerators', 'Combustion flue gas', 'Waste-to-Energy', 'LOW'),
    '562910': ('Remediation Services', 'Various', 'Waste - Other', 'LOW'),
    
    # === PULP & PAPER ===
    '322110': ('Pulp Mills', 'Combustion (black liquor)', 'Pulp & Paper', 'LOW-BIOGENIC'),
    '322120': ('Paper Mills', 'Combustion flue gas', 'Pulp & Paper', 'LOW-BIOGENIC'),
    '322130': ('Paperboard Mills', 'Combustion flue gas', 'Pulp & Paper', 'LOW-BIOGENIC'),
    
    # === FOOD & AGRICULTURE ===
    '311221': ('Wet Corn Milling', 'Fermentation CO2', 'Food Processing - Fermentation', 'HIGH'),
    '311224': ('Soybean and Other Oilseed Processing', 'Process CO2', 'Food Processing', 'MEDIUM'),
    '311313': ('Beet Sugar Manufacturing', 'Combustion / process', 'Food Processing', 'LOW'),
    '311411': ('Frozen Fruit/Vegetable Manufacturing', 'Refrigeration / combustion', 'Food Processing', 'LOW'),
    '311421': ('Fruit and Vegetable Canning', 'Combustion', 'Food Processing', 'LOW'),
    '311611': ('Animal (except Poultry) Slaughtering', 'Combustion', 'Food Processing', 'LOW'),
    '311613': ('Rendering and Meat Byproduct Processing', 'Combustion', 'Food Processing', 'LOW'),
    '311615': ('Poultry Processing', 'Combustion', 'Food Processing', 'LOW'),
    
    # === MINING ===
    '212112': ('Bituminous Coal Underground Mining', 'Ventilation air methane', 'Coal Mining', 'LOW'),
    '212115': ('Anthracite Mining', 'Ventilation air methane', 'Coal Mining', 'LOW'),
    '212210': ('Iron Ore Mining', 'Combustion', 'Mining - Other', 'LOW'),
    '212390': ('Other Nonmetallic Mineral Mining', 'Combustion', 'Mining - Other', 'LOW'),
    
    # === OTHER ===
    '334413': ('Semiconductor and Related Device Mfg', 'Process gases (PFCs)', 'Electronics', 'N/A-PFC'),
    '336110': ('Automobile and Light Duty Motor Vehicle Mfg', 'Combustion', 'Manufacturing - Other', 'LOW'),
    '611310': ('Colleges, Universities, and Professional Schools', 'Combustion (heating)', 'Institutional', 'LOW'),
    '622110': ('General Medical and Surgical Hospitals', 'Combustion (heating)', 'Institutional', 'LOW'),
    '928110': ('National Security', 'Combustion', 'Government/Military', 'LOW'),
    '488999': ('All Other Support Activities for Transportation', 'Combustion', 'Transportation', 'LOW'),
    '561210': ('Facilities Support Services', 'Combustion', 'Services', 'LOW'),
}

def load_emitters():
    with open('/Users/devanagrawal/Desktop/project-seer/map/web/public/data/emitters.geojson') as f:
        return json.load(f)

def analyze_emitters():
    data = load_emitters()
    
    # Aggregate by NAICS
    naics_data = defaultdict(lambda: {'count': 0, 'emissions': 0, 'facilities': []})
    unknown_naics = defaultdict(lambda: {'count': 0, 'emissions': 0, 'sectors': set()})
    
    for f in data['features']:
        p = f['properties']
        code = str(p.get('primary_naics_code', 'None'))
        emissions = p.get('total_emissions_2023', 0) or 0
        sector = p.get('industry_type_sectors', 'Unknown')
        
        if code in NAICS_CLASSIFICATION or code == 'None':
            naics_data[code]['count'] += 1
            naics_data[code]['emissions'] += emissions
        else:
            unknown_naics[code]['count'] += 1
            unknown_naics[code]['emissions'] += emissions
            unknown_naics[code]['sectors'].add(sector)
    
    # Print classified NAICS
    print("=" * 140)
    print("CLASSIFIED EMITTERS BY NAICS CODE (sorted by emissions)")
    print("=" * 140)
    print(f"{'NAICS':<8} {'Count':>6} {'Total Mt':>10} {'Avg kt':>8} {'Industry Description':<45} {'CO2 Stream':<25} {'Category':<25} {'Purity'}")
    print("-" * 140)
    
    # Group by purity for summary
    purity_summary = defaultdict(lambda: {'count': 0, 'emissions': 0})
    category_summary = defaultdict(lambda: {'count': 0, 'emissions': 0})
    
    for code, d in sorted(naics_data.items(), key=lambda x: -x[1]['emissions']):
        if code == 'None':
            continue
        info = NAICS_CLASSIFICATION.get(code, ('Unknown', 'Unknown', 'Unclassified', 'UNKNOWN'))
        avg_kt = d['emissions'] / d['count'] / 1000 if d['count'] > 0 else 0
        print(f"{code:<8} {d['count']:>6} {d['emissions']/1e6:>10.3f} {avg_kt:>8.1f} {info[0]:<45} {info[1]:<25} {info[2]:<25} {info[3]}")
        
        purity_summary[info[3]]['count'] += d['count']
        purity_summary[info[3]]['emissions'] += d['emissions']
        category_summary[info[2]]['count'] += d['count']
        category_summary[info[2]]['emissions'] += d['emissions']
    
    # Print unknown NAICS
    print("\n" + "=" * 100)
    print("UNCLASSIFIED NAICS CODES (need manual review)")
    print("=" * 100)
    for code, d in sorted(unknown_naics.items(), key=lambda x: -x[1]['emissions'])[:20]:
        sectors = ', '.join(list(d['sectors'])[:2])
        print(f"{code:<8} {d['count']:>5} facilities  {d['emissions']/1e6:>8.3f} Mt  Sectors: {sectors}")
    
    # Summary by purity
    print("\n" + "=" * 80)
    print("SUMMARY BY CO2 PURITY LEVEL")
    print("=" * 80)
    print(f"{'Purity Level':<20} {'Facilities':>10} {'Total Mt CO2':>15} {'% of Total':>12}")
    print("-" * 60)
    total_emissions = sum(d['emissions'] for d in purity_summary.values())
    for purity in ['HIGH', 'MEDIUM-HIGH', 'MEDIUM', 'LOW', 'LOW-BIOGENIC', 'N/A-PFC', 'UNKNOWN']:
        d = purity_summary.get(purity, {'count': 0, 'emissions': 0})
        pct = 100 * d['emissions'] / total_emissions if total_emissions > 0 else 0
        print(f"{purity:<20} {d['count']:>10} {d['emissions']/1e6:>15.2f} {pct:>11.1f}%")
    
    # Summary by category
    print("\n" + "=" * 80)
    print("SUMMARY BY CO2 SOURCE CATEGORY")
    print("=" * 80)
    print(f"{'Category':<30} {'Facilities':>10} {'Total Mt CO2':>15} {'Avg kt/fac':>12}")
    print("-" * 70)
    for cat, d in sorted(category_summary.items(), key=lambda x: -x[1]['emissions']):
        avg = d['emissions'] / d['count'] / 1000 if d['count'] > 0 else 0
        print(f"{cat:<30} {d['count']:>10} {d['emissions']/1e6:>15.2f} {avg:>12.1f}")
    
    return naics_data, category_summary, purity_summary

if __name__ == '__main__':
    analyze_emitters()
