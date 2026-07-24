import math

# CORRECT: 1 hex = 65 sq miles = 168.22 km²
HEX_SQ_MILES = 64.95
HEX_SQ_KM = 168.22

print("="*80)
print("CORRECTED THESIS TABLES - 1 hex = 65 sq miles = 168 km²")
print("="*80)

# Table 8
viable = 20172
print(f"\nTABLE 8: Viable hexes={viable}, Area={viable*HEX_SQ_KM:,.0f} km², Acres={viable*HEX_SQ_MILES*640/1e6:.1f}M")

# Table 9
conv = 1188
print(f"\nTABLE 9: Conv={conv} hex ({conv*HEX_SQ_KM:,.0f} km²), CO2-EGS={viable} hex ({viable*HEX_SQ_KM:,.0f} km²)")

# Table 10 states
states = [('California',2006),('Montana',1916),('Texas',1540),('Nevada',1451),
          ('Arizona',1438),('Oregon',1322),('Idaho',1238),('New Mexico',1194),
          ('Colorado',1188),('Wyoming',1120)]
print("\nTABLE 10:")
for s,h in states:
    print(f"  {s}: {h} hex = {h*HEX_SQ_KM:,.0f} km²")

# Table 11
print(f"\nTABLE 11: Primacy=4790 hex ({4790*HEX_SQ_KM:,.0f} km²), Federal=14939 hex ({14939*HEX_SQ_KM:,.0f} km²)")

# Table 14
print("\nTABLE 14:")
for d,h,m,l in [('0-5km',276,88,340),('5-15km',1590,592,2109),('15-25km',2727,1076,3584),('25-50km',6866,3549,8725)]:
    print(f"  {d}: High {h} ({h*HEX_SQ_KM:,.0f} km²), Med {m} ({m*HEX_SQ_KM:,.0f} km²), Low {l} ({l*HEX_SQ_KM:,.0f} km²)")
