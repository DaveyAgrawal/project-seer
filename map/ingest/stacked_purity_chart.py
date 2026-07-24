import matplotlib.pyplot as plt
import numpy as np

# Data from user - Emissions (MtCO2/yr) by sector
# Format: sector, facilities, emissions, capture_cost, purity

# High purity (>90%)
high_ethanol = 33.7       # Ethanol Fermentation
high_ammonia = 41.4       # Ammonia / Fertilizer  
high_indgas = 33.5        # Industrial Gas Production
high_ngp = 111.5          # Natural Gas Processing

# Medium-High purity
medhigh_refining = 188.3  # Petroleum Refining
medhigh_lime = 21.1       # Lime Manufacturing

# Medium purity
med_petrochem = 88.0      # Petrochemicals
med_cement = 64.8         # Cement Manufacturing
med_steel = 60.5          # Iron & Steel

# Low purity (<15%)
low_power = 1441.1        # Fossil Power Generation
low_wte = 10.0            # Waste-to-Energy

# Low (biogenic)
low_landfill = 81.4       # Landfills
low_pulp = 35.5           # Pulp & Paper

fig, ax = plt.subplots(figsize=(10, 7))

# X positions for 4 bars
x = np.array([0, 1.2, 2.4, 3.6])
width = 0.7

# High purity stacked bar
b = 0
h1 = ax.bar(x[0], high_ngp, width, label='Natural Gas Processing', color='#1a5276')
b += high_ngp
h2 = ax.bar(x[0], high_ammonia, width, bottom=b, label='Ammonia/Fertilizer', color='#2874a6')
b += high_ammonia
h3 = ax.bar(x[0], high_indgas, width, bottom=b, label='Industrial Gas', color='#3498db')
b += high_indgas
h4 = ax.bar(x[0], high_ethanol, width, bottom=b, label='Ethanol', color='#85c1e9')

# Medium purity stacked bar (combining medium-high and medium)
b = 0
m1 = ax.bar(x[1], medhigh_refining, width, label='Petroleum Refining', color='#7d3c00')
b += medhigh_refining
m2 = ax.bar(x[1], med_petrochem, width, bottom=b, label='Petrochemicals', color='#b35900')
b += med_petrochem
m3 = ax.bar(x[1], med_cement, width, bottom=b, label='Cement', color='#e67300')
b += med_cement
m4 = ax.bar(x[1], med_steel, width, bottom=b, label='Iron & Steel', color='#f39c12')
b += med_steel
m5 = ax.bar(x[1], medhigh_lime, width, bottom=b, label='Lime', color='#f7c566')

# Low purity (<15%) stacked bar
b = 0
l1 = ax.bar(x[2], low_power, width, label='Fossil Power', color='#78281f')
b += low_power
l2 = ax.bar(x[2], low_wte, width, bottom=b, label='Waste-to-Energy', color='#b03a2e')

# Low (biogenic) stacked bar
b = 0
lb1 = ax.bar(x[3], low_landfill, width, label='Landfills', color='#196f3d')
b += low_landfill
lb2 = ax.bar(x[3], low_pulp, width, bottom=b, label='Pulp & Paper', color='#27ae60')

ax.set_ylabel('CO₂ Emissions (MtCO₂/yr)', fontsize=12)
ax.set_title('U.S. Industrial CO₂ Emissions by Purity Level and Sector', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(['High Purity\n(>90% CO₂)', 'Medium Purity\n(15-50% CO₂)', 'Low Purity\n(<15% CO₂)', 'Low Purity\n(Biogenic)'], fontsize=10)

# Create legend with all sectors
ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=9)

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig('/Users/devanagrawal/Desktop/project-seer/map/ingest/co2_purity_chart.png', dpi=150, bbox_inches='tight')
print('Saved co2_purity_chart.png')
plt.close()
