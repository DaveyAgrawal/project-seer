import matplotlib.pyplot as plt
import numpy as np

# Chart 1: CO2 Purity - Facilities by distance bands
fig, ax = plt.subplots(figsize=(10, 6))

purity = ['High Purity', 'Medium Purity', 'Low Purity']
d_0_5 = [209, 73, 344]
d_5_15 = [431, 126, 700]
d_15_25 = [113, 27, 207]
d_25_50 = [195, 42, 274]

x = np.arange(len(purity))
width = 0.2

bars1 = ax.bar(x - 1.5*width, d_0_5, width, label='0-5 km', color='#27ae60')
bars2 = ax.bar(x - 0.5*width, d_5_15, width, label='5-15 km', color='#3498db')
bars3 = ax.bar(x + 0.5*width, d_15_25, width, label='15-25 km', color='#f39c12')
bars4 = ax.bar(x + 1.5*width, d_25_50, width, label='25-50 km', color='#e74c3c')

ax.set_ylabel('Number of Facilities', fontsize=12)
ax.set_title('Facilities by CO₂ Purity and Distance to Viable CO₂-EGS Land', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(purity, fontsize=11)
ax.legend(title='Distance Band')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig('/Users/devanagrawal/Desktop/project-seer/map/ingest/co2_purity_chart.png', dpi=150)
print('Saved co2_purity_chart.png')
plt.close()

# Chart 2: Primacy vs Federal
fig, ax = plt.subplots(figsize=(8, 6))

categories = ['Primacy\nStates', 'Federal\nStates', 'Application\nPending']
hexes = [3400, 11928, 2482]
percentages = [19.1, 67.0, 13.9]
colors = ['#27ae60', '#3498db', '#f39c12']

bars = ax.bar(categories, hexes, color=colors, edgecolor='black', linewidth=1)
ax.set_ylabel('Viable CO₂-EGS Hexagons', fontsize=12)
ax.set_title('Viable CO₂-EGS Land by Class VI Regulatory Status', fontsize=14, fontweight='bold')

for bar, h, pct in zip(bars, hexes, percentages):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200, 
            f'{h:,}\n({pct}%)', ha='center', va='bottom', fontsize=11, fontweight='bold')

ax.set_ylim(0, max(hexes) * 1.2)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig('/Users/devanagrawal/Desktop/project-seer/map/ingest/primacy_vs_federal_chart.png', dpi=150)
print('Saved primacy_vs_federal_chart.png')
plt.close()

print('Done!')
