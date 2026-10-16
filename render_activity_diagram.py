import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Create figure
fig, ax = plt.subplots(figsize=(12, 16), dpi=300)
ax.set_xlim(0, 12)
ax.set_ylim(0, 22)
ax.axis('off')

# Font settings
font_title = {'family': 'sans-serif', 'weight': 'bold', 'size': 14}
font_box = {'family': 'sans-serif', 'weight': 'normal', 'size': 9}
font_bold = {'family': 'sans-serif', 'weight': 'bold', 'size': 9}
font_small = {'family': 'sans-serif', 'size': 8}

# Colors
c_start = '#2B3A4A'
c_box_bg = '#EBF3FA'
c_box_border = '#1F5A92'
c_decision_bg = '#FFF4E5'
c_decision_border = '#D97706'
c_fork = '#1E293B'
c_fail_bg = '#FEE2E2'
c_fail_border = '#DC2626'
c_success_bg = '#DCFCE7'
c_success_border = '#16A34A'
c_arrow = '#334155'

# Helper function to draw rounded box
def draw_box(ax, x, y, w, h, text, bg=c_box_bg, border=c_box_border, bold=False):
    box = patches.FancyBboxPatch((x - w/2, y - h/2), w, h,
                                boxstyle="round,pad=0.3,rounding_size=0.2",
                                facecolor=bg, edgecolor=border, linewidth=1.5)
    ax.add_patch(box)
    weight = 'bold' if bold else 'normal'
    ax.text(x, y, text, ha='center', va='center', fontsize=9.5, fontweight=weight, color='#0F172A', multialignment='center')

# Helper function to draw decision diamond
def draw_diamond(ax, x, y, w, h, text):
    diamond = patches.Polygon([
        (x, y + h/2),
        (x + w/2, y),
        (x, y - h/2),
        (x - w/2, y)
    ], facecolor=c_decision_bg, edgecolor=c_decision_border, linewidth=1.5)
    ax.add_patch(diamond)
    ax.text(x, y, text, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#92400E', multialignment='center')

# Helper function for fork/join bar
def draw_bar(ax, x, y, w, h=0.12):
    bar = patches.Rectangle((x - w/2, y - h/2), w, h, facecolor=c_fork, edgecolor=c_fork, zorder=4)
    ax.add_patch(bar)

# Helper function for arrows
def draw_arrow(ax, x1, y1, x2, y2, label=""):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=c_arrow, lw=1.4, mutation_scale=12))
    if label:
        mid_x = (x1 + x2) / 2
        mid_y = (y1 + y2) / 2
        ax.text(mid_x + 0.15, mid_y, label, ha='left', va='center', fontsize=8, fontweight='bold', color='#475569')

# --- DIAGRAM ELEMENTS ---

# Title
ax.text(6, 21.3, "UML Activity Diagram: Airflow Master Pipeline (atb_bi_warehouse_etl)", 
        ha='center', va='center', fontsize=13, fontweight='bold', color='#1E293B')

# 1. Initial State
circle = patches.Circle((6, 20.4), 0.22, facecolor=c_start, edgecolor=c_start)
ax.add_patch(circle)
ax.text(6, 20.75, "02:00 UTC Cron Trigger", ha='center', va='bottom', fontsize=8.5, fontweight='bold', color='#334155')

draw_arrow(ax, 6, 20.18, 6, 19.5)

# 2. Trigger Action
draw_box(ax, 6, 19.2, 5.0, 0.6, "Start DAG Execution\n(atb_bi_warehouse_etl)", bold=True)
draw_arrow(ax, 6, 18.9, 6, 18.3)

# 3. Fork 1: Ingestion
draw_bar(ax, 6, 18.25, 7.5)
ax.text(9.9, 18.25, "Fork: Parallel Ingestion", ha='left', va='center', fontsize=8, style='italic', color='#64748B')

draw_arrow(ax, 3.2, 18.19, 3.2, 17.2)
draw_arrow(ax, 6.0, 18.19, 6.0, 17.2)
draw_arrow(ax, 8.8, 18.19, 8.8, 17.2)

draw_box(ax, 3.2, 16.8, 2.6, 0.7, "Ingest Customers\n(ODS_CUSTOMER)")
draw_box(ax, 6.0, 16.8, 2.6, 0.7, "Ingest Accounts\n(ODS_ACCOUNT)")
draw_box(ax, 8.8, 16.8, 2.6, 0.7, "Ingest References\n(ODS_DAO / SECTOR / ...)")

draw_arrow(ax, 3.2, 16.45, 3.2, 15.51)
draw_arrow(ax, 6.0, 16.45, 6.0, 15.51)
draw_arrow(ax, 8.8, 16.45, 8.8, 15.51)

# 4. Join 1: Ingestion Complete
draw_bar(ax, 6, 15.45, 7.5)
ax.text(9.9, 15.45, "Join: PFE_ODS Ready", ha='left', va='center', fontsize=8, style='italic', color='#64748B')

draw_arrow(ax, 6, 15.39, 6, 14.7)

# 5. dbt Staging
draw_box(ax, 6, 14.35, 5.4, 0.6, "dbt Staging Models\n(Cleaning, Types & Language Standardisation)")
draw_arrow(ax, 6, 14.05, 6, 13.45)

# 6. dbt Intermediate
draw_box(ax, 6, 13.1, 5.4, 0.6, "dbt Intermediate Models\n(Risk Pillar Scores & Business Rules)")
draw_arrow(ax, 6, 12.8, 6, 12.2)

# 7. dbt Warehouse
draw_box(ax, 6, 11.85, 5.4, 0.6, "dbt Warehouse Loading\n(8 Dimensions + 2 Fact Tables)")
draw_arrow(ax, 6, 11.55, 6, 10.95)

# 8. dbt Test
draw_box(ax, 6, 10.6, 5.4, 0.6, "dbt Test Suite\n(Schema, Uniqueness & Referential Integrity)")
draw_arrow(ax, 6, 10.3, 6, 9.6)

# 9. Decision 1: dbt Tests Pass?
draw_diamond(ax, 6, 9.1, 3.2, 1.0, "dbt Tests\nPassed?")

# Failure branch (Right)
ax.annotate('', xy=(10.2, 9.1), xytext=(7.6, 9.1),
            arrowprops=dict(arrowstyle="-|>", color=c_fail_border, lw=1.4, mutation_scale=12))
ax.text(8.2, 9.25, "No", ha='center', va='bottom', fontsize=8.5, fontweight='bold', color='#DC2626')

draw_box(ax, 10.2, 8.1, 2.4, 0.6, "Fail DAG Run &\nSend Alert Email", bg=c_fail_bg, border=c_fail_border, bold=True)
circle_fail_outer = patches.Circle((10.2, 6.8), 0.22, facecolor='none', edgecolor=c_fail_border, linewidth=1.5)
circle_fail_inner = patches.Circle((10.2, 6.8), 0.14, facecolor=c_fail_border, edgecolor=c_fail_border)
ax.add_patch(circle_fail_outer)
ax.add_patch(circle_fail_inner)
draw_arrow(ax, 10.2, 7.8, 10.2, 7.05)

# Success branch (Down)
draw_arrow(ax, 6, 8.6, 6, 7.8, "Yes")

# 10. Fork 2: Parallel Quality Checks
draw_bar(ax, 6, 7.75, 6.5)
ax.text(9.4, 7.75, "Fork: Parallel Quality", ha='left', va='center', fontsize=8, style='italic', color='#64748B')

draw_arrow(ax, 4.0, 7.69, 4.0, 6.8)
draw_arrow(ax, 8.0, 7.69, 8.0, 6.8)

draw_box(ax, 4.0, 6.4, 3.4, 0.7, "check_fact_customer_risk()\n(Row count >= 100k & 0 NULL FKs)")
draw_box(ax, 8.0, 6.4, 3.4, 0.7, "check_fact_account()\n(Row count >= 100k & 0 NULL FKs)")

draw_arrow(ax, 4.0, 6.05, 4.0, 5.16)
draw_arrow(ax, 8.0, 6.05, 8.0, 5.16)

# 11. Join 2: Quality Complete
draw_bar(ax, 6, 5.1, 6.5)
ax.text(9.4, 5.1, "Join: Quality PASSED", ha='left', va='center', fontsize=8, style='italic', color='#64748B')

draw_arrow(ax, 6, 5.04, 6, 4.4)

# 12. Log Metrics
draw_box(ax, 6, 4.05, 5.2, 0.6, "log_pipeline_metrics()\n(XCom Push Row Counts & Log Summary)")
draw_arrow(ax, 6, 3.75, 6, 3.1)

# 13. Decision 2: ML Trigger Gate?
draw_diamond(ax, 6, 2.6, 3.2, 1.0, "Sunday /\nML Gate?")

# Yes branch (Right)
ax.annotate('', xy=(9.2, 2.6), xytext=(7.6, 2.6),
            arrowprops=dict(arrowstyle="-|>", color=c_arrow, lw=1.4, mutation_scale=12))
ax.text(8.3, 2.75, "Yes (Sunday)", ha='center', va='bottom', fontsize=8, fontweight='bold', color='#334155')

draw_box(ax, 9.2, 1.6, 2.6, 0.6, "Trigger Downstream\natb_ml_pipeline DAG", bg='#F1F5F9', border='#475569')

# Connect ML trigger down to Final
ax.annotate('', xy=(6.3, 0.6), xytext=(9.2, 1.3),
            arrowprops=dict(arrowstyle="-|>", color=c_arrow, lw=1.4, mutation_scale=12,
                            connectionstyle="angle,angleA=0,angleB=90,rad=10"))

# No branch (Down)
draw_arrow(ax, 6, 2.1, 6, 1.4, "No")

# 14. Mark Success
draw_box(ax, 6, 1.1, 4.2, 0.6, "Mark DAG Execution as SUCCESS", bg=c_success_bg, border=c_success_border, bold=True)
draw_arrow(ax, 6, 0.8, 6, 0.45)

# 15. Final State
circle_outer = patches.Circle((6, 0.25), 0.22, facecolor='none', edgecolor=c_start, linewidth=1.8)
circle_inner = patches.Circle((6, 0.25), 0.14, facecolor=c_start, edgecolor=c_start)
ax.add_patch(circle_outer)
ax.add_patch(circle_inner)

plt.tight_layout()
out_path = r"c:\Users\Ahmed\Desktop\atb_bi_project\report\figures\ActivityDiagramAIRFLOW1.png"
plt.savefig(out_path, dpi=300, bbox_inches='tight')
print(f"Successfully generated activity diagram image at {out_path}")
