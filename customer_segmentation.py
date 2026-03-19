"""
AEDC ADO Area Office — Customer Segmentation using SQL & RFM Analysis
======================================================================
Author: Opemipo Daniel Owolabi
Project: Portfolio Project 3 — SQL Analytics & Customer Segmentation
Tools: Python, SQLite (SQL), Pandas, Matplotlib, Seaborn

Business Problem:
-----------------
The Abuja Electricity Distribution Company (AEDC) has thousands of customers
across multiple book codes (zones). Not all customers behave the same way:
  - Some pay regularly and on time (Champions)
  - Some used to pay but have stopped (At Risk)
  - Some have never engaged properly (Lost)
  - Some are new and showing promise (Promising)

Without segmentation, marketers treat ALL customers the same way —
wasting time chasing good payers and ignoring recoverable ones.

This project uses SQL to build a full RFM (Recency, Frequency, Monetary)
segmentation model on real AEDC customer book code data, answering:
  1. Which book codes are our highest value customers?
  2. Which zones are at risk of becoming non-paying?
  3. Where should marketers focus their collection efforts?
  4. How much revenue is locked in each customer segment?

RFM Explained Simply:
---------------------
R = Recency   → How recently did they pay? (lower days = better)
F = Frequency → How many times did they pay? (higher = better)
M = Monetary  → How much did they pay in total? (higher = better)

Each customer gets scored 1-3 on each dimension.
Combined score determines their segment.
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

sns.set_theme(style="whitegrid", font_scale=1.1)

print("=" * 60)
print("  AEDC CUSTOMER SEGMENTATION — SQL & RFM ANALYSIS")
print("  Opemipo Daniel Owolabi | Data Analytics Portfolio")
print("=" * 60)


# ─────────────────────────────────────────────
# STEP 1: BUILD THE DATABASE
# We create a SQLite database from real AEDC
# book code and collection data extracted from
# the original Excel dashboard files
# ─────────────────────────────────────────────
print("\n[STEP 1] Building SQLite database from AEDC data...")

conn = sqlite3.connect("/home/claude/project3/aedc_customers.db")
cursor = conn.cursor()

# Create customers table
cursor.execute("DROP TABLE IF EXISTS customers")
cursor.execute("""
    CREATE TABLE customers (
        book_code     TEXT PRIMARY KEY,
        zone          TEXT,
        service_centre TEXT,
        customer_pop  INTEGER,
        tariff_band   TEXT
    )
""")

# Create transactions table — daily collection snapshots per book code
cursor.execute("DROP TABLE IF EXISTS transactions")
cursor.execute("""
    CREATE TABLE transactions (
        transaction_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        book_code       TEXT,
        collection_date TEXT,
        amount_collected REAL,
        units_kwh       INTEGER,
        customers_paid  INTEGER,
        FOREIGN KEY (book_code) REFERENCES customers(book_code)
    )
""")

# ── Insert customer data (real book codes from AEDC dashboard) ──
customers = [
    ("98-5601", "ADO 1",  "ADO I",    208, "Band C"),
    ("98-5602", "ADO 1",  "ADO I",    193, "Band C"),
    ("98-5603", "ADO 1",  "ADO I",    52,  "Band D"),
    ("98-5606", "ADO 1",  "ADO I",    141, "Band C"),
    ("98-5609", "ADO 1",  "ADO I",    14,  "Band D"),
    ("98-5613", "ADO 1",  "ADO I",    184, "Band C"),
    ("98-5619", "ADO 1",  "ADO I",    175, "Band C"),
    ("98-5625", "ADO 1",  "ADO I",    153, "Band B"),
    ("98-5627", "ADO 1",  "ADO I",    120, "Band B"),
    ("98-5633", "ADO 1",  "ADO I",    45,  "Band D"),
    ("98-5634", "ADO 1",  "ADO I",    130, "Band C"),
    ("98-5638", "ADO 1",  "ADO I",    22,  "Band D"),
    ("98-5639", "ADO 1",  "ADO I",    35,  "Band D"),
    ("98-5640", "ADO 1",  "ADO I",    88,  "Band C"),
    ("98-5641", "ADO 1",  "ADO I",    95,  "Band C"),
    ("98-5642", "ADO 1",  "ADO I",    78,  "Band C"),
    ("98-5701", "ADO K",  "ADO KASA", 210, "Band B"),
    ("98-5702", "ADO K",  "ADO KASA", 290, "Band B"),
    ("98-5703", "ADO K",  "ADO KASA", 185, "Band C"),
    ("98-5707", "ADO 2",  "ADO II",   63,  "Band C"),
    ("98-5709", "ADO 2",  "ADO II",   177, "Band C"),
    ("98-5711", "ADO 2",  "ADO II",   517, "Band B"),
    ("98-5714", "ADO 2",  "ADO II",   231, "Band C"),
    ("98-5717", "ADO 2",  "ADO II",   196, "Band C"),
    ("98-5718", "ADO 2",  "ADO II",   248, "Band C"),
    ("98-5719", "ADO 2",  "ADO II",   109, "Band D"),
]
cursor.executemany(
    "INSERT INTO customers VALUES (?, ?, ?, ?, ?)", customers
)

# ── Insert transaction data (real daily collection snapshots) ──
# Each row = what was collected per book code on a given date
# Data sourced from the p-sheets in AEDC_ADO_2026.xlsx
transactions = [
    # Date: 2021-06-18
    ("98-5601","2021-06-18", 88450,   2219, 26),
    ("98-5602","2021-06-18", 219329,  3577, 58),
    ("98-5603","2021-06-18", 18500,   471,  6),
    ("98-5606","2021-06-18", 164925,  4098, 43),
    ("98-5609","2021-06-18", 32000,   754,  6),
    ("98-5613","2021-06-18", 259100,  2088, 72),
    ("98-5619","2021-06-18", 301445,  2799, 72),
    ("98-5625","2021-06-18", 223742,  2603, 53),
    ("98-5627","2021-06-18", 219161,  2927, 55),
    ("98-5633","2021-06-18", 109510,  512,  15),
    ("98-5634","2021-06-18", 202993,  2773, 49),
    ("98-5638","2021-06-18", 26400,   633,  9),
    ("98-5639","2021-06-18", 79400,   2280, 15),
    ("98-5640","2021-06-18", 119150,  3048, 31),
    ("98-5641","2021-06-18", 134970,  4073, 37),
    ("98-5642","2021-06-18", 131458,  2216, 31),
    ("98-5701","2021-06-18", 485098,  11171,70),
    # Date: 2021-06-21
    ("98-5601","2021-06-21", 105950,  2468, 31),
    ("98-5602","2021-06-21", 276177,  4649, 73),
    ("98-5603","2021-06-21", 27300,   794,  9),
    ("98-5606","2021-06-21", 177125,  4252, 47),
    ("98-5609","2021-06-21", 36500,   930,  7),
    ("98-5613","2021-06-21", 303650,  2420, 84),
    ("98-5619","2021-06-21", 348645,  2905, 84),
    ("98-5625","2021-06-21", 317411,  4209, 75),
    ("98-5627","2021-06-21", 268261,  4084, 68),
    ("98-5633","2021-06-21", 132710,  1026, 22),
    ("98-5634","2021-06-21", 239301,  3027, 58),
    ("98-5638","2021-06-21", 32400,   750,  11),
    ("98-5639","2021-06-21", 98770,   2851, 20),
    ("98-5640","2021-06-21", 158950,  3953, 40),
    ("98-5641","2021-06-21", 153870,  4619, 42),
    ("98-5642","2021-06-21", 161258,  2777, 39),
    ("98-5701","2021-06-21", 574668,  13274,85),
    ("98-5702","2021-06-21", 787039,  17123,102),
    # Date: 2021-06-22
    ("98-5601","2021-06-22", 114450,  2660, 33),
    ("98-5602","2021-06-22", 282977,  4783, 75),
    ("98-5603","2021-06-22", 33300,   794,  10),
    ("98-5606","2021-06-22", 180625,  4376, 48),
    ("98-5609","2021-06-22", 36500,   930,  7),
    ("98-5613","2021-06-22", 314550,  2420, 86),
    ("98-5619","2021-06-22", 357145,  2905, 86),
    ("98-5625","2021-06-22", 324911,  4328, 77),
    ("98-5627","2021-06-22", 285761,  4613, 72),
    ("98-5633","2021-06-22", 141710,  1026, 24),
    ("98-5634","2021-06-22", 242301,  3142, 59),
    ("98-5638","2021-06-22", 35400,   865,  12),
    ("98-5639","2021-06-22", 102770,  3004, 21),
    ("98-5640","2021-06-22", 180450,  4535, 46),
    ("98-5641","2021-06-22", 156870,  4666, 43),
    ("98-5642","2021-06-22", 171258,  2901, 41),
    ("98-5701","2021-06-22", 624668,  13977,87),
    ("98-5702","2021-06-22", 825039,  18123,108),
    # Date: 2021-06-24
    ("98-5601","2021-06-24", 131950,  3084, 39),
    ("98-5602","2021-06-24", 306777,  4933, 82),
    ("98-5603","2021-06-24", 36300,   794,  11),
    ("98-5606","2021-06-24", 221225,  5317, 59),
    ("98-5609","2021-06-24", 57500,   1357, 11),
    ("98-5613","2021-06-24", 381220,  2850, 101),
    ("98-5619","2021-06-24", 404545,  3140, 96),
    ("98-5625","2021-06-24", 343111,  4446, 81),
    ("98-5627","2021-06-24", 322061,  5633, 84),
    ("98-5633","2021-06-24", 146610,  1079, 26),
    ("98-5634","2021-06-24", 293391,  3801, 74),
    ("98-5638","2021-06-24", 51250,   1345, 17),
    ("98-5639","2021-06-24", 132770,  3327, 27),
    ("98-5640","2021-06-24", 225950,  5650, 57),
    ("98-5641","2021-06-24", 187370,  5321, 51),
    ("98-5642","2021-06-24", 183758,  3035, 45),
    ("98-5701","2021-06-24", 680668,  14696,95),
    ("98-5702","2021-06-24", 961039,  20736,127),
    # Date: 2021-06-27
    ("98-5601","2021-06-27", 142450,  3084, 41),
    ("98-5602","2021-06-27", 306777,  4933, 82),
    ("98-5603","2021-06-27", 36300,   794,  11),
    ("98-5606","2021-06-27", 229225,  5517, 60),
    ("98-5609","2021-06-27", 62300,   1541, 12),
    ("98-5613","2021-06-27", 400020,  2987, 107),
    ("98-5619","2021-06-27", 412045,  3140, 98),
    ("98-5625","2021-06-27", 363819,  4805, 86),
    ("98-5627","2021-06-27", 332043,  5959, 88),
    ("98-5633","2021-06-27", 157110,  1308, 28),
    ("98-5634","2021-06-27", 297391,  3895, 75),
    ("98-5638","2021-06-27", 60560,   1463, 20),
    ("98-5639","2021-06-27", 145770,  3727, 30),
    ("98-5640","2021-06-27", 242450,  6024, 61),
    ("98-5641","2021-06-27", 201170,  5591, 54),
    ("98-5642","2021-06-27", 187258,  3035, 46),
    ("98-5701","2021-06-27", 718688,  15010,100),
    ("98-5702","2021-06-27", 997039,  21736,132),
    # Date: 2021-06-30
    ("98-5601","2021-06-30", 161450,  3417, 47),
    ("98-5602","2021-06-30", 342077,  5489, 93),
    ("98-5603","2021-06-30", 36300,   794,  11),
    ("98-5606","2021-06-30", 251325,  6045, 66),
    ("98-5609","2021-06-30", 62300,   1541, 12),
    ("98-5613","2021-06-30", 422320,  3274, 114),
    ("98-5619","2021-06-30", 429045,  3389, 103),
    ("98-5625","2021-06-30", 410419,  5160, 97),
    ("98-5627","2021-06-30", 358943,  5959, 94),
    ("98-5633","2021-06-30", 177960,  1627, 34),
    ("98-5634","2021-06-30", 328441,  4529, 84),
    ("98-5638","2021-06-30", 67860,   1617, 22),
    ("98-5639","2021-06-30", 145770,  3727, 30),
    ("98-5640","2021-06-30", 256550,  6219, 65),
    ("98-5641","2021-06-30", 217435,  5909, 57),
    ("98-5642","2021-06-30", 207887,  3528, 52),
    ("98-5701","2021-06-30", 740688,  15478,105),
    ("98-5702","2021-06-30", 1199641, 26172,167),
    ("98-5703","2021-06-30", 650000,  15039,85),
]

cursor.executemany(
    "INSERT INTO transactions (book_code, collection_date, amount_collected, units_kwh, customers_paid) VALUES (?,?,?,?,?)",
    transactions
)
conn.commit()
print(f"   ✓ Database created with {len(customers)} customers")
print(f"   ✓ {len(transactions)} transaction records inserted")


# ─────────────────────────────────────────────
# STEP 2: RFM ANALYSIS USING SQL
# This is the core of the project — pure SQL
# doing the heavy analytical lifting
# ─────────────────────────────────────────────
print("\n[STEP 2] Running RFM Analysis SQL queries...")

# ── SQL Query 1: Basic revenue summary per book code ──
print("\n   SQL Query 1: Revenue Summary per Book Code")
q1 = """
    SELECT
        t.book_code,
        c.zone,
        c.service_centre,
        c.customer_pop,
        c.tariff_band,
        COUNT(DISTINCT t.collection_date)   AS total_visits,
        SUM(t.amount_collected)             AS total_collected,
        AVG(t.amount_collected)             AS avg_per_visit,
        MAX(t.amount_collected)             AS best_collection,
        SUM(t.customers_paid)               AS total_customers_paid,
        ROUND(SUM(t.customers_paid) * 1.0 /
              (COUNT(DISTINCT t.collection_date) * c.customer_pop) * 100, 1)
                                            AS avg_response_rate_pct
    FROM transactions t
    JOIN customers c ON t.book_code = c.book_code
    GROUP BY t.book_code
    ORDER BY total_collected DESC
"""
df_summary = pd.read_sql_query(q1, conn)
print(f"   ✓ Retrieved {len(df_summary)} book code summaries")

# ── SQL Query 2: RFM Scoring ──
print("   SQL Query 2: Calculating RFM Scores...")
q2 = """
    WITH rfm_base AS (
        SELECT
            t.book_code,
            c.zone,
            c.tariff_band,
            c.customer_pop,
            -- RECENCY: days since last collection (lower = better)
            JULIANDAY('2021-07-01') - JULIANDAY(MAX(t.collection_date)) AS recency_days,
            -- FREQUENCY: how many collection visits
            COUNT(DISTINCT t.collection_date) AS frequency,
            -- MONETARY: total amount collected
            SUM(t.amount_collected) AS monetary
        FROM transactions t
        JOIN customers c ON t.book_code = c.book_code
        GROUP BY t.book_code
    ),
    rfm_scored AS (
        SELECT *,
            -- Score Recency: 3=most recent, 1=least recent
            CASE
                WHEN recency_days <= 1  THEN 3
                WHEN recency_days <= 4  THEN 2
                ELSE 1
            END AS r_score,
            -- Score Frequency: 3=most frequent, 1=least
            CASE
                WHEN frequency >= 6 THEN 3
                WHEN frequency >= 4 THEN 2
                ELSE 1
            END AS f_score,
            -- Score Monetary: 3=highest value, 1=lowest
            CASE
                WHEN monetary >= 400000 THEN 3
                WHEN monetary >= 200000 THEN 2
                ELSE 1
            END AS m_score
        FROM rfm_base
    )
    SELECT *,
        (r_score + f_score + m_score) AS rfm_total,
        -- Segment based on combined RFM score
        CASE
            WHEN (r_score + f_score + m_score) >= 8 THEN 'Champion'
            WHEN (r_score + f_score + m_score) >= 6 THEN 'Loyal'
            WHEN (r_score + f_score + m_score) >= 5 THEN 'Promising'
            WHEN (r_score + f_score + m_score) >= 4 THEN 'At Risk'
            ELSE 'Lost'
        END AS segment
    FROM rfm_scored
    ORDER BY rfm_total DESC
"""
df_rfm = pd.read_sql_query(q2, conn)
print(f"   ✓ RFM scores calculated for {len(df_rfm)} book codes")

# ── SQL Query 3: Segment Summary ──
print("   SQL Query 3: Segment Revenue Summary...")
q3 = """
    WITH rfm_base AS (
        SELECT
            t.book_code,
            c.zone,
            c.customer_pop,
            JULIANDAY('2021-07-01') - JULIANDAY(MAX(t.collection_date)) AS recency_days,
            COUNT(DISTINCT t.collection_date) AS frequency,
            SUM(t.amount_collected) AS monetary
        FROM transactions t
        JOIN customers c ON t.book_code = c.book_code
        GROUP BY t.book_code
    ),
    rfm_scored AS (
        SELECT *,
            CASE WHEN recency_days <= 1 THEN 3 WHEN recency_days <= 4 THEN 2 ELSE 1 END AS r_score,
            CASE WHEN frequency >= 6 THEN 3 WHEN frequency >= 4 THEN 2 ELSE 1 END AS f_score,
            CASE WHEN monetary >= 400000 THEN 3 WHEN monetary >= 200000 THEN 2 ELSE 1 END AS m_score
        FROM rfm_base
    ),
    segmented AS (
        SELECT *,
            CASE
                WHEN (r_score+f_score+m_score) >= 8 THEN 'Champion'
                WHEN (r_score+f_score+m_score) >= 6 THEN 'Loyal'
                WHEN (r_score+f_score+m_score) >= 5 THEN 'Promising'
                WHEN (r_score+f_score+m_score) >= 4 THEN 'At Risk'
                ELSE 'Lost'
            END AS segment
        FROM rfm_scored
    )
    SELECT
        segment,
        COUNT(*)            AS book_codes,
        SUM(monetary)       AS total_revenue,
        AVG(monetary)       AS avg_revenue,
        SUM(customer_pop)   AS total_customers,
        AVG(frequency)      AS avg_visits
    FROM segmented
    GROUP BY segment
    ORDER BY total_revenue DESC
"""
df_segments = pd.read_sql_query(q3, conn)
print(f"   ✓ {len(df_segments)} customer segments identified")


# ─────────────────────────────────────────────
# STEP 3: VISUALISATIONS
# ─────────────────────────────────────────────
print("\n[STEP 3] Building visualisation dashboard...")

SEGMENT_COLORS = {
    "Champion":  "#1f4e79",
    "Loyal":     "#2e75b6",
    "Promising": "#70ad47",
    "At Risk":   "#ffc000",
    "Lost":      "#c00000"
}

fig, axes = plt.subplots(2, 2, figsize=(16, 11))
fig.suptitle(
    "AEDC ADO Area Office — Customer Segmentation Dashboard (RFM Analysis)\n"
    "SQL-Powered Analytics  |  Analyst: Opemipo Daniel Owolabi",
    fontsize=14, fontweight="bold", y=1.01
)

# --- Chart 1: Revenue by Segment ---
ax1 = axes[0, 0]
seg_colors = [SEGMENT_COLORS.get(s, "#999") for s in df_segments["segment"]]
bars = ax1.bar(df_segments["segment"],
               df_segments["total_revenue"] / 1e6, color=seg_colors)
ax1.set_title("💰 Total Revenue by Customer Segment", fontweight="bold")
ax1.set_ylabel("Total Revenue (₦ Millions)")
ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter("₦%.1fM"))
for bar, val in zip(bars, df_segments["total_revenue"]):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
             f"₦{val/1e6:.1f}M", ha="center", fontsize=9, fontweight="bold")

# --- Chart 2: Number of Book Codes per Segment (Pie) ---
ax2 = axes[0, 1]
wedge_colors = [SEGMENT_COLORS.get(s, "#999") for s in df_segments["segment"]]
wedges, texts, autotexts = ax2.pie(
    df_segments["book_codes"],
    labels=df_segments["segment"],
    colors=wedge_colors,
    autopct="%1.0f%%",
    startangle=90,
    pctdistance=0.75
)
for text in autotexts:
    text.set_fontsize(10)
    text.set_fontweight("bold")
    text.set_color("white")
ax2.set_title("📊 Book Code Distribution by Segment", fontweight="bold")

# --- Chart 3: Top 10 Book Codes by Revenue ---
ax3 = axes[1, 0]
top10 = df_summary.head(10)
bar_colors = []
for bc in top10["book_code"]:
    seg = df_rfm[df_rfm["book_code"] == bc]["segment"].values
    bar_colors.append(SEGMENT_COLORS.get(seg[0] if len(seg) > 0 else "Lost", "#999"))
bars3 = ax3.barh(top10["book_code"], top10["total_collected"] / 1e6, color=bar_colors)
ax3.set_title("🏆 Top 10 Book Codes by Total Revenue\n(colour = RFM segment)",
              fontweight="bold")
ax3.set_xlabel("Total Collected (₦ Millions)")
ax3.xaxis.set_major_formatter(mticker.FormatStrFormatter("₦%.1fM"))
ax3.invert_yaxis()
for bar, val in zip(bars3, top10["total_collected"]):
    ax3.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
             f"₦{val/1e6:.2f}M", va="center", fontsize=9)

# --- Chart 4: RFM Scatter (Frequency vs Monetary, coloured by segment) ---
ax4 = axes[1, 1]
for segment, group in df_rfm.groupby("segment"):
    ax4.scatter(
        group["frequency"], group["monetary"] / 1e6,
        label=segment, color=SEGMENT_COLORS.get(segment, "#999"),
        s=group["customer_pop"] / 2,
        alpha=0.8, edgecolors="white", linewidth=1
    )
    for _, row in group.iterrows():
        ax4.annotate(row["book_code"],
                     (row["frequency"], row["monetary"] / 1e6),
                     fontsize=7, xytext=(3, 3),
                     textcoords="offset points", alpha=0.7)
ax4.set_xlabel("Frequency (No. of Collection Visits)")
ax4.set_ylabel("Total Revenue (₦ Millions)")
ax4.yaxis.set_major_formatter(mticker.FormatStrFormatter("₦%.1fM"))
ax4.set_title("🎯 RFM Map: Frequency vs Revenue\n(bubble size = customer population)",
              fontweight="bold")
ax4.legend(title="Segment", fontsize=8)

plt.tight_layout()
plt.savefig("/home/claude/project3/customer_segmentation_dashboard.png",
            dpi=150, bbox_inches="tight")
print("   ✓ Dashboard saved!")


# ─────────────────────────────────────────────
# STEP 4: PRINT BUSINESS INSIGHTS
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("  KEY BUSINESS INSIGHTS FROM SQL ANALYSIS")
print("=" * 60)

for _, row in df_segments.iterrows():
    emoji = {"Champion": "🏆", "Loyal": "💙", "Promising": "🌱",
             "At Risk": "⚠️", "Lost": "💀"}.get(row["segment"], "📌")
    print(f"\n  {emoji}  {row['segment'].upper()}")
    print(f"     Book Codes:      {int(row['book_codes'])}")
    print(f"     Total Revenue:   ₦{row['total_revenue']/1e6:.2f}M")
    print(f"     Avg per Zone:    ₦{row['avg_revenue']:,.0f}")
    print(f"     Total Customers: {int(row['total_customers'])}")

print("\n  📋 RECOMMENDED ACTIONS:")
print("     🏆 Champions → Maintain service quality, protect relationship")
print("     💙 Loyal     → Reward with priority service")
print("     🌱 Promising → Increase marketer visit frequency")
print("     ⚠️  At Risk   → Urgent outreach, investigate payment barriers")
print("     💀 Lost      → Last-resort recovery campaign or write-off")
print("\n  ✅ SQL Analysis complete!")
print("=" * 60)

conn.close()
