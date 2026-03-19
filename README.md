# 🎯 AEDC Customer Segmentation — SQL & RFM Analysis

**Portfolio Project 3** — SQL-powered customer segmentation using RFM Analysis on real electricity distribution data from the **Abuja Electricity Distribution Company (AEDC)** ADO Area Office.

> Built by **Opemipo Daniel Owolabi** — Data Analyst | Python · SQL · Power BI · Tableau  
> 📍 Faro, Portugal | 📧 opemipoowolabi001@gmail.com

---

## 🧩 Business Problem

AEDC has hundreds of customer book codes (zones) across multiple service centres. Without proper segmentation, marketers waste time treating all customers the same — chasing reliable payers while ignoring recoverable ones.

This project uses **SQL + RFM Analysis** to answer:
1. Which book codes are our highest value customers?
2. Which zones are at risk of becoming non-paying?
3. Where should marketers focus their collection efforts?
4. How much revenue is locked in each customer segment?

---

## 📊 Dashboard Preview

![Customer Segmentation Dashboard](customer_segmentation_dashboard.png)

---

## 🤖 What is RFM Analysis?

RFM is one of the most widely used customer segmentation techniques in business analytics:

| Letter | Metric | Question | Scoring |
|--------|--------|----------|---------|
| **R** | Recency | How recently did they pay? | 3 = most recent |
| **F** | Frequency | How many times did they pay? | 3 = most frequent |
| **M** | Monetary | How much did they pay total? | 3 = highest value |

Combined RFM score determines the customer segment:

| Segment | Score | Description | Action |
|---------|-------|-------------|--------|
| 🏆 **Champion** | 8-9 | Pays often, pays a lot, paid recently | Protect & reward |
| 💙 **Loyal** | 6-7 | Consistent payers, good value | Prioritise service |
| 🌱 **Promising** | 5 | New or growing, showing good signs | Increase visits |
| ⚠️ **At Risk** | 4 | Used to pay well, dropping off | Urgent outreach |
| 💀 **Lost** | 3 | Inactive, minimal payment history | Recovery campaign |

---

## 🗄️ Database Structure (SQL)

```sql
-- Customers table
CREATE TABLE customers (
    book_code      TEXT PRIMARY KEY,
    zone           TEXT,
    service_centre TEXT,
    customer_pop   INTEGER,
    tariff_band    TEXT
);

-- Transactions table
CREATE TABLE transactions (
    transaction_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    book_code        TEXT,
    collection_date  TEXT,
    amount_collected REAL,
    units_kwh        INTEGER,
    customers_paid   INTEGER,
    FOREIGN KEY (book_code) REFERENCES customers(book_code)
);
```

---

## 🔍 Key SQL Queries

### Query 1 — Revenue Summary per Book Code
```sql
SELECT
    t.book_code,
    c.zone,
    COUNT(DISTINCT t.collection_date) AS total_visits,
    SUM(t.amount_collected)           AS total_collected,
    AVG(t.amount_collected)           AS avg_per_visit
FROM transactions t
JOIN customers c ON t.book_code = c.book_code
GROUP BY t.book_code
ORDER BY total_collected DESC
```

### Query 2 — RFM Scoring (Core Logic)
```sql
WITH rfm_base AS (
    SELECT
        book_code,
        JULIANDAY('2021-07-01') - JULIANDAY(MAX(collection_date)) AS recency_days,
        COUNT(DISTINCT collection_date) AS frequency,
        SUM(amount_collected)           AS monetary
    FROM transactions
    GROUP BY book_code
)
SELECT *,
    CASE WHEN recency_days <= 1 THEN 3
         WHEN recency_days <= 4 THEN 2
         ELSE 1 END AS r_score,
    CASE WHEN frequency >= 6 THEN 3
         WHEN frequency >= 4 THEN 2
         ELSE 1 END AS f_score,
    CASE WHEN monetary >= 400000 THEN 3
         WHEN monetary >= 200000 THEN 2
         ELSE 1 END AS m_score
FROM rfm_base
```

---

## 📈 Results Summary

| Segment | Book Codes | Total Revenue | Avg per Zone |
|---------|-----------|---------------|--------------|
| 🏆 Champion | 17 | ₦27.41M | ₦1,612,625 |
| 💙 Loyal | 2 | ₦0.84M | ₦419,000 |

---

## ▶️ How to Run

```bash
# Clone the repo
git clone https://github.com/opemipo-analytics/aedc-customer-segmentation.git
cd aedc-customer-segmentation

# Install dependencies
pip install pandas matplotlib seaborn

# Run the analysis
python customer_segmentation.py
```

---

## 🛠️ Tools & Technologies

| Tool | Purpose |
|------|---------|
| **SQL (SQLite)** | Database creation, RFM queries, aggregations |
| **Python 3** | Script orchestration |
| **Pandas** | Reading SQL query results |
| **Matplotlib / Seaborn** | Dashboard visualisations |

---

## 💡 Skills Demonstrated

- **SQL database design** — creating tables, foreign keys, relationships
- **Advanced SQL** — CTEs (WITH clauses), CASE statements, JOINs, aggregations
- **RFM Analysis** — industry-standard customer segmentation technique
- **Business thinking** — translating segments into actionable recommendations
- **Data visualisation** — 4-panel dashboard telling a complete story

---

## 🔗 Other Projects

| Project | Description |
|---------|-------------|
| [AEDC Marketer Performance Analysis](https://github.com/opemipo-analytics/AEDC-MARKETERS-ANALYTICS) | Python analysis of electricity marketer KPIs |
| [AEDC Revenue Forecasting ML](https://github.com/opemipo-analytics/aedc-revenue-forecasting) | Machine Learning revenue forecast (99.8% accuracy) |
| Power BI Revenue Dashboard *(coming soon)* | Interactive BI dashboard |

---

*Built from real data during my time as a Data Analyst at the Abuja Electricity Distribution Company (AEDC), Nigeria.*
