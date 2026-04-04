# dbt-DuckDB Benchmark Suite — 10 Projects

A set of 10 self-contained dbt projects that benchmark DuckDB across
diverse schemas, query shapes, and DAG topologies.

---

## Quick Start

```bash
# 1. Install dependency (once)
pip install dbt-duckdb

# 2. Run everything at small scale (sf=0.05, ~seconds per project)
bash run_all.sh 0.05

# 3. Or run a single project
cd p01_ecommerce
python generate_data.py 0.1        # generate data  (sf controls row counts)
dbt run --profiles-dir . --project-dir .
```

---

## Projects at a Glance

| # | Name | Domain | Source Tables | Models | Layers | Final Layer |
|---|------|--------|--------------|--------|--------|-------------|
| 01 | p01_ecommerce    | E-Commerce         | 6 | 18 | 7 | table |
| 02 | p02_fraud        | Fraud Detection    | 4 | 16 | 8 | table |
| 03 | p03_iot          | IoT Sensors        | 4 | 13 | 6 | table |
| 04 | p04_hr           | HR / Workforce     | 5 | 16 | 7 | table |
| 05 | p05_logistics    | Supply Chain       | 5 | 18 | 8 | table |
| 06 | p06_saas         | SaaS Analytics     | 5 | 15 | 6 | table |
| 07 | p07_healthcare   | Healthcare Claims  | 5 | 17 | 7 | table |
| 08 | p08_adtech       | Ad-Tech / Attribution | 4 | 12 | 5 | table |
| 09 | p09_gaming       | Gaming Telemetry   | 5 | 18 | 7 | table |
| 10 | p10_energy       | Energy Grid        | 4 | 19 | 8 | table |

---

## Scale Factor (`sf`)

Every `generate_data.py` accepts a positional `sf` argument.
Row counts scale **linearly** with `sf`:

| sf    | Size class  | Typical use                   |
|-------|-------------|-------------------------------|
| 0.01  | Tiny        | CI smoke-test (< 1 s)         |
| 0.1   | Small       | Dev / integration test        |
| 1.0   | Medium      | Standard benchmark run        |
| 10.0  | Large       | Stress / concurrency test     |
| 100.0 | X-Large     | Full-scale DBMS benchmark     |

```bash
# Example: generate 10× baseline data
python generate_data.py 10.0
dbt run --profiles-dir . --project-dir .
```

---

## Project Details

### p01_ecommerce — 7 layers
**Schema:** `customers → orders → order_items`, `products → categories`, `reviews`  
**SQL patterns:** multi-join aggregations, window functions (`rank`, `lag`, running totals),
CTEs, `FILTER (WHERE …)`, date arithmetic, `CROSS JOIN` for cross-cutting metrics.

### p02_fraud — 8 layers
**Schema:** `accounts`, `merchants`, `transactions`, `alerts`  
**SQL patterns:** velocity aggregations per hour/day, composite risk scoring, cumulative sums,
`PARTITION BY` ranking, `NTILE`, self-referential subqueries.

### p03_iot — 6 layers
**Schema:** `sites`, `devices`, `readings` (high-cardinality), `maintenance_logs`  
**SQL patterns:** time-bucket rollups (`date_trunc`), z-score anomaly detection with
`STDDEV`, `BOOL_OR`, wide fan-out Layer 1 feeding narrow Layer 2.

### p04_hr — 7 layers
**Schema:** `departments`, `employees`, `salaries`, `performance_reviews`, `leave_requests`  
**SQL patterns:** `ROW_NUMBER` for latest-salary deduplication, window-based pay equity
(`AVG … OVER PARTITION BY`), additive risk scoring, `MEDIAN`.

### p05_logistics — 8 layers
**Schema:** `suppliers`, `warehouses`, `shipments`, `inventory`, `purchase_orders`  
**SQL patterns:** weighted composite scoring, reorder alert logic, fill-rate calculations,
multi-table join fan-outs, `LEAST/GREATEST` clamping.

### p06_saas — 6 layers
**Schema:** `accounts`, `subscriptions`, `events`, `feature_usage`, `support_tickets`  
**SQL patterns:** product-led-growth health scores, churn-risk banding, MRR/ARR
aggregations, engagement metrics, `CROSS JOIN` scalar subquery.

### p07_healthcare — 7 layers
**Schema:** `patients`, `providers`, `claims`, `claim_lines`, `diagnoses`  
**SQL patterns:** `NTILE` cost deciles, `BOOL_OR` chronic flags, allowed/paid gap
calculations, denial-rate tracking, `AVG … OVER ()` normalisation.

### p08_adtech — 5 layers
**Schema:** `campaigns`, `impressions` (very wide), `clicks`, `conversions`  
**SQL patterns:** funnel metrics (CTR, CVR, CPA, ROAS), time-to-click attribution,
spend/revenue share window fractions, high-volume impression aggregation.

### p09_gaming — 7 layers
**Schema:** `players`, `sessions`, `events`, `purchases`, `levels`  
**SQL patterns:** monetisation tier classification, playtime quintile `NTILE`,
completion-rate funnels, `CROSS JOIN` for platform totals, `MODE()`.

### p10_energy — 8 layers
**Schema:** `substations`, `meters`, `consumption_readings` (very wide), `outage_events`  
**SQL patterns:** voltage z-score anomaly detection, availability % calculation,
tariff/meter-type profiles, rolling monthly trends, customer-minutes-lost metric.

---

## File Layout (per project)

```
pXX_<name>/
├── generate_data.py      # synthetic data → data/warehouse.duckdb
├── dbt_project.yml       # dbt config; all layers view except final = table
├── profiles.yml          # DuckDB connection (local file)
├── data/                 # created by generate_data.py
└── models/
    ├── sources.yml       # source table declarations
    ├── layer_1/          # stg_* views (direct source reads + light transforms)
    ├── layer_2/          # enriched / joined views
    ├── …
    └── layer_N/          # final materialised TABLE
```

---

## Running Individual dbt Commands

```bash
cd p03_iot
python generate_data.py 1.0

# Full run
dbt run --profiles-dir . --project-dir .

# Single model
dbt run --profiles-dir . --project-dir . --select anomaly_detection

# Specific layer
dbt run --profiles-dir . --project-dir . --select "layer_4.*"

# Show compiled SQL
dbt compile --profiles-dir . --project-dir . --select iot_operations_report

# DAG overview
dbt ls --profiles-dir . --project-dir . --output json
```
