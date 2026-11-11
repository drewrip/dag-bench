# SPEC.md — Synthetic Data Generation Specification

This document specifies the synthetic source data generated for each of the 10 benchmark
dbt projects (`projects/p01_iot` … `projects/p10_energy`), and how the source data of the
vendored `projects/p11_tuva`, `projects/p12_stripe`, `projects/p13_snowflake` and `projects/p14_astronomer` is derived (last sections). Each section below covers one
product: a short description of the synthetic business it models, followed by its schema —
every generated table and column, the number of rows relative to the scale factor `sf`
(default `1.0`), and how each column's values are produced.

---

## p01_iot — IoT fleet monitoring

A company operates physical sites, each fitted with monitoring devices, each producing a
stream of sensor readings and serviced periodically by maintenance visits.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| sites | site_id | INTEGER | `max(round(6642·sf), 3)` | Sequential 1-indexed id |
| sites | name | VARCHAR | same | `"Site-{id}"` |
| sites | region | VARCHAR | same | Weighted categorical: north_america 35%, europe 25%, apac 25%, latin_america 10%, middle_east_africa 5% |
| sites | latitude | DOUBLE | same | Uniform(-60, 60), 4 decimals |
| sites | longitude | DOUBLE | same | Uniform(-180, 180), 4 decimals |
| sites | timezone | VARCHAR | same | Uniform pick from the IANA zone pool for the row's `region` (e.g. America/New_York, America/Chicago, America/Los_Angeles for north_america) |
| devices | device_id | INTEGER | `max(round(sites·5), 10)` — avg 5 devices/site | Sequential 1-indexed id |
| devices | site_id | INTEGER | same | Popularity-weighted FK over sites (Pareto skew s=0.9) |
| devices | device_type | VARCHAR | same | Weighted categorical: temperature_sensor 30%, humidity_sensor 20%, pressure_sensor 15%, multi_sensor 25%, gateway 10% |
| devices | model | VARCHAR | same | Uniform pick from a 2-3 value model pool keyed by `device_type` (e.g. TS-100/TS-200 for temperature_sensor) |
| devices | firmware | VARCHAR | same | `v{major}.{minor}.{patch}`; major weighted 1:5%, 2:15%, 3:40%, 4:40%; minor/patch uniform |
| devices | installed_date | DATE | same | `base_date(2023-01-01) − Uniform(0, 1096)` days (founding offset) |
| devices | is_active | BOOLEAN | same | Bernoulli(0.95) |
| readings | reading_id | BIGINT | `max(round(devices·1300), 100)` — avg 1300 readings/device | Sequential 1-indexed id |
| readings | device_id | INTEGER | same | Stratified minimum guarantees every device ≥1 reading; remainder popularity-weighted FK over devices (s=0.9) |
| readings | ts | TIMESTAMP | same | `base_ts + Uniform(0, 180 days)` |
| readings | temperature_c | DOUBLE | same | Normal(mean=20, sd=8), clipped to [-20, 60], 2 decimals |
| readings | humidity_pct | DOUBLE | same | Uniform(20, 95), 2 decimals |
| readings | pressure_hpa | DOUBLE | same | Normal(mean=1013, sd=15), 2 decimals |
| readings | battery_pct | TINYINT | same | Deterministic 30-day sawtooth decay from 100%, resetting each cycle, clamped to [5, 100] |
| readings | rssi_dbm | SMALLINT | same | Uniform(-90, -29) |
| readings | error_flag | BOOLEAN | same | Bernoulli(0.015 base; 0.045 when `battery_pct` < 15) |
| maintenance_logs | log_id | INTEGER | `max(round(devices·3.3), 5)` — avg 3.3 logs/device | Sequential 1-indexed id |
| maintenance_logs | device_id | INTEGER | same | Popularity-weighted FK over devices (s=0.9) |
| maintenance_logs | log_ts | TIMESTAMP | same | `base_ts + Uniform(0, 4321 hours)` |
| maintenance_logs | action | VARCHAR | same | Weighted categorical: routine_inspection 35%, battery_replacement 25%, firmware_update 20%, repair 12%, recalibration 5%, decommission 3% |
| maintenance_logs | technician | VARCHAR | same | `"Tech-{Uniform(1,20)}"` |
| maintenance_logs | notes | VARCHAR | same | `"Performed {action} on device"` |

---

## p02_adtech — Digital advertising funnel

An ad platform runs campaigns that serve impressions, a fraction of which click through,
and a fraction of those convert on-site — a classic decaying acquisition funnel.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| campaigns | campaign_id | INTEGER | `max(round(17608·sf), 10)` | Sequential 1-indexed id |
| campaigns | name | VARCHAR | same | `"Campaign {id}"` |
| campaigns | advertiser | VARCHAR | same | `"Brand {Uniform(1,20)}"` |
| campaigns | channel | VARCHAR | same | Weighted categorical: search 30%, social 25%, display 20%, video 12%, native 8%, email 5% |
| campaigns | objective | VARCHAR | same | Weighted categorical: conversion 35%, awareness 25%, consideration 20%, retargeting 20% |
| campaigns | start_date | DATE | same | `base_date(2023-01-01) + Uniform(0, 201)` days |
| campaigns | end_date | DATE | same | `base_date + Uniform(200, 366)` days |
| campaigns | budget | DECIMAL(12,2) | same | Log-normal(median=60000, sigma=0.8), clipped [5000, 2000000] |
| campaigns | cpm_target | DECIMAL(6,2) | same | Uniform(0.5, 15) |
| impressions | imp_id | BIGINT | `max(round(campaigns·2500), 100)` — avg 2500 impressions/campaign | Sequential 1-indexed id |
| impressions | campaign_id | INTEGER | same | Popularity-weighted FK where each campaign's weight is its own real `budget` (bigger budgets draw proportionally more impressions) |
| impressions | user_id | BIGINT | same | Uniform(1, imp_count·3) — a synthetic user pool ~3x impression volume |
| impressions | imp_ts | TIMESTAMP | same | `base_ts + Uniform(0, 300 days)` |
| impressions | device | VARCHAR | same | Weighted categorical: mobile 55%, desktop 30%, tablet 10%, ctv 5% |
| impressions | geo | VARCHAR | same | Weighted categorical over 15 countries: US 40%, UK 10%, CA 8%, DE 7%, AU 6%, remainder split across FR/BR/IN/JP/MX/IT/ES/NL/SE/KR |
| impressions | placement | VARCHAR | same | Weighted categorical: banner 35%, native 25%, video 25%, interstitial 15% |
| impressions | cost_usd | DECIMAL(8,6) | same | `(campaign.cpm_target/1000) · Uniform(0.7,1.3)` — tracks the owning campaign's target CPM |
| clicks | click_id | BIGINT | `max(round(impressions·0.03), 20)` — 3% click-through rate | Sequential 1-indexed id |
| clicks | imp_id, campaign_id, user_id, device | — | same | Materialized-parent sample of real impression rows; fields copied from the sampled impression |
| clicks | click_ts | TIMESTAMP | same | `imp_ts + Exponential(mean=90s)`, capped at 4 hours |
| clicks | landing_url | VARCHAR | same | `"https://brand.com/lp/{Uniform(1,20)}"` |
| conversions | conv_id | INTEGER | `max(round(clicks·0.175), 5)` — 17.5% of clicks convert | Sequential 1-indexed id |
| conversions | click_id, campaign_id, user_id | — | same | Materialized-parent sample of real click rows; fields copied from the sampled click |
| conversions | conv_ts | TIMESTAMP | same | `click_ts + Gamma(shape=2, scale=1 day)` |
| conversions | conv_type | VARCHAR | same | Weighted categorical: purchase 45%, lead 20%, signup 15%, app_install 12%, subscription 8% |
| conversions | revenue | DECIMAL(10,2) | same | Log-normal(median = 15 + campaign.cpm_target·3, sigma=0.6), clipped [1, 5000] — correlates with the campaign's CPM tier |

---

## p03_ecommerce — Online retail

A retailer sells products, organized into a category hierarchy, to customers via its own
storefront (orders/order_items/reviews), and additionally receives an order feed from
third-party marketplaces.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| categories | category_id | INTEGER | `max(round(32555·sf), 5)` | Sequential 1-indexed id |
| categories | name | VARCHAR | same | Cycles through 8 fixed top-level names (Electronics, Home & Kitchen, Apparel, Beauty & Personal Care, Sports & Outdoors, Toys & Games, Books, Grocery) |
| categories | parent_id | INTEGER (nullable) | same | NULL for the first 8 rows (top level); else Uniform pick among the 8 top-level ids — a 2-level tree |
| categories | display_rank | INTEGER | same | = `id` |
| customers | customer_id | INTEGER | `max(round(3255392·sf), 10)` | Sequential 1-indexed id |
| customers | full_name | VARCHAR | same | `"Cust {id}"` |
| customers | email | VARCHAR | same | `"u{id}@ex.com"` |
| customers | country | VARCHAR | same | Weighted categorical over 15 countries: US 40%, CA 8%, GB 8%, DE 6%, FR 5%, AU 5%, remainder across JP/BR/IN/MX/IT/ES/NL/SE/KR |
| customers | signup_date | DATE | same | `base_date(2018-01-01) + Uniform(0, 2001)` days |
| customers | is_active | BOOLEAN | same | Bernoulli(0.9) |
| customers | lifetime_spend | DECIMAL(12,2) | same | Rolled up after generation as the real sum of that customer's `order_items.quantity·unit_price` — never generated independently |
| products | product_id | INTEGER | `max(round(813848·sf), 20)` | Sequential 1-indexed id |
| products | category_id | INTEGER | same | Popularity-weighted FK over categories (s=1.0) |
| products | sku | VARCHAR | same | `"SKU-{id, zero-padded to 6}"` |
| products | name | VARCHAR | same | `"Prod {id}"` |
| products | cost | DECIMAL(10,2) | same | Uniform(1, 400) |
| products | price | DECIMAL(10,2) | same | `cost · Uniform(1.3, 3.0)` — markup over cost |
| products | weight_kg | DECIMAL(6,3) | same | Uniform(0.1, 20) |
| products | is_active | BOOLEAN | same | Bernoulli(0.95) |
| products | stock_qty | INTEGER | same | Uniform(0, 1000) |
| orders | order_id | INTEGER | `max(round(customers·4), 30)` — avg 4 orders/customer | Sequential 1-indexed id |
| orders | customer_id | INTEGER | same | Popularity-weighted FK over customers (s=1.1) |
| orders | order_date | DATE | same | `base_date + Uniform(0, 2001)` days |
| orders | status | VARCHAR | same | Weighted categorical: completed 75%, cancelled 8%, returned 7%, pending 5%, processing 5% |
| orders | channel | VARCHAR | same | Weighted categorical: web 55%, mobile_app 35%, phone 5%, in_store_kiosk 5% |
| orders | discount_pct | DECIMAL(5,2) | same | Bernoulli(0.4) gate → Uniform(0, 30), else 0 |
| orders | shipping_cost | DECIMAL(8,2) | same | Uniform(0, 25) |
| order_items | item_id | INTEGER | `max(round(orders·2.8), 50)` — avg 2.8 items/order | Sequential 1-indexed id |
| order_items | order_id | INTEGER | same | Stratified minimum guarantees every order ≥1 item; remainder popularity-weighted FK over orders (s=1.0) |
| order_items | product_id | INTEGER | same | Popularity-weighted FK over products (s=1.0) |
| order_items | quantity | INTEGER | same | Weighted categorical over 1-8: weights 35,25,15,10,7,4,2,2 |
| order_items | unit_price | DECIMAL(10,2) | same | Uniform(5, 500) |
| reviews | review_id | INTEGER | `max(count of eligible completed order_items sampled at 22.5%), 20)` | Sequential 1-indexed id |
| reviews | product_id, customer_id | INTEGER | same | Filtered-subset sample from `order_items JOIN orders WHERE status='completed'` (a 22.5% Bernoulli sample of eligible rows) |
| reviews | rating | TINYINT | same | Uniform(1, 5) |
| reviews | review_date | DATE | same | `base_date + Uniform(0, 2001)` days |
| reviews | helpful_votes | INTEGER | same | Uniform(0, 200) |
| marketplace_orders | external_order_id | VARCHAR | `max(round(1139387·sf), 10)` | `"MKT-{id, zero-padded to 10}"` |
| marketplace_orders | customer_id | INTEGER | same | Popularity-weighted FK over the same customer pool (s=1.1) |
| marketplace_orders | order_date | DATE | same | `base_date + Uniform(0, 2001)` days |
| marketplace_orders | marketplace_name | VARCHAR | same | Weighted categorical: Amazon 55%, eBay 20%, Walmart Marketplace 15%, Etsy 10% |
| marketplace_orders | partner_status | VARCHAR | same | Weighted categorical: shipped 70%, pending 12%, cancelled 10%, refunded 8% |
| marketplace_orders | gross_amount | DECIMAL(10,2) | same | Uniform(10, 600) |
| marketplace_orders | commission_fee | DECIMAL(8,2) | same | `gross_amount · Uniform(0.08, 0.20)` |

---

## p04_fraud — Card/banking fraud detection

A bank monitors card accounts transacting with merchants; a fraud team raises alerts,
concentrated on the subset of transactions the system flagged as suspicious.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| accounts | account_id | INTEGER | `max(round(1988346·sf), 10)` | Sequential 1-indexed id |
| accounts | holder_name | VARCHAR | same | `"Holder {id}"` |
| accounts | account_type | VARCHAR | same | Weighted categorical: checking 45%, savings 30%, credit_card 20%, business 5% |
| accounts | country | VARCHAR | same | Weighted categorical over 10 countries: US 60%, remainder across CA/UK/DE/FR/MX/AU/JP/BR/IN |
| accounts | credit_limit | DECIMAL(12,2) | same | Uniform(500, 50000) |
| accounts | opened_date | DATE | same | `base_date(2022-01-01) − Uniform(30, 3651)` days |
| accounts | is_frozen | BOOLEAN | same | Bernoulli(0.03) |
| merchants | merchant_id | INTEGER | `max(round(596340·sf), 10)` | Sequential 1-indexed id |
| merchants | name | VARCHAR | same | `"Merchant {id}"` |
| merchants | category | VARCHAR | same | Weighted categorical: grocery 18%, restaurant 16%, online_retail 15%, gas_station 12%, electronics 10%, travel 8%, entertainment 8%, utilities 7%, other 6% |
| merchants | country | VARCHAR | same | Weighted categorical: US 45%, GB 15%, CN 12%, NG 10%, RU 8%, MX 6%, DE 4% |
| merchants | risk_tier | VARCHAR | same | Weighted categorical: low 60%, medium 30%, high 10% |
| merchants | avg_txn_amount | DECIMAL(10,2) | same | Uniform(5, 500) |
| transactions | txn_id | INTEGER | `max(round(accounts·20), 50)` — avg 20 txns/account | Sequential 1-indexed id |
| transactions | account_id | INTEGER | same | Popularity-weighted FK over accounts (s=1.0) |
| transactions | merchant_id | INTEGER | same | Popularity-weighted FK over merchants (s=1.1) |
| transactions | amount | DECIMAL(12,2) | same | Uniform(1, 5000) |
| transactions | txn_ts | TIMESTAMP | same | `base_ts + Uniform(0, 365 days)` |
| transactions | channel | VARCHAR | same | Weighted categorical: card_present 45%, online 30%, mobile_wallet 15%, card_not_present 7%, atm 3% |
| transactions | currency | VARCHAR | same | Weighted categorical: USD 85%, EUR 6%, GBP 4%, CAD 3%, other 2% |
| transactions | is_declined | BOOLEAN | same | Bernoulli(0.85 if `account.is_frozen` else 0.05) |
| transactions | is_flagged | BOOLEAN | same | Bernoulli, base rate by `merchant.risk_tier` (high 0.10 / medium 0.04 / low 0.02), ×1.8 odds when `amount` > 2000, capped at 0.5 — tuned so ~3-5% of transactions flag overall |
| transactions | response_code | VARCHAR | same | `"approved"` when not declined; else weighted categorical: insufficient_funds 3%, do_not_honor 2%, expired_card 1.5%, invalid_pin 1%, fraud_suspected 0.5% |
| alerts | alert_id | INTEGER | `max(round(flagged_txn_count·1.1), 5)` — sized off the real flagged count | Sequential 1-indexed id |
| alerts | txn_id | INTEGER | same | Filtered-subset sample, uniform within the actual set of `is_flagged` transactions |
| alerts | alert_type | VARCHAR | same | Weighted categorical: unusual_amount 30%, velocity 25%, geo_anomaly 20%, card_not_present 15%, identity_theft 10% |
| alerts | severity | VARCHAR | same | Weighted categorical: low 35%, medium 35%, high 22%, critical 8% |
| alerts | created_ts | TIMESTAMP | same | `base_ts + Uniform(0, 365 days)` |
| alerts | resolved | BOOLEAN | same | Bernoulli(0.6) |
| alerts | resolution | VARCHAR (nullable) | same | When resolved: weighted categorical false_positive 45%, customer_verified 30%, confirmed_fraud 25%; else NULL |

---

## p05_hr — People analytics

A company organized into departments employs people in a manager hierarchy; each employee
has a salary history, periodic performance reviews, and leave requests.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| departments | dept_id | INTEGER | `max(round(122133·sf), 5)` | Sequential 1-indexed id |
| departments | name | VARCHAR | same | `"Dept-{id}"` |
| departments | division | VARCHAR | same | Weighted categorical over 9 divisions: Engineering 25%, Sales 18%, Customer Success 12%, Marketing 10%, Operations 10%, Finance 8%, Product 8%, HR 5%, Legal 4% |
| departments | location | VARCHAR | same | Weighted categorical: New York 25%, San Francisco 20%, Austin 15%, London 10%, Berlin 10%, Remote 20% |
| departments | budget | DECIMAL(14,2) | same | Uniform(100000, 10000000) |
| departments | headcount_target | INTEGER | same | Uniform(5, 100) |
| employees | emp_id | INTEGER | `max(round(3257326·sf), 20)` | Sequential 1-indexed id |
| employees | dept_id | INTEGER | same | Popularity-weighted FK over departments (s=0.9) |
| employees | manager_id | INTEGER (nullable) | same | NULL for the top ~9% earliest-hired employees (manager-eligible pool); else popularity-weighted FK restricted to that pool (s=1.1) |
| employees | first_name, last_name | VARCHAR | same | `"First{id}"` / `"Last{id}"` |
| employees | gender | VARCHAR | same | Weighted categorical: female 48%, male 48%, nonbinary_other 4% |
| employees | hire_date | DATE | same | `base_date(2015-01-01) + Uniform(0, 3001)` days |
| employees | job_title | VARCHAR | same | Manager-eligible rows: weighted categorical Manager 60%, Senior Manager 25%, Director 10%, VP 5%; others: IC1 15%, IC2 25%, IC3 30%, IC4 20%, IC5 10% |
| employees | employment_type | VARCHAR | same | Weighted categorical: full_time 85%, contractor 8%, part_time 5%, intern 2% |
| employees | is_active | BOOLEAN | same | Bernoulli(0.93) |
| salaries | salary_id | INTEGER | Σ over employees of `1 + floor(tenure_days/540)` — one hire-date row plus a raise every ~18 months of tenure | Sequential 1-indexed id |
| salaries | emp_id, effective_date | — | same | From the precomputed per-employee raise schedule |
| salaries | base_salary | DECIMAL(12,2) | same | Uniform over a band keyed by `job_title` seniority (e.g. VP 220k-350k, Director 150k-220k, Senior Manager 120k-160k, Manager 90k-130k, IC1-5 50k-95k) |
| salaries | bonus | DECIMAL(10,2) | same | `base_salary · Uniform(0, 0.2)` |
| salaries | currency | VARCHAR | same | Constant `"USD"` |
| performance_reviews | review_id | INTEGER | Σ over employees of `min(tenure_years, 10)`, floor 1 — one review per year of tenure | Sequential 1-indexed id |
| performance_reviews | emp_id, review_date | — | same | From the precomputed per-employee annual-review schedule |
| performance_reviews | reviewer_id | INTEGER | same | The employee's real `manager_id` (self-reviewed if none) |
| performance_reviews | score | DECIMAL(4,2) | same | Uniform(1.0, 5.0) |
| performance_reviews | category | VARCHAR | same | Weighted categorical: exceeds_expectations 20%, meets_expectations 60%, needs_improvement 15%, unsatisfactory 5% |
| performance_reviews | notes | VARCHAR | same | `"Review notes for review {id}"` |
| leave_requests | leave_id | INTEGER | `max(round(employees·7.5), 10)` | Sequential 1-indexed id |
| leave_requests | emp_id | INTEGER | same | Popularity-weighted FK over employees (s=1.2) |
| leave_requests | leave_type | VARCHAR | same | Weighted categorical: vacation 55%, sick 25%, parental 8%, unpaid 5%, bereavement 4%, jury_duty 3% |
| leave_requests | start_date | DATE | same | `base_date + Uniform(0, 3001)` days |
| leave_requests | end_date | DATE | same | `start_date + Uniform(1, 31)` days |
| leave_requests | approved | BOOLEAN | same | Bernoulli(0.9) |

---

## p06_logistics — Supply chain / warehouse logistics

Suppliers ship goods (identified by SKU) into warehouses; warehouses hold periodic
inventory snapshots; purchasing issues purchase orders to replenish stock.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| suppliers | supplier_id | INTEGER | `max(round(267835·sf), 5)` | Sequential 1-indexed id |
| suppliers | name | VARCHAR | same | `"Supplier {id}"` |
| suppliers | country | VARCHAR | same | Weighted categorical over 12 countries: CN 25%, US 15%, VN 10%, MX 10%, DE 8%, IN 8%, remainder across BR/ID/PL/TH/KR/IT |
| suppliers | reliability_score | DECIMAL(4,2) | same | Uniform(0.5, 1.0) |
| suppliers | lead_time_days | INTEGER | same | Uniform(3, 60) |
| suppliers | category | VARCHAR | same | Weighted categorical: electronics_components 25%, packaging 20%, raw_materials 18%, hardware 15%, textiles 12%, food_ingredients 10% |
| suppliers | is_preferred | BOOLEAN | same | Bernoulli(0.5); preferred suppliers get a 2.5× popularity-weight bias for shipments/POs |
| warehouses | wh_id | INTEGER | `max(round(1034·sf), 3)` | Sequential 1-indexed id |
| warehouses | name | VARCHAR | same | `"WH-{id}"` |
| warehouses | country | VARCHAR | same | Uniform pick from [US, DE, SG, BR, AU] |
| warehouses | region | VARCHAR | same | Weighted categorical: north_america 35%, europe 25%, apac 30%, latin_america 10% |
| warehouses | capacity_m3 | INTEGER | same | Uniform(1000, 50000) |
| warehouses | is_active | BOOLEAN | same | Bernoulli(0.95) |
| *(sku catalog)* | sku | VARCHAR | `max(round(10352·sf), 50)` distinct SKUs | A generated string pool, each SKU given a fixed base cost: Uniform(1, 500), independently seeded per SKU |
| shipments | shipment_id | INTEGER | `max(round(suppliers·50), 20)` — avg 50 shipments/supplier | Sequential 1-indexed id |
| shipments | supplier_id | INTEGER | same | Popularity-weighted FK over suppliers (Pareto s=1.0, ×2.5 for `is_preferred`) |
| shipments | wh_id | INTEGER | same | Popularity-weighted FK over warehouses (s=0.8) |
| shipments | sku | VARCHAR | same | Popularity-weighted FK over the SKU catalog (s=1.0) |
| shipments | quantity | INTEGER | same | Uniform(10, 10000) |
| shipments | unit_cost | DECIMAL(10,2) | same | `sku.base_cost · Uniform(0.9, 1.1)` |
| shipments | shipped_date | DATE | same | `base_date(2021-01-01) + Uniform(0, 1001)` days |
| shipments | received_date | DATE | same | `shipped_date + max(supplier.lead_time_days + Uniform(-2, 3), 1)` days |
| shipments | status | VARCHAR | same | Weighted categorical: delivered 70%, in_transit 15%, delayed 8%, customs_hold 4%, cancelled 3% |
| shipments | freight_cost | DECIMAL(10,2) | same | Uniform(50, 5000) |
| inventory | inv_id | INTEGER | `warehouses · skus · 3 snapshots` (exact, full cross-join) | Sequential 1-indexed id |
| inventory | wh_id, sku, snapshot_date | — | same | Deterministic enumeration of every (warehouse, SKU, snapshot) triple; 3 fixed snapshot dates 120 days apart |
| inventory | qty_on_hand | INTEGER | same | Uniform(0, 10000) |
| inventory | qty_reserved | INTEGER | same | Uniform(0, 500) |
| inventory | reorder_point | INTEGER | same | Uniform(100, 1000) |
| purchase_orders | po_id | INTEGER | `max(round(suppliers·20), 10)` — avg 20 POs/supplier | Sequential 1-indexed id |
| purchase_orders | supplier_id | INTEGER | same | Popularity-weighted FK over suppliers (same weights as shipments) |
| purchase_orders | sku | VARCHAR | same | Popularity-weighted FK over the SKU catalog (s=1.0) |
| purchase_orders | ordered_qty | INTEGER | same | Uniform(100, 5000) |
| purchase_orders | unit_price | DECIMAL(10,2) | same | `sku.base_cost · Uniform(0.95, 1.15)` |
| purchase_orders | order_date | DATE | same | `base_date + Uniform(0, 901)` days |
| purchase_orders | expected_date | DATE | same | `order_date + Uniform(7, 60)` days |
| purchase_orders | received_qty | INTEGER | same | `round(ordered_qty · fraction)`; fraction is Uniform(0.95,1.0) 85% of the time, else Uniform(0.3,0.9) (a shortfall event) |
| purchase_orders | status | VARCHAR | same | Weighted categorical: received 55%, approved 15%, partially_received 12%, submitted 10%, draft 5%, cancelled 3% |

---

## p07_saas — B2B SaaS product analytics

SaaS accounts subscribe to plans, generate in-product usage events and per-feature usage,
and file support tickets.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| accounts | account_id | BIGINT | `max(round(343952·sf), 10)` | Sequential 1-indexed id |
| accounts | name | VARCHAR | same | `"Account {id}"` |
| accounts | industry | VARCHAR | same | Weighted categorical over 9 industries: Software/SaaS 20%, Financial Services 15%, Retail/E-commerce 12%, Healthcare 12%, Manufacturing 10%, Media/Entertainment 10%, Education 10%, Government/Public Sector 6%, Other 5% |
| accounts | country | VARCHAR | same | Weighted categorical: US 40%, UK 12%, CA 10%, DE 10%, AU 8%, FR 6%, IN 6%, BR 4%, JP 4% |
| accounts | arr | DECIMAL(12,2) | same | Uniform(5000, 500000) |
| accounts | created_date | DATE | same | `base_date(2022-01-01) + Uniform(0, 700)` days |
| accounts | csm_id | INTEGER | same | Uniform(1, 20) |
| accounts | health_score | TINYINT | same | Uniform(1, 100) |
| subscriptions | sub_id | BIGINT | `max(round(accounts·1.4), 10)` | Sequential 1-indexed id |
| subscriptions | account_id | INTEGER | same | Uniform(1, account_count) — uniform, not popularity-weighted |
| subscriptions | plan | VARCHAR | same | Weighted categorical: starter 35%, professional 35%, business 20%, enterprise 10% |
| subscriptions | seats | INTEGER | same | Uniform(1, 200) |
| subscriptions | mrr | DECIMAL(10,2) | same | `seats · per_seat_rate[plan] · Uniform(0.9,1.1)`; per-seat rates: starter 20, professional 50, business 90, enterprise 150 |
| subscriptions | start_date | DATE | same | `base_date + Uniform(0, 600)` days |
| subscriptions | end_date | DATE | same | `start_date + 365` days |
| subscriptions | is_active | BOOLEAN | same | Bernoulli(0.9) |
| subscriptions | renewal_date | DATE | same | Denormalized copy of `end_date` |
| events | event_id | BIGINT | `max(round(accounts·100), 100)` — avg 100 events/account | Sequential 1-indexed id |
| events | account_id | INTEGER | same | Popularity-weighted FK, weight = `sqrt(account.arr) · max(health_score/100, 0.05)` |
| events | user_id | INTEGER | same | `(account_id-1)·20 + Uniform(1,20)` — a 20-user pool per account |
| events | event_type | VARCHAR | same | Weighted categorical: page_view 30%, feature_used 25%, login 15%, api_call 12%, export 8%, report_generated 6%, invite_sent 4% |
| events | event_ts | TIMESTAMP | same | `base_ts(2022-01-01) + Uniform(0, 700 days)` |
| events | session_id | VARCHAR | same | `"sess_{account_id}_{Uniform(1,500)}"` |
| events | platform | VARCHAR | same | Weighted categorical: web 65%, api 20%, ios 8%, android 7% |
| feature_usage | fu_id | BIGINT | `max(round(accounts·10), 20)` | Sequential 1-indexed id |
| feature_usage | account_id | INTEGER | same | Same arr/health-derived popularity weighting as `events` |
| feature_usage | feature_name | VARCHAR | same | Weighted categorical over 13 features, dominated by dashboards 22%, reporting 18%, alerts 15% |
| feature_usage | usage_date | DATE | same | `base_date + Uniform(0, 700)` days |
| feature_usage | usage_count | INTEGER | same | Uniform(1, 1000) |
| support_tickets | ticket_id | BIGINT | `max(round(accounts·4), 10)` | Sequential 1-indexed id |
| support_tickets | account_id | INTEGER | same | Popularity-weighted FK, weight = `max(100 - health_score, 1)` — inversely correlated with health |
| support_tickets | created_ts | TIMESTAMP | same | `base_ts + Uniform(0, 700 days)` |
| support_tickets | resolved_ts | TIMESTAMP (nullable) | same | Bernoulli(0.8) gate → `base_ts + Uniform(0, 700 days)`, else NULL |
| support_tickets | priority | VARCHAR | same | Weighted categorical: low 40%, medium 35%, high 18%, urgent 7% |
| support_tickets | category | VARCHAR | same | Weighted categorical: technical 30%, billing 18%, bug 18%, onboarding 15%, feature_request 12%, account_access 7% |
| support_tickets | csat_score | TINYINT (nullable) | same | Bernoulli(0.7) gate → Uniform(1, 5), else NULL |
| support_tickets | is_resolved | BOOLEAN | same | Bernoulli(0.8) |

---

## p08_healthcare — Health insurance claims

Patients receive services from providers; claims are filed per encounter, composed of
line items (procedures) and diagnoses.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| patients | patient_id | INTEGER | `max(round(3099146·sf), 20)` | Sequential 1-indexed id |
| patients | dob | DATE | same | `base_date(2020-01-01) − Uniform(5, 85)` years |
| patients | gender | VARCHAR | same | Weighted categorical: female 51%, male 48%, other_unknown 1% |
| patients | zip_code | VARCHAR | same | Uniform(10000, 99999), zero-padded |
| patients | plan_type | VARCHAR | same | Weighted categorical: PPO 35%, HMO 30%, Medicare 15%, EPO 10%, Medicaid 7%, POS 3% |
| patients | state | VARCHAR | same | Weighted categorical over 15 US states by population: CA 12%, TX 9%, FL 7%, NY 6%, remainder across PA/IL/OH/GA/NC/MI/NJ/VA/WA/AZ/MA |
| providers | provider_id | INTEGER | `max(round(619829·sf), 10)` | Sequential 1-indexed id |
| providers | name | VARCHAR | same | `"Provider {id}"` |
| providers | specialty | VARCHAR | same | Weighted categorical over 10 specialties: primary_care 25%, emergency_medicine 12%, cardiology 10%, remainder across orthopedics/psychiatry/general_surgery/dermatology/radiology/pediatrics/oncology |
| providers | state | VARCHAR | same | Same 15-state weighted categorical as `patients.state` |
| providers | is_in_network | BOOLEAN | same | Bernoulli(0.8) |
| providers | npi | VARCHAR | same | `"NPI{id, zero-padded to 10}"` |
| claims | claim_id | INTEGER | `max(round(patients·3), 30)` — avg 3 claims/patient | Sequential 1-indexed id |
| claims | patient_id | INTEGER | same | Popularity-weighted FK by a per-patient utilization tier (low/medium/high, 70/25/5% weights, multipliers 1×/5×/20×) |
| claims | provider_id | INTEGER | same | Popularity-weighted FK over providers (s=0.9) |
| claims | service_date | DATE | same | `base_date + Uniform(0, 1096)` days |
| claims | claim_type | VARCHAR | same | Weighted categorical: professional 45%, institutional 25%, pharmacy 18%, dental 7%, vision 5% |
| claims | status | VARCHAR | same | Weighted categorical: paid 78%, pending 10%, denied 7%, partially_paid 5% |
| claims | denial_reason | VARCHAR (nullable) | same | When `status='denied'`: weighted categorical not_medically_necessary 30%, out_of_network 22%, missing_authorization 20%, incorrect_coding 15%, coverage_terminated 8%, duplicate_claim 5%; else NULL |
| claims | total_billed, total_allowed, total_paid | DECIMAL(12,2) | same | Rolled up after generation as the real `SUM(quantity·unit_cost)` / `SUM(allowed_amount)` / `SUM(paid_amount)` across that claim's `claim_lines` |
| claim_lines | line_id | INTEGER | `max(round(claims·3), 50)` — avg 3 lines/claim | Sequential 1-indexed id |
| claim_lines | claim_id | INTEGER | same | Stratified minimum guarantees every claim ≥1 line; remainder popularity-weighted FK over claims (s=0.8) |
| claim_lines | cpt_code | VARCHAR | same | Weighted categorical over a 50-code CPT pool; the 10 most common codes carry ~50% combined weight |
| claim_lines | quantity | INTEGER | same | Uniform(1, 5) |
| claim_lines | unit_cost | DECIMAL(10,2) | same | `cpt.base_cost · Uniform(0.9, 1.1)` |
| claim_lines | allowed_amount | DECIMAL(10,2) | same | `(unit_cost·quantity) · Uniform(0.5, 0.9)` |
| claim_lines | paid_amount | DECIMAL(10,2) | same | 0 if the owning claim's `status='denied'`; else `allowed_amount · Uniform(0.6, 1.0)` — enforces `paid ≤ allowed ≤ billed` by construction |
| diagnoses | diag_id | INTEGER | `max(round(claims·1.75), 10)` — avg 1.75 diagnoses/claim | Sequential 1-indexed id |
| diagnoses | claim_id | INTEGER | same | Stratified minimum guarantees every claim ≥1 diagnosis; remainder popularity-weighted FK over claims (s=0.8) |
| diagnoses | icd_code | VARCHAR | same | Weighted categorical over a 100-code ICD pool; the 15 most common codes carry ~40% combined weight |
| diagnoses | is_primary | BOOLEAN | same | Bernoulli(0.7) |
| diagnoses | chronic_flag | BOOLEAN | same | Bernoulli(0.4) |

---

## p09_gaming — Mobile/video game analytics

Players progress through levels across play sessions, generating in-session events, and
make in-app purchases.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| players | player_id | INTEGER | `max(round(762872·sf), 20)` | Sequential 1-indexed id |
| players | username | VARCHAR | same | `"Player_{id}"` |
| players | country | VARCHAR | same | Weighted categorical over 13 countries: US 25%, BR 10%, IN 10%, UK 8%, remainder across DE/JP/KR/FR/CA/MX/AU/RU/IT |
| players | platform | VARCHAR | same | Weighted categorical: android 50%, ios 35%, steam 10%, playstation 3%, xbox 1.5%, nintendo_switch 0.5% |
| players | created_ts | TIMESTAMP | same | `base_ts(2023-01-01) + Uniform(0, 200 days)` |
| players | age_group | VARCHAR | same | Weighted categorical: 18_24 25%, 25_34 28%, 13_17 15%, 35_44 18%, 45_plus 10%, under_13 4% |
| players | is_paid_user | BOOLEAN | same | Bernoulli(0.4) |
| levels | level_id | INTEGER | `max(round(19074·sf), 10)` | Sequential 1-indexed id |
| levels | name | VARCHAR | same | `"Level_{id}"` |
| levels | world | VARCHAR | same | Deterministic contiguous blocks across 6 worlds in order (Forest, Desert, Ice Caverns, Volcano, Sky Kingdom, Void) |
| levels | difficulty | VARCHAR | same | Trends harder with world index (base tier + Uniform jitter of ±1) over 4 bands |
| levels | par_time_sec | INTEGER | same | Uniform(60, 600) |
| levels | reward_coins | INTEGER | same | Uniform(10, 500) |
| levels | unlock_level | INTEGER | same | `max(id - Uniform(1,4), 1)` — always a strictly earlier level |
| sessions | session_id | INTEGER | `max(round(players·5), 50)` — avg 5 sessions/player | Sequential 1-indexed id |
| sessions | player_id | INTEGER | same | Popularity-weighted FK (Pareto s=1.0, ×3 bias for `is_paid_user`) |
| sessions | session_start | TIMESTAMP | same | `base_ts + Uniform(0, 300 days)` |
| sessions | session_end | TIMESTAMP | same | `session_start + Uniform(60, 7200)` seconds |
| sessions | platform | VARCHAR | same | Same weighted categorical as `players.platform` |
| sessions | version | VARCHAR | same | Weighted categorical: 2.4.0 10%, 2.5.0 20%, 2.6.1 30%, 2.7.0 40% |
| sessions | levels_attempted | INTEGER | same | Uniform(0, 10) |
| sessions | coins_earned | INTEGER | same | Uniform(0, 1000) |
| events | event_id | BIGINT | `max(round(sessions·8), 200)` — avg 8 events/session | Sequential 1-indexed id |
| events | session_id, player_id, session_start | — | same | Materialized-parent sample of real session rows; fields copied from the sampled session |
| events | event_type | VARCHAR | same | Weighted categorical over 9 types: level_start 25%, level_complete 18%, item_collected 18%, level_fail 15%, remainder across tutorial_step/achievement_unlocked/purchase_prompt_shown/session_start/session_end |
| events | event_ts | TIMESTAMP | same | `session_start + Uniform(0, 7200)` seconds |
| events | level_id | INTEGER | same | Popularity-weighted FK with an unshuffled decay `weight(k) = k^-0.7` over level order — models a drop-off funnel toward early levels |
| events | value | DOUBLE | same | Uniform(0, 1000) |
| events | metadata | VARCHAR | same | `"meta_{id}"` |
| purchases | purchase_id | INTEGER | `max(round(paid_player_count·4), 10)` — avg 4 purchases/paid player | Sequential 1-indexed id |
| purchases | player_id | INTEGER | same | Popularity-weighted FK restricted to `is_paid_user=true` players only |
| purchases | purchase_ts | TIMESTAMP | same | `base_ts + Uniform(0, 300 days)` |
| purchases | item_type | VARCHAR | same | Weighted categorical: coin_pack 35%, skin 20%, booster 18%, battle_pass 15%, character_unlock 8%, remove_ads 4% |
| purchases | item_name | VARCHAR | same | `"Item_{Uniform(1,50)}"` |
| purchases | price_usd | DECIMAL(8,2) | same | Uniform(0.99, 99.99) |
| purchases | currency | VARCHAR | same | Weighted categorical: USD 70%, EUR 12%, GBP 6%, BRL 5%, JPY 4%, other 3% |
| purchases | is_refunded | BOOLEAN | same | Bernoulli(0.03) |

---

## p10_energy — Electric utility / smart grid

Substations feed smart and non-smart meters at customer premises; meters report
consumption readings; substations occasionally experience outages.

### Schema

| Table | Column | Type | Cardinality (rel. to `sf`) | Generation |
|---|---|---|---|---|
| substations | sub_id | INTEGER | `max(round(4562·sf), 5)` | Sequential 1-indexed id |
| substations | name | VARCHAR | same | `"SUB-{id, zero-padded to 3}"` |
| substations | region | VARCHAR | same | Weighted categorical: north_america 35%, europe 30%, apac 25%, latin_america 10% |
| substations | capacity_mw | DECIMAL(10,2) | same | Uniform(10, 500) |
| substations | voltage_kv | INTEGER | same | Uniform pick from [11, 33, 66, 110, 132, 220] |
| substations | lat | DOUBLE | same | Uniform(25, 50), 4 decimals |
| substations | lon | DOUBLE | same | Uniform(-120, -70), 4 decimals |
| meters | meter_id | INTEGER | `max(round(substations·20), 20)` — avg 20 meters/substation | Sequential 1-indexed id |
| meters | sub_id | INTEGER | same | Popularity-weighted FK, weight = the owning substation's real `capacity_mw` |
| meters | customer_id | INTEGER | same | Uniform(1, meter_count·1.05) — a near-1:1 synthetic customer pool |
| meters | meter_type | VARCHAR | same | Weighted categorical: residential 75%, commercial 20%, industrial 5% |
| meters | tariff_class | VARCHAR | same | Conditional on `meter_type`: residential → residential_standard 75% / residential_tou 25%; commercial → commercial_standard 70% / commercial_demand 30%; industrial → fixed industrial_demand |
| meters | install_date | DATE | same | `base_date(2023-01-01) − Uniform(0, 3651)` days |
| meters | is_smart | BOOLEAN | same | Bernoulli(0.7) |
| meters | rated_capacity_kw | DECIMAL(8,2) | same | Uniform range by `meter_type`: industrial 200-1000, commercial 20-200, residential 1-20 |
| consumption_readings | reading_id | BIGINT | `max(round(meters·500), 200)` — avg 500 readings/meter | Sequential 1-indexed id |
| consumption_readings | meter_id | INTEGER | same | Popularity-weighted FK over meters (s=0.9) |
| consumption_readings | read_ts | TIMESTAMP | same | `base_ts + Uniform(0, 364 days)` |
| consumption_readings | kwh | DECIMAL(12,4) | same | `abs(Normal(mean, sd))` by the owning meter's `meter_type`: industrial (60, 25), commercial (18, 8), residential (5, 3) |
| consumption_readings | voltage_v | DOUBLE | same | Normal(mean=230, sd=5) |
| consumption_readings | power_factor | DOUBLE | same | Uniform(0.7, 1.0) |
| consumption_readings | is_estimated | BOOLEAN | same | Bernoulli(0.02) |
| outage_events | outage_id | INTEGER | `max(round(substations·3.5), 5)` — avg 3.5 outages/substation | Sequential 1-indexed id |
| outage_events | sub_id | INTEGER | same | Uniform(1, substation_count) — uniform, not popularity-weighted |
| outage_events | start_ts | TIMESTAMP | same | `base_ts + Uniform(0, 364 days)` |
| outage_events | end_ts | TIMESTAMP | same | `start_ts + Uniform(300, 86460)` seconds |
| outage_events | cause | VARCHAR | same | Weighted categorical: weather 35%, equipment_failure 25%, vegetation 15%, vehicle_accident 10%, animal_contact 8%, planned_maintenance 6%, cyberattack 1% |
| outage_events | affected_meters | INTEGER | same | `round(substation.meter_count · Uniform(0.02, 0.9))`, floor 1 |
| outage_events | severity | VARCHAR | same | Weighted categorical, tier chosen by the `affected_meters` fraction — high fractions skew toward major/critical, low fractions toward minor/moderate |

---

## m01–m06 — Microbenchmarks

The microbenchmark projects (`projects/m01_bad_view_mat` … `projects/m06_bad_small_view_mat`)
have no generator of their own. Their `generate_data.py` runs the p01_iot generator (sharing
its dbgen cache), and their `sources.yml` exposes only two of its tables: `readings` (S_1)
and `devices` (S_2), as specified in the p01_iot section above. Two properties of that data
matter to the microbenchmarks: `readings.device_id` is Pareto-skewed, so a fraction of
devices does not map to the same fraction of readings, and each `devices.model` belongs to
exactly one `device_type`.

---

## p11_tuva — Tuva Core (vendored)

Not generated by `dbgen`, and **not scaled**. `generate_data.py` (`utils/tuva_data.py`) loads
Tuva's published data assets (`s3://tuva-public-resources/tuva-core/1.0.0/`) exactly as
published: the reference data plus one of Tuva's two synthetic Input Layer payloads. Downloads
are cached in `dbgen/.cache/` and the assembled warehouse as `dbgen/.cache/p11_<dataset>.duckdb`.

Choosing the dataset: `generate_data.py` with no argument, or `large`, uses `large`;
`generate_data.py small` uses `small`. Given a scale factor (as `prepare.py` passes), it prints
a warning that sf is ignored and picks the dataset whose nominal sf is nearest on a log scale:
`small` ≈ sf=0.01, `large` ≈ sf=1, so sf < 0.1 → `small`. The nominal sfs are the ratio of
medical claim lines, the volume that drives the heavy models.

### Reference data (`source('tuva_seeds', …)`, 71 tables, same for both datasets)

Terminology (ICD-9/10, SNOMED, LOINC, HCPCS, DRGs, calendar, …), value sets (condition/procedure
groupers, service categories, attribution codes) and provider data (NPPES). ~44M rows; it makes up
nearly all of the ~1.3 GB warehouse. Largest: `provider_data__provider_taxonomy_unpivot` 12.1M,
`provider_data__other_provider_taxonomy` 11.4M, `provider_data__provider` 9.7M,
`terminology__snomed_ct_transitive_closures` 7.9M.

### Input data (`source('tuva', 'synthetic_data__*')`, 15 tables)

| Table | small | large |
|---|---|---|
| synthetic_data__patient | 1,000 | 1,000 |
| synthetic_data__eligibility | 31 | 2,309 |
| synthetic_data__medical_claim | 2,075 | 167,995 |
| synthetic_data__pharmacy_claim | 291 | 12,019 |
| synthetic_data__provider_attribution | 25 | 250 |
| synthetic_data__encounter | 4,418 | 4,418 |
| synthetic_data__appointment | 4,418 | 4,418 |
| synthetic_data__condition | 3,187 | 3,187 |
| synthetic_data__procedure | 5,221 | 5,221 |
| synthetic_data__lab_result | 25,790 | 25,790 |
| synthetic_data__observation | 22,090 | 22,090 |
| synthetic_data__medication | 1,989 | 1,989 |
| synthetic_data__immunization | 500 | 500 |
| synthetic_data__location | 15 | 15 |
| synthetic_data__practitioner | 30 | 30 |

Both payloads have the same 1,000 clinical patients; `large` has 1,000 claims members (500 of
them also clinical patients) with ~2.6 enrolled years each; `small` has 10.

---

## p12_stripe — Fivetran dbt_stripe (vendored)

Not generated by `dbgen`. `generate_data.py` (`utils/stripe_data.py`) synthesizes the Fivetran
Stripe connector schema in DuckDB SQL; every random choice is a hash of (tag, row key), so the
output is deterministic. Every table has all the columns of dbt_stripe's
`get_<table>_columns()` macro; columns not listed below are NULL, except `livemode`,
`is_deleted` (false, ~1% of customers true) and `_fivetran_synced` (2026-01-01).
sf=1 is a ~1 GB `warehouse.duckdb` (~12M source rows).

The business: one Stripe account (USD) selling subscriptions and one-off charges between
2023-01-01 and 2025-12-31 (`dagbench_start_date` / `dagbench_as_of_date`). Customers are in
the US (50%), CA, GB, DE, FR, AU, NL and pay in their local currency (usd, gbp or eur).

Cross-cutting properties:

- **Ids** look like Stripe's: `<prefix>_<24 base62 chars>` (`ch_yFdM896HqfM3l2A9c3UQZp93`),
  a hash of an internal key, so they are random and uncorrelated with time or row order.
- **Test mode**: ~3% extra rows with `livemode = false` (and their own ids) in the 15 tables
  whose staging models filter on livemode; all other rows are live.
- **Growth**: signups, one-off charges and transfers have density ∝ e^(1.5·t) over the window
  (t ∈ [0, 1]), so 2025 has ~3x the charges of 2023.
- **Weekly and daily shape**: weekend days get ~57% of a weekday's events; times of day are
  triangular around 13:00 UTC (06:00–20:24). Payouts happen on business days only.

| Table | Rows (sf=1) | Scaled | Contents |
|---|---|---|---|
| account | 1 | no | the platform account |
| product | 50 | no | Starter/Basic/Team/Pro/Business services |
| price | 450 | no | 9 per product: month / year (10x) / week (¼) × usd / eur (×0.92) / gbp (×0.79) |
| coupon | 100 | no | once / repeating (3–12 months) / forever; percent_off or amount_off |
| customer | 50,000 | yes | signups over the first 95% of the window; 30% taxed at 7% |
| card, payment_method, payment_method_card | 65,050 | yes | 1 card per customer, 30% have 2; one payment method per card |
| subscription | 30,085 | yes | 60% of customers, starting 0–30 days after signup, in the customer's currency. Interval: month 75%, year 20%, week 5%. Constant cancellation hazard (4%/month; 25% per yearly renewal), effective at period end |
| subscription_item | 43,443 | yes | 1–3 per subscription, all on the same price interval and currency; items after the first start in one of the first 6 periods |
| invoice | 318,613 | yes | one per subscription billing period. paid ~95%, void 1.5%, uncollectible 2%, open (latest period only) |
| invoice_line_item | 437,221 | yes | one per active item per invoice; quantity 1–20 (skewed), stepping up/down every 6 periods so MRR expands and contracts |
| subscription_discount | 3,595 | yes | 12% of subscriptions, starting in one of their first 3 periods |
| discount | 19,558 | yes | one per invoice inside a subscription discount's window |
| credit_note, credit_note_line_item | 2,913 | yes | 1% of paid invoices, half the total |
| charge | 1,831,164 | yes | invoice payments (1 attempt; 5% of paid invoices retry after a failure; uncollectible = 3 failures) and 30 one-off charges per customer (8% guest checkouts; each charge picks a customer who had signed up by then, skewed towards long-standing customers; 200–60,200 cents). 5% of one-off charges fail, 1% are authorized but not captured |
| payment_intent | 1,804,153 | yes | one per invoice or one-off charge |
| refund | 82,958 | yes | 5% of captured charges, 1–31 days later; 60% full |
| dispute | 6,861 | yes | 0.4% of captured charges (5% disputed twice); won/lost/needs_response/under_review/warning_* |
| transfer | 10,000 | yes | to 50 connected accounts; 2% reversed |
| balance_transaction | 1,808,278 | yes | one per captured charge, refund, transfer and payout; disputes add a withdrawal (and a reversal if won). Amounts in usd (eur ×1.08, gbp ×1.27, with exchange_rate); fee 2.9% + 30c, +1% for non-usd; available 2 days after creation |
| fee | 2,285,501 | yes | the balance transactions' fees itemized (processing, currency conversion, dispute) |
| payout | 782 | no | one automatic payout per business day sweeping the funds available since the last; arrives 2 business days later; 0.5% fail (with a payout_failure balance transaction) |
| payout_balance_transaction | 1,802,038 | yes | each non-payout balance transaction -> the payout that swept it |

Live row counts are given for tables with test-mode rows; the tables hold ~3% more.
Rows scale linearly with `sf` except the fixed tables and payouts (one per business day).

---

## p13_snowflake — data-eng-bench retail warehouse (vendored)

Not generated by `dbgen`, and **not scaled**. `generate_data.py` (`utils/retail_data.py`)
downloads data-eng-bench's `base-image/database/retail.duckdb` (Git LFS, pinned commit,
sha256-checked, cached in `dbgen/.cache/`) and uses it as-is; given an sf (as `prepare.py`
passes), it prints a warning that sf is ignored. The assembled warehouse is cached as
`dbgen/.cache/p13_snowflake_v<N>.duckdb`.

Upstream's file keeps raw tables in 19 upper-case schemas plus a few tables in `main`, next to
a prebuilt copy of every model and alias views, which are dropped. Each raw table is copied
into `main` as `src_<schema>__<table>` (lower case), with lower-case column names; all-NULL
columns (DuckDB type `NULL`) become VARCHAR. 581 of them are read by enabled models,
~568k rows in all; the largest is `src_digital__web_page_views` (20k rows).

The data is synthetic and deliberately dirty: duplicates and `_HIST` copies of most tables,
dates as strings in several formats (`2024-05-13 19:52:17`, `07/23/2020`, `20231117`, epoch
seconds), orders from 2020-01-02 to 2026-01-07.

| Source schema | Systems | Tables | Rows |
|---|---|---|---|
| raw_sap | SAP ERP (VBAK/VBAP orders, EKKO/EKPO purchasing, BKPF/BSEG accounting, ...) | 162 | 124,481 |
| raw_ga | web analytics: sessions, events, carts, ads | 78 | 111,363 |
| raw_sfdc | Salesforce contacts, quotes, campaigns, email | 52 | 35,989 |
| raw_legacy | AS/400 item, vendor, GL, PO, invoice files | 48 | 45,868 |
| raw_wms | warehouse management: movements, picks, shipments | 36 | 22,410 |
| raw_pos | point of sale transactions, refunds, promotions | 28 | 22,814 |
| raw_payments | charges, payouts, refunds, subscriptions | 10 | 6,530 |
| orders, customer, product, inventory, procurement, finance, hr, marketing, digital, analytics, audit, reference | cleaner domain tables | 157 | 197,388 |
| main | upstream seeds and `_lookup_*` tables | 10 | 1,413 |

---

## p14_astronomer — de-bench copperline warehouse (vendored)

Not generated by `dbgen`. `generate_data.py` (`utils/astronomer_data.py`) runs de-bench's
own world generator, vendored as `utils/astronomer_gen/gen_copperline` with its config
(`timeline.yaml`, `planted.yaml`), and keeps the 46 tables the models read. The generator
simulates the retailer (`sim_*` schemas) and then writes what its feeds would land
(`raw.*`, `ops.*`); each kept table is copied into `main` as `src_<schema>__<table>`.
Its other outputs (a landing tree of dated files and a frozen `northwave.duckdb`, for
de-bench's Airflow DAGs) are discarded. The output is cached as
`dbgen/.cache/p14_v<N>_sf<sf>.duckdb`.

Every draw is seeded (`seed: 20260615`), so the output is deterministic. Before it
writes, the generator runs its own invariant pass (keys, ties between feeds, load-lag
tails) and refuses to write a world that fails it.

**Scaling.** Upstream has two profiles: `full` and `small` (fact volumes / 20). We pass
a continuous factor, `sf` (sf=1 is `full`, de-bench's shipped world). Fact tables scale
linearly; entity and reference tables (stores, 4,000 trade customers, products,
calendars, FX rates, plans) and per-store-day tables do not, by upstream's design.
sf=1 is a ~1.2 GB `warehouse.duckdb` (~40M source rows) and takes ~35 minutes to
generate (on tmpfs; much longer where every commit's fsync hits disk). sf=0.1 is
~140 MB and takes ~3 minutes.

The business: Copperline, a home, hardware and outdoor-living retailer with stores,
a web shop, a third-party marketplace and ~4,000 trade accounts, trading in the US, CA,
GB, IE and DE (BR, MX, PL and ID from FY2026), on a 4-5-4 fiscal calendar. Orders run
from 2024-02-04 to 2026-06-14; "today" is 2026-06-15 (var `ds`). ~4,000 orders a day on average,
growing 9% a year, with weekday shape and Black Friday / Cyber Monday spikes.

The data carries the seams de-bench plants for its tasks, which the models handle as
upstream does: a recycled seven-digit order reference, a trade-account re-key
(`CUST####` -> `C-######`) with orphans, an acquired company's account book that never
merged, two payment processors that both settled the same payments for a quarter, POS
times in store-local time until a UTC cutover, an inventory valuation change, a SKU
spelled two ways across feeds, at-least-once clickstream delivery (replayed events) and
late-arriving loads.

| Table (`src_raw__*` unless noted) | Rows (sf=1) | Scaled | Contents |
|---|---|---|---|
| order_lines | 9,430,789 | yes | OMS order lines |
| order_status_history | 3,438,328 | yes | status transitions from two feeds (trailing 400 days) |
| orders | 3,436,578 | yes | orders across store, web, marketplace and trade channels |
| shipment_packages | 3,077,924 | yes | carrier packages, with billed freight |
| web_events | 2,524,834 | yes | clickstream events, including replays |
| inventory_snapshots | 2,304,800 | yes | WMS positions: A-class SKUs daily, the tail weekly |
| promo_applications | 2,079,853 | yes | header and line promotion applications |
| pay_meridian_settlements | 2,058,581 | yes | current processor's event stream, with restatements |
| pos_sales_header | 1,850,468 | yes | till transactions |
| pay_halcyon_settlements | 1,481,038 | yes | legacy processor's nightly file, all text columns |
| shipments | 1,397,733 | yes | carrier shipments |
| invoice_lines | 1,370,758 | partly | trade invoice lines (scaled) and plan lines (fixed) |
| wms_movements | 1,199,904 | yes | WMS movement log |
| payment_intents | 1,194,355 | yes | payment attempts |
| marketplace_settlements | 1,039,085 | yes | principal, commission and fee lines, refunds |
| marketplace_orders | 352,415 | yes | marketplace orders |
| gift_card_ledger, gift_cards | 276,314, 195,674 | yes | issue, redeem and breakage entries; the card master |
| pos_batch_manifest | 214,530 | no | one row per store-night file |
| invoices | 196,754 | partly | goods invoices (scaled) and 24,000 plan invoices (fixed) |
| returns | 154,341 | yes | return authorisations |
| ads_spend_daily | 112,519 | no | ad spend by day, platform, campaign, market |
| support_tickets | 95,108 | yes | support tickets |
| pos_sales_daily | 70,861 | no | store-day totals before the header feed existed |
| email_events | 60,340 | yes | email sends, opens, clicks (hashed addresses) |
| plan_lines | 27,048 | no | service-plan contract lines |
| marketplace_payouts | 21,951 | partly | seller payouts |
| pim_product_versions | 21,000 | no | product master versions from three sources |
| market_calendar | 16,443 | no | trading days and expected feeds per market |
| `src_ops__load_control` | 15,577 | no | ingestion job watermarks |
| credit_memos, disputes | 11,528, 1,583 | yes | AR credit memos and disputes |
| fx_rates | 6,034 | no | daily rates, integer parts per million |
| customers, customer_id_map, nwv_accounts | 4,000, 2,660, 1,400 | no | trade accounts, the re-key crosswalk, the acquired book |
| fiscal_calendar | 1,827 | no | 4-5-4 calendar with a 53rd week and comp dates |
| 10 more | 1,065 in all | no | promotions, merge candidates (`ops`), stores (SCD2), categories, carrier invoices, cost complements, lanes, payment terms, merchant regions |

Money is integer cents (`*_cents`, BIGINT) throughout.

