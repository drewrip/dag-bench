# SPEC.md — Synthetic Data Generation Specification

This document specifies, in natural language and mathematics (no code), how to generate
synthetic source data for each of the 10 benchmark dbt projects under `projects/p01_iot`
… `projects/p10_energy`. It is written as ground truth for a data generator (the current
one lives in `dbgen/`, written in Rust) and may diverge from that implementation where the
current approach is unrealistic. Every deviation from `dbgen/`'s current behavior is called
out explicitly as **[CHANGE]**.

The overriding design goal: every table's row count, every foreign key's fan-out, and every
filter's selectivity must look like it came from a real operational system, not from
uniform random noise. Two failure modes to avoid everywhere:

- **Degenerate joins** — a parent table with rows nobody ever references (fine sometimes —
  not every customer buys), or a child table that references only a handful of distinct
  parents when it should spread across most of them.
- **Degenerate filters** — a `WHERE` clause that returns 0 rows, or one that returns
  essentially 100% of rows, at the row counts this benchmark actually generates. Rare-event
  columns (fraud flags, denials, refunds) must be tuned so the *absolute* row count after
  filtering is still large enough to be statistically meaningful (dozens to low thousands),
  not just a fixed percentage that becomes 3 rows at low scale factors.

---

## 1. General Principles (apply to every project unless a project section overrides them)

### 1.1 Scale factor and row counts

Every project takes a single scale factor `sf` (default `1.0`). Each table has a **base row
count** `base` defined at `sf = 1.0`. The generated row count is:

```
rows(table) = max( floor(base(table) * sf), floor_min(table) )
```

`floor_min` exists only so that tiny scale factors (e.g. `sf = 0.01`, used for fast CI runs)
still produce non-empty, joinable tables — it should be roughly `min(base, 5)` for dimension
tables and `min(base, 20)` for fact tables, never higher than `base` itself.

Row counts for *child* tables should not be computed independently of their parent's row
count — see §1.3. Where a project's fact table count is naturally a multiple of a parent
count (e.g., "average 3 line items per order"), express it as `rows(parent) * avg_fanout`
rather than as an unrelated base constant, so that scaling `sf` scales the whole DAG
consistently and average fan-out stays constant across scale factors. **[CHANGE from
dbgen]**: the current implementation gives every table an independent base constant, which
means average fan-out (e.g. items-per-order) silently drifts as `sf` changes because floors
round independently. This spec instead defines most fact-table counts as
`rows(parent) * avg_fanout`, and gives fan-out ratios directly instead of two independent
base counts.

### 1.2 Reproducibility

Generation must be deterministic given `(project, table, sf, row_index)`. Recommended: seed
a per-row RNG from a hash of `(table_name, row_index)` rather than the row index alone, so
that two tables at the same row index don't draw correlated values by coincidence when
generated in parallel batches. This is a one-line hardening of the current per-row-index
seeding and preserves the current property that re-running with the same `sf` reproduces
byte-identical output.

### 1.3 Foreign key sampling strategies

Three distinct strategies, chosen per (child table → parent table) edge based on what the
edge needs to model. Do not use one strategy everywhere — the choice is part of the spec.

**(a) Popularity-weighted sampling** — the default for most fact→dimension edges (orders→
customers, transactions→accounts, readings→devices, events→players, claims→patients,
impressions→campaigns, consumption_readings→meters, etc.). Real operational data is never
uniform: a minority of entities (heavy customers, hot SKUs, popular campaigns, always-on
devices) account for a disproportionate share of activity. Model this as follows:

1. Assign every parent row `k` a popularity weight `w_k` drawn once at parent-generation
   time from a Pareto/power-law shape: `w_k = (rank_k)^(-s)` where `rank_k` is the parent's
   position in a random shuffle of `1..n_parent`, and `s` is a skew exponent in the range
   `0.8`–`1.3` (higher `s` = more concentrated activity; use `~1.0` for a classic 80/20
   split, lower `~0.8` for milder skew, higher `~1.3` for "superstar" entities like a handful
   of huge enterprise accounts).
2. Normalize weights to a categorical distribution `p_k = w_k / Σw_k`.
3. Each child row draws its parent by sampling from this categorical distribution.

This produces realistic long-tail fan-out: most parents get some children, a few get a lot,
and a small fraction may legitimately get zero (exactly as in real systems — not every
customer has ordered, not every device is heavily used). This is a deliberate
**[CHANGE from dbgen]**: today's generator samples FKs uniformly (`rng.gen_range(1..=n)`)
everywhere except a few hand-special-cased tables, which under-represents the "power user /
hot entity" pattern real analytics queries are written to find (top-N customers, top-N
SKUs, anomaly-vs-baseline device comparisons all assume skew exists).

Expected number of zero-child parents under this scheme is not simply the uniform-case
approximation; it depends on `s`. As a sanity check at generation time, cap `s` such that the
top 1% of parents receive no more than roughly 20–40% of all child rows — if a single
parent would receive a majority of rows, reduce `s`. Always spot check: `distinct parent ids
referenced / n_parent` should land somewhere in the 60%–95% range for most fact tables (not
100% — some dimension rows realistically go unused — and not below 50%, which would make
the parent table feel padded with fictional entities).

**(b) Materialized parent sampling** — used when a child row must inherit correlated
attributes from the specific parent it references (e.g., a click's timestamp must be after
its impression's timestamp; a conversion's timestamp after its click's; a game event's
timestamp within its session's start/end window). Procedure: after generating the parent
table, draw a sample of `n_child` actual parent rows (with replacement if `n_child >
n_parent`, without replacement otherwise, still using popularity weights from (a) rather than
uniform sampling — i.e., materialized sampling and popularity weighting are not mutually
exclusive, they compose), keep the parent's key attributes (id, timestamp, amount) in
memory, and derive the child's correlated fields as an offset/fraction of the parent's
values. This guarantees zero orphans and coherent time ordering. **[CHANGE from dbgen]**:
today this sampling is uniform over parents; apply popularity weights here too so that (for
example) high-traffic campaigns realistically generate proportionally more clicks, not an
equal share.

**(c) Filtered-subset sampling** — used when a child table's rows are conceptually
*consequences* of a parent row satisfying some predicate (fraud alerts referencing flagged
transactions; healthcare denial follow-ups; support ticket escalations referencing
high-severity events). Procedure: first materialize the subset of parent rows satisfying the
predicate, then draw the child's parent id from that subset only (uniformly within the
subset, since the predicate itself is already the interesting stratification; add popularity
weighting only if there's a plausible secondary skew, e.g. some flagged accounts get
multiple alerts). Guard rail: `n_child` for a filtered-subset child must never exceed a small
multiple (2–3×) of the filtered subset's size, otherwise you are forcing implausibly many
alerts per flagged transaction; if the predicate's selectivity is too low to support the
desired child count, raise the predicate's Bernoulli rate instead of over-sampling the
subset. This keeps the resulting join (`alerts JOIN transactions WHERE is_flagged`)
returning a plausible, non-explosive row count.

### 1.4 Value distributions

- **Categorical fields** (region, channel, status, plan, industry, etc.): draw from a small
  fixed vocabulary (typically 3–12 values) *with non-uniform weights*, not uniform choice.
  Real categorical columns are skewed (most orders ship via one or two dominant channels;
  most support tickets are "low" priority; most transactions are approved not declined).
  Assign each vocabulary value a fixed weight (e.g., a geometric decay `weight_i =
  0.5^i`, renormalized) rather than picking uniformly. **[CHANGE from dbgen]**: categorical
  values today are drawn uniformly from their vocabulary array; this spec asks for skewed
  weights per field, chosen to match the business meaning (e.g., `order.status` should be
  overwhelmingly "completed" with small tails of "cancelled"/"returned"/"pending", not a flat
  25% each).
- **Continuous physical quantities** (temperature, voltage, pressure, power factor): Normal
  distribution around a realistic operating mean with a domain-appropriate standard
  deviation, clipped to a physically valid range (e.g., battery percentage clipped to
  `[0, 100]`, power factor clipped to `[0, 1]`).
- **Monetary amounts**: Log-normal rather than uniform — real transaction/order/claim amounts
  cluster at a modest value with a long right tail of large outliers, which uniform ranges
  cannot represent. Parametrize by a median and a spread (σ of the underlying normal in log
  space, typically `0.5`–`0.9`), then clip to a sane floor/ceiling.
- **Boolean "rate" columns** (is_flagged, is_declined, is_resolved, is_refunded, etc.):
  Bernoulli with a fixed probability, but the probability must be picked so that
  `rows * probability` is large enough to be a meaningful downstream cohort at the *smallest*
  scale factor the benchmark actually runs (see §1.6) — not just realistic-looking as a
  percentage.
- **Counts / durations** (quantity per line item, session length, time between events):
  Poisson or Gamma-shaped rather than uniform integer ranges, since real count/duration data
  is right-skewed (most orders have 1–3 items, a few have a dozen) — uniform integer ranges
  make the tail as likely as the mode, which is not how retail baskets, session lengths, or
  service durations behave.

### 1.5 Time and date modeling

Every project fixes a `base_date` representing "when this business's history starts" and an
`observation_window` in days representing "how long this synthetic history runs." Timestamps
for the top-of-DAG entity (accounts, customers, devices, employees, patients, players,
substations, sites) may predate `base_date` by a "founding" offset (the entity existed before
the observation window began — e.g., an employee hired 5 years before the HR data warehouse's
observation window starts). Timestamps for *activity* tables (readings, transactions,
sessions, claims, impressions) must fall within `[base_date, base_date + observation_window]`,
and should not be uniform across that whole window if the business has a natural trend —
e.g., SaaS usage events and IoT readings should show mild week-over-week growth or at least
day-of-week/hour-of-day seasonality (weekday-heavy for B2B SaaS/logistics, evening-heavy for
consumer gaming) rather than being perfectly flat over the window. Seasonality can be modeled
simply as a multiplicative weight per day-of-week / hour-of-day bucket when assigning
timestamps, reusing the same weighted-categorical-draw machinery as §1.4.

### 1.6 Filter realism budget

For every Bernoulli/rare-event flag referenced by a downstream `WHERE` clause, choose the
rate `r` such that at the **smallest scale factor this benchmark is run at** (`sf = 0.01` is
used for fast CI per the cached files in `dbgen/.cache/`), `rows(table) * r` is still at least
~20–30 rows, and at `sf = 1.0` it is not so large a fraction that the "interesting" cohort
stops being a minority (target 1%–10% of rows for true rare events like fraud/denial/refund;
10%–30% for "notable but not rare" events like discounts or ticket escalations). This is a
straightforward floor/ceiling check to run per column when picking `r`, not a new mechanism.

### 1.7 Derived-value consistency

Where one column is economically dependent on another, generate it as a function of the
other rather than independently, and keep that function consistent across every table in
the chain a query might roll up:

- Price should be cost times a markup factor (markup > 1), not independently random. A commission/fee should be a percentage of the gross amount it's charged against.
- Allowed/paid amounts in claims-like tables should be monotonically `paid ≤ allowed ≤
  billed`, enforced by generating each as a fraction of the previous, not independently.
- Where a "detail" table's rows should plausibly sum to something close to a "header"
  row's total (e.g., order_items unit_price*quantity vs. an order-level amount, if one
  exists), either omit the header-level total entirely (let a dbt model compute it — the
  current schemas mostly do this correctly, e.g. ecommerce `orders` has no
  redundant total) or, if a header total is present, derive it as the actual sum of its
  detail rows plus a small stated adjustment (tax/shipping) rather than an independent
  random figure. Never generate two numbers that are supposed to reconcile independently.

---

## 2. Per-Project Specifications

Each project section lists: business context, entities in dependency order, the row-count
formula for each, the FK-sampling strategy for each edge, and column-generation notes. Only
notable/non-obvious columns are called out; unnamed columns follow §1.4's generic guidance
for their type (id → sequential; free-text name → templated string; category → weighted
categorical).

### 2.1 p01_iot — IoT fleet monitoring

Business context: a company operates physical sites, each with monitoring devices, each
producing a stream of sensor readings, serviced periodically by maintenance visits.

Entities and row counts (`sf` = scale factor):
- `sites`: `rows = max(round(30*sf), 3)`
- `devices`: `rows = max(round(5 * rows(sites) * sf^0), 10)` — model devices as **5 per
  site on average**, drawn from a Poisson(5) per site rather than an unrelated flat 150, so
  that site-to-device fan-out stays constant as `sf` scales the number of sites.
- `readings`: `rows(devices) * avg_readings_per_device`, where `avg_readings_per_device ≈
  1300` (≈ hourly readings across a ~54-day window at sf=1; adjust window/frequency to taste
  but keep the product roughly matching today's 200,000-row base). Use popularity weighting
  across devices (§1.3a) with skew `s ≈ 0.9` — some devices report more frequently or have
  been active longer.
- `maintenance_logs`: filtered/derived from devices, `rows ≈ rows(devices) * 3.3` (one
  maintenance visit roughly every ~2 months per device across the observation window),
  popularity-weighted toward older/higher-usage devices.

Column notes:
- `devices.installed_date`: predates `base_date` by 0–3 years (founding offset, §1.5).
- `readings.temperature_c`: Normal(mean=20°C, sd=8°C), clipped to a plausible physical band
  (e.g. −20°C to 60°C) — do not clip so tight that the Normal's tail is truncated away
  entirely, or later anomaly-detection models downstream have nothing to detect.
- `readings.error_flag`: Bernoulli, rate should be higher for devices with older firmware or
  lower `battery_pct` (a light correlation, not independent) — e.g. base rate 1.5%, +3× odds
  when `battery_pct < 15`. This gives downstream "device health" models something causally
  real to find instead of pure noise.
- `readings.battery_pct`: should trend downward over a device's lifetime between
  maintenance visits (a simple linear decay from ~100% resetting after each maintenance
  event) rather than uniform, so maintenance logs and battery level correlate.
- Every device should have at least one reading (guarantee via stratified minimum: assign
  each device a floor of 1 before distributing the popularity-weighted remainder) — a device
  with literally zero readings is a data-collection outage, not a normal device, and 100% of
  registered devices reporting at least once over a multi-year window is realistic.

### 2.2 p02_adtech — Digital advertising funnel

Business context: an ad platform runs campaigns that serve impressions, which sometimes
convert to clicks, which sometimes convert to on-site conversions — a classic decaying
funnel.

Entities:
- `campaigns`: `rows = max(round(200*sf), 10)`.
- `impressions`: `rows = rows(campaigns) * avg_impressions_per_campaign`, `avg ≈ 2500`,
  popularity-weighted across campaigns (larger `budget` campaigns should draw proportionally
  more impressions — tie the popularity weight directly to each campaign's `budget` value
  rather than an independent random weight, so budget and impression volume correlate as they
  would in a real auction system).
- `clicks`: materialized-parent-sampled from impressions (§1.3b) at a **click-through rate**
  of ~3% (i.e. `rows(clicks) ≈ rows(impressions) * 0.03`), not an unrelated flat 15,000 —
  CTR should be the parameter, not the row count. `click_ts = imp_ts + Exponential(mean=90s)`
  clipped to a few hours, not a flat uniform 0–3600s (most clicks happen fast after
  impression; a long tail happens later).
- `conversions`: materialized-parent-sampled from clicks at a **conversion rate** of ~15–20%
  of clicks (funnel decay: CTR 3% then CVR 15–20% of clicks yields an overall impression→
  conversion rate around 0.5%, which is realistic for a display/search ad funnel).
  `conv_ts = click_ts + Gamma(shape=2, scale=1 day)` (conversions can lag by hours to days,
  unlike clicks which are near-instant).

Column notes:
- `conversions.revenue` should correlate with the campaign's `objective`/`cpm_target` tier
  (e.g. draw from a log-normal whose median scales with the campaign's target CPM) rather
  than being fully independent — §1.7. This lets a downstream ROAS-by-campaign model produce
  a meaningful, non-random spread.
- `impressions.cost_usd` should be drawn around the owning campaign's `cpm_target`/1000 with
  small noise, not independently — so `SUM(cost_usd)` per campaign tracks `budget`
  plausibly (it need not reconcile exactly — campaigns overspend/underspend in reality — but
  it shouldn't be pure noise against budget).

### 2.3 p03_ecommerce — Online retail

Business context: a retailer sells products (organized into a category hierarchy) to
customers via its own storefront (orders/order_items/reviews) and additionally receives a
feed of orders placed through third-party marketplaces.

Entities:
- `categories`: `rows = max(round(20*sf), 5)`, with a shallow tree (depth 2–3: a handful of
  top-level categories, each with several children) via `parent_id`, not a flat namespace.
- `customers`: `rows = max(round(2000*sf), 10)`.
- `products`: `rows = max(round(500*sf), 20)`, popularity-weighted across categories so a
  few categories (e.g. "Electronics") hold disproportionately more SKUs than a niche one.
- `orders`: `rows(customers) * avg_orders_per_customer`, `avg ≈ 4`, popularity-weighted
  across customers with skew `s ≈ 1.1` (a realistic retail pattern: most customers order
  once or twice, a loyal minority orders many times) — correlated with `customer.
  lifetime_spend`, i.e. generate `lifetime_spend` as a rollup-consistent function of that
  customer's actual order totals (§1.7) rather than independently.
- `order_items`: `rows(orders) * avg_items_per_order`, `avg ≈ 2.8` via a Poisson/Gamma count
  per order (most orders 1–3 items, some larger), popularity-weighted across products with
  skew `s ≈ 1.0` (hot sellers).
- `reviews`: filtered-subset sampled from **delivered/completed orders' order_items** only
  (you cannot review what you haven't received) at roughly a 20–25% review rate per eligible
  item, not an unrelated flat 6,000 rows disconnected from real completed purchases.
  **[CHANGE from dbgen]**: today reviews reference random `product_id`/`customer_id` pairs
  independently of whether that customer ever purchased that product — this spec ties
  reviews to actual completed order_items so `reviews JOIN order_items JOIN orders` is a
  meaningful, non-empty, but not-all-inclusive relationship.
- `marketplace_orders`: `rows = max(round(700*sf), 10)`, referencing `customer_id` from the
  same customer pool via popularity weighting (a subset of storefront customers also buy via
  marketplaces — do not invent a disjoint customer population). Kept deliberately without
  line items (as today) to model a lower-fidelity external feed — this is intentional, not a
  gap: it exercises a benchmark DAG's ability to blend a partial-grain external source with
  the fully-normalized internal one.

Column notes:
- `products.price = cost * Uniform(1.3, 3.0)` (markup, §1.7).
- `marketplace_orders.commission_fee = gross_amount * Uniform(0.08, 0.20)`.
- `orders.status`: weighted categorical, dominated by "completed" (~75%), with smaller
  shares of "cancelled" (~8%), "returned" (~7%), "pending"/"processing" (~10%) — not flat.

### 2.4 p04_fraud — Card/banking fraud detection

Business context: a bank monitors card accounts transacting with merchants; a fraud team
raises alerts, concentrated on the subset of transactions the system flagged as suspicious.

Entities:
- `accounts`: `rows = max(round(1000*sf), 10)`.
- `merchants`: `rows = max(round(300*sf), 10)`.
- `transactions`: `rows(accounts) * avg_txn_per_account`, `avg ≈ 20`, popularity-weighted
  across both accounts (spending activity) and merchants (some merchants are far higher
  volume, e.g. a grocery chain vs. a boutique) — draw the merchant side from merchant
  popularity weights too, not just the account side.
- `alerts`: filtered-subset sampled (§1.3c) from `transactions WHERE is_flagged`, sized at
  `~0.9–1.3×` the flagged-transaction count (a small number of flagged transactions get more
  than one alert type, a few flagged transactions never get an alert opened) — this keeps
  the guard rail from §1.3c satisfied automatically since it's derived from the flagged
  count rather than an unrelated flat 500.

Column notes:
- `transactions.is_flagged`: Bernoulli, rate tuned per §1.6 — target ~3–5% of transactions,
  which at `sf=1.0` (≈20,000 txns) yields 600–1000 flagged rows, comfortably supporting the
  alerts table above; at `sf=0.01` floor-adjusted counts, verify the floor keeps this ≥20
  rows.
- `transactions.is_flagged` odds should correlate with `amount` (higher amounts more likely
  flagged) and with the merchant's `risk_tier` — a light logistic bump, not independent —
  so a downstream fraud model has real signal to find.
- `transactions.is_declined` independent Bernoulli ~4–6%, mildly correlated with
  `account.is_frozen` (frozen accounts should have a much higher decline rate, e.g. >80%,
  since that's what "frozen" means operationally).

### 2.5 p05_hr — People analytics

Business context: a company organized into departments employs people in a manager
hierarchy; each employee has a salary history, periodic performance reviews, and leave
requests.

Entities:
- `departments`: `rows = max(round(30*sf), 5)`.
- `employees`: `rows = max(round(800*sf), 20)`, assigned to departments via popularity
  weighting (departments vary in size — engineering/sales bigger than legal/finance).
  Manager hierarchy: designate the top ~8–10% of employees (by hire date, earliest hires) as
  eligible managers; every other employee's `manager_id` references one of those (popularity
  weighted, so managers have realistic, varying span-of-control rather than perfectly even
  teams — e.g. Gamma-distributed team sizes), and manager-eligible employees may themselves
  have a manager one level up, forming 2–3 levels of hierarchy, not a single flat layer.
- `salaries`: one **base row at hire** plus roughly one additional row every ~18 months of
  tenure (a raise/adjustment history), so `rows(salaries) ≈ Σ over employees of (1 +
  tenure_months/18)` — expect this to land close to today's flat 2,000 at sf=1 but now
  causally tied to tenure rather than an unrelated constant.
- `performance_reviews`: roughly one per employee per year of tenure (annual review cycle),
  `reviewer_id` sampled from that employee's manager chain (not an unrelated random
  employee) — reviews should be a real manager→report relationship.
- `leave_requests`: `rows(employees) * avg_leave_requests_per_year * years_in_window`,
  `avg ≈ 2–3/year`, popularity-weighted (some employees take much more leave than others,
  e.g. parental/medical leave outliers), not flat per employee.

Column notes:
- `employees.hire_date`: spans the full observation window (10 years), weighted toward more
  recent years if department `headcount_target` implies growth, to avoid a flat hiring rate
  that real companies never have.
- `salaries.base_salary`: should scale with `job_title` seniority band (a fixed multiplier
  per band) plus noise, not be independent of title.

### 2.6 p06_logistics — Supply chain / warehouse logistics

Business context: suppliers ship goods (identified by SKU) into warehouses; warehouses hold
inventory snapshots; purchasing issues purchase orders to replenish stock.

Entities:
- `suppliers`: `rows = max(round(100*sf), 5)`.
- `warehouses`: `rows = max(round(20*sf), 3)`.
- A **SKU catalog** of `rows = max(round(200*sf), 50)` distinct SKUs should be introduced as
  a first-class dimension-by-convention list (even without a full `products` table, keep a
  generated array of SKU strings with an attached popularity weight and unit-cost baseline
  per SKU) rather than the current flat 50-SKU pool independent of `sf` — this keeps SKU
  breadth scaling with `sf` like every other dimension. **[CHANGE from dbgen]**.
- `shipments`: `rows(suppliers) * avg_shipments_per_supplier`, `avg ≈ 50`, popularity
  weighted across suppliers (preferred suppliers, `is_preferred=true`, should ship
  meaningfully more often — bias their popularity weight up directly) and across SKUs.
  `received_date = shipped_date + lead_time` where `lead_time` is drawn around the owning
  supplier's `lead_time_days` with small noise (§1.7), not independent.
- `inventory`: one row per `(warehouse, SKU)` pair per snapshot date (a handful of monthly
  snapshots across the window) rather than an unrelated flat 1,000 — this makes `rows =
  rows(warehouses) * n_skus_stocked_per_warehouse * n_snapshots`, and guarantees full
  warehouse×SKU coverage (every warehouse stocks most SKUs, so joins against `shipments`/
  `purchase_orders` by SKU are not sparse).
- `purchase_orders`: `rows(suppliers) * avg_po_per_supplier`, `avg ≈ 20`, popularity
  weighted, `received_qty` should be close to (not always exactly) `ordered_qty` — model
  as `ordered_qty * Beta-shaped fraction near 1` with a small chance of a significant
  shortfall (a partial-fulfillment event), not independent of `ordered_qty`.

### 2.7 p07_saas — B2B SaaS product analytics

Business context: SaaS accounts subscribe to plans, generate in-product usage events and
per-feature usage, and file support tickets.

Entities:
- `accounts`: `rows = max(round(500*sf), 10)`.
- `subscriptions`: mostly one active row per account plus occasional plan-change history —
  `rows(subscriptions) ≈ rows(accounts) * 1.4` (a minority of accounts have upgraded/
  downgraded, producing a second historical row), rather than an unrelated flat 700.
  `mrr` should be a deterministic function of `plan` and `seats` (a fixed per-seat rate per
  plan tier) plus small noise — not independent — so plan/seats/mrr are internally
  consistent (§1.7). Drop the duplicated `renewal_date = end_date` column, or if kept for
  schema-compat reasons, document it explicitly as a denormalized copy, not a separately
  random field.
- `events`: `rows(accounts) * avg_events_per_account`, `avg ≈ 100`, materialized-parent /
  popularity-weighted across accounts by their `health_score` and `arr` (bigger, healthier
  accounts use the product more) with a synthetic `user_id` drawn from a per-account user
  pool sized roughly `seats` from that account's subscription (so `user_id` density per
  account roughly matches its seat count instead of an unrelated flat `accounts*5` range).
- `feature_usage`: `rows(accounts) * n_features * avg_days_active_per_month`, popularity
  weighted like `events`.
- `support_tickets`: filtered/derived — ticket rate should correlate *inversely* with
  `health_score` (unhealthy accounts file more tickets) — popularity-weighted by
  `(100 - health_score)` rather than uniform across accounts, so downstream health-score
  models have a real relationship to detect. **[CHANGE from dbgen]**: today ticket volume
  per account is not tied to health_score at all.

### 2.8 p08_healthcare — Health insurance claims

Business context: patients receive services from providers; claims are filed per encounter,
composed of line items (procedures) and diagnoses.

Entities:
- `patients`: `rows = max(round(1000*sf), 20)`.
- `providers`: `rows = max(round(200*sf), 10)`.
- `claims`: `rows(patients) * avg_claims_per_patient`, `avg ≈ 3`, popularity weighted (a
  small share of patients — the chronically ill — generate disproportionately many claims;
  model this explicitly: draw each patient a "utilization tier" — low/medium/high with
  roughly 70/25/5% weights — and set their claim-count weight from that tier, which is more
  interpretable than a raw Pareto exponent for a healthcare context and gives downstream
  "high-utilizer" cohort models a clean, intentional signal).
- `claim_lines`: `rows(claims) * avg_lines_per_claim`, `avg ≈ 3`, Poisson-shaped count per
  claim, referencing the fixed CPT-code pool with popularity weighting (some procedures are
  far more common than others).
- `diagnoses`: **[FIX]** must reference `claims.claim_id` directly and be named/typed as
  such in the schema (the current column is ambiguously named and, per the source
  investigation, actually samples against the claims id-space despite cosmetic naming that
  suggests claim_lines) — one to a few diagnoses per claim, `rows(diagnoses) ≈
  rows(claims) * avg_diagnoses_per_claim` (`avg ≈ 1.5–2`), from the 100-value ICD pool with
  popularity weighting (a handful of chronic/common diagnoses dominate).

Column notes:
- `claims.total_allowed = total_billed * Beta-shaped fraction, mean ≈ 0.65`;
  `total_paid = total_allowed * Beta-shaped fraction, mean ≈ 0.8`, and `total_paid = 0`
  when `status = 'denied'` — enforce `paid ≤ allowed ≤ billed` exactly (§1.7).
- `claims.denial_reason`: populated only when `status = 'denied'`; denial rate tuned per
  §1.6 (target ~5–8% of claims, checked against the floor at `sf=0.01`).
- `claim_lines.paid_amount` should roll up consistently with its parent claim's
  `total_paid` (generate line-level paid amounts first as fractions of `allowed_amount`,
  then set the claim-level `total_paid`/`total_allowed` as the actual sum across its lines
  rather than two independently random figures) — this is the one place in the whole spec
  where a header total should be a literal rollup of its details, since insurance claims
  really do reconcile this way operationally.

### 2.9 p09_gaming — Mobile/video game analytics

Business context: players progress through levels across play sessions, generating
in-session events, and make in-app purchases.

Entities:
- `players`: `rows = max(round(2000*sf), 20)`.
- `levels`: `rows = max(round(50*sf), 10)`, ordered into a small number of "worlds" with
  monotonically increasing `difficulty`/`par_time_sec`, and `unlock_level` referencing an
  earlier level in the same or previous world (a real progression gate), not an arbitrary
  `level_id - rand(0..3)`.
- `sessions`: `rows(players) * avg_sessions_per_player`, `avg ≈ 5`, strongly popularity
  weighted across players by `is_paid_user` and an implicit "engagement tier" (mirroring the
  healthcare utilization-tier idea in §2.8: most players are casual with few sessions, a
  minority are highly engaged "whales"/regulars with many) — this is the single most
  important skew in this project since purchases and events both derive from session
  activity.
- `events`: materialized-parent sampled from sessions (§1.3b) with `event_ts` constrained to
  fall within `[session_start, session_end]`, at `avg ≈ 8` events per session, referencing
  `level_id` popularity-weighted toward early levels (drop-off funnel — most players see
  early levels far more often than late ones, a real progression funnel pattern).
- `purchases`: filtered/derived from paid sessions only — restrict purchase eligibility to
  `is_paid_user = true` players (do not let free players generate purchase rows at all; today
  purchases likely reference the full player pool) **[CHANGE from dbgen]**, at `rows(paid
  players) * avg_purchases_per_paid_player ≈ 4`, popularity weighted toward the same
  engagement tier as sessions so heavy players also spend more (a standard mobile-game
  monetization pattern — most revenue from a small whale cohort).

### 2.10 p10_energy — Electric utility / smart grid

Business context: substations feed smart/non-smart meters at customer premises; meters
report consumption readings; substations occasionally experience outages.

Entities:
- `substations`: `rows = max(round(50*sf), 5)`.
- `meters`: `rows(substations) * avg_meters_per_substation`, `avg ≈ 20`, popularity weighted
  by substation `capacity_mw` (bigger substations serve more meters) rather than an
  unrelated flat 1,000; `customer_id` should be a 1:1 or near-1:1 relationship with meter
  (most premises have one meter), so size the synthetic customer id-space to
  `rows(meters) * ~1.05` rather than an unrelated flat 2,001 range.
- `consumption_readings`: `rows(meters) * avg_readings_per_meter`, `avg ≈ 500` (e.g.
  half-hourly reads over ~10 days, or daily reads over ~1.5 years — pick one granularity and
  apply consistently), popularity/consumption-weighted by `meter.rated_capacity_kw` and
  `tariff_class` (commercial/industrial meters should read much higher `kwh` than
  residential — draw the Normal's mean per tariff class, not one global mean for every
  meter) **[CHANGE from dbgen]**: today `kwh` uses one global `Normal(5.0, 3.0)` regardless
  of meter type, which erases the residential-vs-commercial signal a real grid dataset would
  have.
- `outage_events`: filtered/derived at the substation level, `rows(substations) *
  avg_outages_per_substation`, `avg ≈ 3–4` over the observation window, `affected_meters`
  drawn as a fraction of that substation's actual downstream meter count (§1.7) rather than
  an independent random integer, and `severity` should correlate with `affected_meters` (more
  meters affected → more likely "major"/"critical") rather than being independent.

---

## 3. Fixed Categorical Vocabularies

For every column that should draw from a fixed set of string values, use the vocabulary and
weights below instead of an unweighted/arbitrary list (per §1.4, weights should be skewed,
not uniform, unless noted as "even split"). Weights are approximate target shares, not exact
requirements — renormalize if a project's own logic needs to add/remove a value. Where a
column's realistic value depends on another column in the same row (e.g. `timezone` on
`region`, `denial_reason` only when `status='denied'`), that dependency is noted explicitly.

### 3.1 p01_iot

| Column | Values (weight) |
|---|---|
| `sites.region` | `north_america` (35%), `europe` (25%), `apac` (25%), `latin_america` (10%), `middle_east_africa` (5%) |
| `sites.timezone` | pick a real IANA zone consistent with the row's `region` (e.g. `America/New_York`, `America/Chicago`, `America/Los_Angeles` for `north_america`; `Europe/London`, `Europe/Berlin`, `Europe/Paris` for `europe`; `Asia/Tokyo`, `Asia/Singapore`, `Asia/Kolkata` for `apac`; `America/Sao_Paulo` for `latin_america`; `Africa/Johannesburg`, `Asia/Dubai` for `middle_east_africa`) — do not draw independently of `region` |
| `devices.device_type` | `temperature_sensor` (30%), `humidity_sensor` (20%), `pressure_sensor` (15%), `multi_sensor` (25%), `gateway` (10%) |
| `devices.model` | 2–3 fixed model strings per `device_type` (e.g. `TS-100`, `TS-200` for `temperature_sensor`; `GW-500`, `GW-900` for `gateway`) — model implies device_type, not drawn independently |
| `devices.firmware` | version-like strings `v{major}.{minor}.{patch}` with `major` in 1–4, weighted toward newer major versions (e.g. v1 5%, v2 15%, v3 40%, v4 40%) |
| `maintenance_logs.action` | `routine_inspection` (35%), `battery_replacement` (25%), `firmware_update` (20%), `repair` (12%), `recalibration` (5%), `decommission` (3%) |

### 3.2 p02_adtech

| Column | Values (weight) |
|---|---|
| `campaigns.channel` | `search` (30%), `social` (25%), `display` (20%), `video` (12%), `native` (8%), `email` (5%) |
| `campaigns.objective` | `conversion` (35%), `awareness` (25%), `consideration` (20%), `retargeting` (20%) |
| `impressions.device` / `clicks.device` | `mobile` (55%), `desktop` (30%), `tablet` (10%), `ctv` (5%) (keep the same distribution and, ideally, the same value per `user_id` across impressions/clicks/conversions for a given user — device doesn't usually change mid-funnel) |
| `impressions.geo` | a fixed pool of 15–20 country or DMA names, weighted toward a handful of large markets (e.g. `US` 40%, `UK` 10%, `CA` 8%, `DE` 7%, `AU` 6%, remainder split across the rest) |
| `impressions.placement` | `banner` (35%), `native` (25%), `video` (25%), `interstitial` (15%) |
| `conversions.conv_type` | `purchase` (45%), `lead` (20%), `signup` (15%), `app_install` (12%), `subscription` (8%) |

### 3.3 p03_ecommerce

| Column | Values (weight) |
|---|---|
| `customers.country` | fixed pool of 15–20 countries, weighted toward the retailer's home markets (e.g. `US` 40%, `CA` 8%, `UK` 8%, `DE` 6%, `FR` 5%, `AU` 5%, remainder split across the rest) |
| `orders.status` | `completed` (75%), `cancelled` (8%), `returned` (7%), `pending` (5%), `processing` (5%) (as in §2.3) |
| `orders.channel` | `web` (55%), `mobile_app` (35%), `phone` (5%), `in_store_kiosk` (5%) |
| `marketplace_orders.marketplace_name` | `Amazon` (55%), `eBay` (20%), `Walmart Marketplace` (15%), `Etsy` (10%) |
| `marketplace_orders.partner_status` | `shipped` (70%), `pending` (12%), `cancelled` (10%), `refunded` (8%) |
| `categories.name` (top-level) | e.g. `Electronics`, `Home & Kitchen`, `Apparel`, `Beauty & Personal Care`, `Sports & Outdoors`, `Toys & Games`, `Books`, `Grocery` — weight `products`/subcategory assignment toward `Electronics`/`Home & Kitchen`/`Apparel` (§2.3) |

### 3.4 p04_fraud

| Column | Values (weight) |
|---|---|
| `accounts.account_type` | `checking` (45%), `savings` (30%), `credit_card` (20%), `business` (5%) |
| `accounts.country` | fixed pool of ~10 countries, weighted toward the issuing bank's home country (e.g. `US` 60%, remainder spread across `CA`, `UK`, `DE`, `FR`, `MX`, etc.) |
| `merchants.category` | `grocery` (18%), `restaurant` (16%), `online_retail` (15%), `gas_station` (12%), `electronics` (10%), `travel` (8%), `entertainment` (8%), `utilities` (7%), `other` (6%) |
| `merchants.risk_tier` | `low` (60%), `medium` (30%), `high` (10%) — `is_flagged` odds (§2.4) should scale with this tier |
| `transactions.channel` | `card_present` (45%), `online` (30%), `mobile_wallet` (15%), `card_not_present` (7%), `atm` (3%) |
| `transactions.currency` | `USD` (85%), `EUR` (6%), `GBP` (4%), `CAD` (3%), `other` (2%) |
| `transactions.response_code` | `approved` (92%), `insufficient_funds` (3%), `do_not_honor` (2%), `expired_card` (1.5%), `invalid_pin` (1%), `fraud_suspected` (0.5%) — must be consistent with `is_declined` (only `approved` when not declined) |
| `alerts.alert_type` | `unusual_amount` (30%), `velocity` (25%), `geo_anomaly` (20%), `card_not_present` (15%), `identity_theft` (10%) |
| `alerts.severity` | `low` (35%), `medium` (35%), `high` (22%), `critical` (8%) |
| `alerts.resolution` (only when `resolved = true`) | `false_positive` (45%), `customer_verified` (30%), `confirmed_fraud` (25%) |

### 3.5 p05_hr

| Column | Values (weight) |
|---|---|
| `departments.division` | `Engineering` (25%), `Sales` (18%), `Customer Success` (12%), `Marketing` (10%), `Operations` (10%), `Finance` (8%), `Product` (8%), `HR` (5%), `Legal` (4%) |
| `departments.location` | fixed pool of office cities (e.g. `New York`, `San Francisco`, `Austin`, `London`, `Berlin`, `Remote`), weighted toward 2–3 hub cities plus a meaningful `Remote` share (~20%) |
| `employees.gender` | `female` (48%), `male` (48%), `nonbinary_other` (4%) |
| `employees.job_title` | banded by division: individual-contributor titles `IC1`…`IC5` (or division-appropriate equivalents like `Software Engineer I/II/Senior/Staff`), `Manager`, `Senior Manager`, `Director`, `VP` — weight so ~65% of employees are IC-band, ~25% manager-band, ~8% director-band, ~2% VP-band; only manager-eligible employees (§2.5) may hold `Manager`/`Director`/`VP` titles |
| `employees.employment_type` | `full_time` (85%), `contractor` (8%), `part_time` (5%), `intern` (2%) |
| `performance_reviews.category` | `exceeds_expectations` (20%), `meets_expectations` (60%), `needs_improvement` (15%), `unsatisfactory` (5%) |
| `leave_requests.leave_type` | `vacation` (55%), `sick` (25%), `parental` (8%), `unpaid` (5%), `bereavement` (4%), `jury_duty` (3%) |

### 3.6 p06_logistics

| Column | Values (weight) |
|---|---|
| `suppliers.country` | fixed pool of ~15 countries with a manufacturing/logistics skew (e.g. `China` 25%, `US` 15%, `Vietnam` 10%, `Mexico` 10%, `Germany` 8%, `India` 8%, remainder spread across others) |
| `suppliers.category` | `electronics_components` (25%), `packaging` (20%), `raw_materials` (18%), `hardware` (15%), `textiles` (12%), `food_ingredients` (10%) |
| `warehouses.region` | `north_america` (35%), `europe` (25%), `apac` (30%), `latin_america` (10%) |
| `shipments.status` | `delivered` (70%), `in_transit` (15%), `delayed` (8%), `customs_hold` (4%), `cancelled` (3%) |
| `purchase_orders.status` | `received` (55%), `approved` (15%), `partially_received` (12%), `submitted` (10%), `draft` (5%), `cancelled` (3%) |

### 3.7 p07_saas

| Column | Values (weight) |
|---|---|
| `accounts.industry` | `Software/SaaS` (20%), `Financial Services` (15%), `Retail/E-commerce` (12%), `Healthcare` (12%), `Manufacturing` (10%), `Media/Entertainment` (10%), `Education` (10%), `Government/Public Sector` (6%), `Other` (5%) |
| `accounts.country` | weighted like p03/p04's country pools toward `US`/`UK`/`CA`/`DE`/`AU` |
| `subscriptions.plan` | `starter` (35%), `professional` (35%), `business` (20%), `enterprise` (10%) — drives `mrr` per-seat rate (§2.7) |
| `events.event_type` | `page_view` (30%), `feature_used` (25%), `login` (15%), `api_call` (12%), `export` (8%), `report_generated` (6%), `invite_sent` (4%) |
| `events.platform` | `web` (65%), `api` (20%), `ios` (8%), `android` (7%) |
| `feature_usage.feature_name` | fixed pool of ~12–15 named features (e.g. `dashboards`, `alerts`, `integrations`, `reporting`, `api_access`, `sso`, `automation`, `exports`), weighted so a handful of "core" features (`dashboards`, `reporting`, `alerts`) dominate usage (~55% combined) |
| `support_tickets.priority` | `low` (40%), `medium` (35%), `high` (18%), `urgent` (7%) |
| `support_tickets.category` | `technical` (30%), `billing` (18%), `bug` (18%), `onboarding` (15%), `feature_request` (12%), `account_access` (7%) |

### 3.8 p08_healthcare

| Column | Values (weight) |
|---|---|
| `patients.gender` | `female` (51%), `male` (48%), `other_unknown` (1%) |
| `patients.plan_type` | `PPO` (35%), `HMO` (30%), `Medicare` (15%), `EPO` (10%), `Medicaid` (7%), `POS` (3%) |
| `patients.state` / `providers.state` | all 50 US states, weighted by real relative population (e.g. `CA`, `TX`, `FL`, `NY` each 6–9%, tapering down through smaller states) rather than an even 1/50 split |
| `providers.specialty` | `primary_care` (25%), `emergency_medicine` (12%), `cardiology` (10%), `orthopedics` (9%), `psychiatry` (8%), `general_surgery` (8%), `dermatology` (7%), `radiology` (7%), `pediatrics` (7%), `oncology` (7%) |
| `claims.claim_type` | `professional` (45%), `institutional` (25%), `pharmacy` (18%), `dental` (7%), `vision` (5%) |
| `claims.status` | `paid` (78%), `pending` (10%), `denied` (7%), `partially_paid` (5%) |
| `claims.denial_reason` (only when `status='denied'`) | `not_medically_necessary` (30%), `out_of_network` (22%), `missing_authorization` (20%), `incorrect_coding` (15%), `coverage_terminated` (8%), `duplicate_claim` (5%) |
| `claim_lines.cpt_code` | pool of 50 CPT-like 5-digit codes; weight so ~10 "common visit" codes (office visits, basic labs, imaging) account for ~50% of lines, remaining 40 codes share the rest |
| `diagnoses.icd_code` | pool of 100 ICD-10-like codes; weight so ~15 common chronic/acute diagnoses (hypertension, diabetes, upper respiratory infection, etc.) account for ~40% of rows, matching real claims data's long tail |

### 3.9 p09_gaming

| Column | Values (weight) |
|---|---|
| `players.country` | weighted country pool similar to p02/p07 (mobile-gaming-skewed: `US` 25%, `Brazil` 10%, `India` 10%, `UK` 8%, `Germany` 6%, `Japan` 6%, remainder spread across others) |
| `players.platform` / `sessions.platform` | `android` (50%), `ios` (35%), `steam` (10%), `playstation` (3%), `xbox` (1.5%), `nintendo_switch` (0.5%) — keep consistent per player across their sessions |
| `players.age_group` | `18_24` (25%), `25_34` (28%), `13_17` (15%), `35_44` (18%), `45_plus` (10%), `under_13` (4%) |
| `levels.world` | fixed pool of 5–8 world names (e.g. `Forest`, `Desert`, `Ice Caverns`, `Volcano`, `Sky Kingdom`), assigned to contiguous blocks of `level_id` in ascending difficulty order, not randomly |
| `levels.difficulty` | `easy` (30%), `medium` (35%), `hard` (25%), `expert` (10%) — should trend harder as `level_id`/world index increases, not be independent of level order |
| `sessions.version` | 4–6 semantic version strings (e.g. `2.4.0`, `2.5.0`, `2.6.1`, `2.7.0`), weighted toward the most recent 1–2 versions (~70% combined) with a long tail of stragglers on older versions |
| `events.event_type` | `level_start` (25%), `level_complete` (18%), `item_collected` (18%), `level_fail` (15%), `tutorial_step` (8%), `achievement_unlocked` (7%), `purchase_prompt_shown` (5%), `session_start` (2%), `session_end` (2%) |
| `purchases.item_type` | `coin_pack` (35%), `skin` (20%), `booster` (18%), `battle_pass` (15%), `character_unlock` (8%), `remove_ads` (4%) |
| `purchases.currency` | `USD` (70%), `EUR` (12%), `GBP` (6%), `BRL` (5%), `JPY` (4%), `other` (3%) |

### 3.10 p10_energy

| Column | Values (weight) |
|---|---|
| `substations.region` | `north_america` (35%), `europe` (30%), `apac` (25%), `latin_america` (10%) |
| `meters.meter_type` | `residential` (75%), `commercial` (20%), `industrial` (5%) — matches real grid customer mix |
| `meters.tariff_class` | conditioned on `meter_type`: residential meters → `residential_standard` (75%) / `residential_tou` (25%); commercial meters → `commercial_standard` (70%) / `commercial_demand` (30%); industrial meters → `industrial_demand` (100%) — do not draw independently of `meter_type` |
| `outage_events.cause` | `weather` (35%), `equipment_failure` (25%), `vegetation` (15%), `vehicle_accident` (10%), `animal_contact` (8%), `planned_maintenance` (6%), `cyberattack` (1%) |
| `outage_events.severity` | `minor` (45%), `moderate` (30%), `major` (18%), `critical` (7%) — should correlate with `affected_meters` as noted in §2.10, not be independent |

---

## 4. Summary of Deliberate Departures from `dbgen/`'s Current Implementation

For quick reference when someone reimplements the generator against this spec:

1. Fact-table row counts should derive from `parent_count * avg_fanout`, not independent
   base constants, so per-parent fan-out stays constant across scale factors (§1.1).
2. FK sampling should default to popularity-weighted (Pareto-shaped), not uniform, for every
   fact→dimension edge, to produce realistic long-tail entity activity (§1.3a).
3. Categorical/status columns should use skewed weights, not uniform draws over their
   vocabulary (§1.4).
4. Monetary/count/duration columns should use log-normal/Gamma/Poisson shapes, not flat
   uniform ranges (§1.4).
5. Rare-event Bernoulli rates must be chosen against an explicit floor/ceiling check at the
   benchmark's smallest actual scale factor, not picked as an isolated "looks about right"
   percentage (§1.6).
6. `p03_ecommerce.reviews` should reference real completed `order_items`, not independent
   random (product, customer) pairs.
7. `p06_logistics`'s SKU catalog should scale with `sf` like every other dimension, instead
   of a fixed 50-value pool.
8. `p07_saas.support_tickets` volume should correlate inversely with `health_score`.
9. `p08_healthcare.diagnoses` should have its claim-referencing column named and typed
   unambiguously as `claim_id` (fixing a naming/grain ambiguity in the current schema), and
   claim-level `total_allowed`/`total_paid` should be literal rollups of their `claim_lines`,
   not independently generated figures.
10. `p09_gaming.purchases` should be restricted to `is_paid_user = true` players only.
11. `p10_energy.consumption_readings.kwh` should be generated per `tariff_class`, not from
    one global distribution, so residential vs. commercial/industrial usage is
    distinguishable.
