# p13_snowflake: vendored data-eng-bench retail warehouse

Upstream: https://github.com/Snowflake-Labs/data-eng-bench (Apache-2.0)
Commit: `a3278ad102829a6084dde086244a0ef665a8011c` (also recorded in `UPSTREAM`)
Project: `base-image/dbt_models_duckdb` (dbt project `retail_dw_master`), the shared retail
warehouse every data-eng-bench task builds on. The 103 tasks themselves (their broken models
and reference solutions) are not vendored.

`models/`, `macros/` (except `macros/dagbench/`), `models/sources.yml` and `UPSTREAM` are
generated. To update upstream, check out a new commit, update `UPSTREAM_COMMIT` and
`RETAIL_SHA256` in `utils/retail_data.py`, and run:

    .venv/bin/python utils/retail_vendor.py <path to data-eng-bench checkout>

The script needs upstream's `retail.duckdb` (to resolve sources and column lists); it
downloads and caches it like `generate_data.py` does. Every rewrite asserts that it matched,
so upstream changes fail loudly.

## The DAG

2,356 models, 24 disabled upstream, so 2,332 build: 1,819 views, 511 tables, 2 incremental.
It is wide and shallow: ~1,500 staging models read sources directly, the longest chain is 5
models, and the hubs are `fct_sales` (112 consumers) and `stg_orders__orders` (107). It is
deliberately messy (`fct_sales_v1`, `fct_sales_FINAL_v2`, `tmp_marcus_debug`,
`int_orders_TESTING`, ...): upstream's tasks are about working in such a warehouse.

## Changes from upstream (done by `utils/retail_vendor.py`)

- **Models and macros only.** `analyses/`, `seeds/`, `snapshots/`, `tests/` and `scripts/`
  are not vendored (no model refs a snapshot).
- **Sources.** Upstream's `_sources.yml` files are merged into `models/sources.yml`, every
  table read from `main` (DuckDB) or the target schema (Postgres) as
  `src_<schema>__<table>`, the name `generate_data.py` gives it (prefixed: upstream has
  models named e.g. `raw_legacy__glacct`). Upstream's `enterprise_db` source (566 refs)
  declares a schema per table, which dbt ignores; it only works because retail.duckdb ships
  571 alias views `main.X -> <SCHEMA>.X`, and 95 of the declared schemas are wrong. Each
  table is pointed at what upstream's run actually reads. The 36 declared tables that don't
  exist in retail.duckdb (all read only by disabled models) are dropped.
- **Seeds.** The one seed a model refs (`seed_manual_currency_corrections`) and the seeds
  `enterprise_db` reads via `main` are read as sources from retail.duckdb's loaded copies.
- **Broken refs.** `stg_contacts_COPY` refs `stg_contacts`; the model is `STG_CONTACTS`.
  Three models read `"ORDERS"."<TABLE>"` by name; now `source('orders', ...)`.
- **Materializations.** Upstream configures `staging`/`intermediate` as views and `marts`
  as tables under the key `retail_warehouse`, which isn't the project name, so it never
  applied. `dbt_project.yml` applies it (inline configs still win); upstream's per-layer
  `+schema` is dropped, so everything is in the target schema.
- **Same SQL on every target.** `ts_customers__new_customers_weekly`'s
  `target.name == 'dev'` branches (upstream runs as `dev`) are pinned on.
- **Fixed "now".** `current_date` / `current_timestamp` (~1,000 uses: audit columns and
  "days since") are `dagbench_current_date()` / `dagbench_current_timestamp()`, var
  `dagbench_as_of_date` = 2026-01-08, the day after the data's last order.
- **Deterministic fakes.** `hash(_id)` (DuckDB's 64-bit hash, used to fake columns from
  ids) becomes `dagbench_hash`, md5-based and identical on both engines;
  `dim_exchange_rates`' `RANDOM()` rates become `dagbench_random`, seeded by (date,
  currency). Values differ from upstream's, which were arbitrary anyway.
- **Postgres portability.** DuckDB-only SQL becomes portable SQL or a
  `macros/dagbench/portability.sql` macro that renders upstream's SQL on DuckDB:
  - `SELECT * EXCLUDE (...)` (464 staging models): dropped where the next CTE selects
    explicit columns, else spelled out as the source table's columns.
  - `date_diff` / `datediff` -> `dagbench_datediff` (`dbt.datediff` on Postgres); a
    `date_diff` that is divided is cast to double first (DuckDB's integer `/` is float
    division, Postgres' truncates).
  - `TRY_CAST` -> `dagbench_try_cast` (`pg_input_is_valid`, Postgres 16+). Dates and
    timestamps must also have DuckDB's shape: upstream's data has `07/23/2020`, `20231117`
    and epoch strings on purpose, NULL in DuckDB but accepted by Postgres' parser.
  - `strptime`, `TRY_STRPTIME`, `regexp_matches`, `epoch_ms`, `dayofweek`, `dayname`,
    `monthname`, `last_day`, `round(<double>, n)` -> macros; `INTERVAL (n) DAY`,
    `interval 'n quarter'`, `extract(dayofweek ...)`, `DOUBLE`, `range()`, `QUALIFY`,
    trailing commas before `FROM`: portable rewrites.
  - `DESC` -> `DESC NULLS LAST`: DuckDB's default; Postgres sorts NULLs first, which
    changes `LIMIT` and `ROW_NUMBER` picks.
  - Duplicate output column names (DuckDB renames them `<col>_1`, Postgres rejects them)
    get DuckDB's names explicitly (5 models).
- On DuckDB, every rewrite except the fixed "now", the deterministic fakes, the
  materializations and the dev branch compiles to upstream's SQL or an equivalent.

## Hand-written

- `dbt_project.yml`: materializations, `dagbench_as_of_date`, upstream's vars.
- `macros/dagbench/portability.sql`: the macros above.
- `load_postgres.py`: loads text columns with `COLLATE "C"`. Upstream compares and sorts
  strings (e.g. varchar dates `BETWEEN '2024-04-01' AND ...`), and the benchmark database's
  default collation orders them differently from DuckDB's bytewise comparison.
- `utils/retail_data.py`: source data, see SPEC.md.

## Known gaps

- **Ties.** Upstream picks rows with `ROW_NUMBER()` and running sums over orderings that
  have ties, so the result is not defined, and the engines break ties differently:
  `stg_vbap`, `stg_order_lines`, `stg_trans_lines` (234 / 15 / 239 tied
  `(variant_id, updated_at, created_at)` groups), `dim_trans_lines` (downstream),
  `rpt_customer_loyalty_balance`, plus `rpt_campaign_roi_analysis` and
  `rpt_cannibalization_analysis`, which also differ between two DuckDB runs. Every other
  model gives the same row count, non-null counts and numeric sums on DuckDB and Postgres,
  and on repeated DuckDB runs.
- **Incremental models** (`dq_late_arriving_facts`, `ts_customers__new_customers_weekly`)
  accumulate rows across runs on a database that isn't rebuilt; compare with
  `--full-refresh`.
- **Numeric types.** Upstream relies on DuckDB's typing (`sum`, `avg`, `round`, numeric
  arithmetic), which Postgres types as unconstrained `numeric`. Left as-is, as in p11_tuva
  and p12_stripe.
- **Data tests** in upstream's model YAML (913) are vendored but not maintained;
  `dbt run` (not `dbt build`) is what's verified.
