# p14_astronomer: vendored de-bench copperline warehouse

Upstream: https://github.com/astronomer/de-bench (Apache-2.0)
Commit: `5dcfc2b81b13dcf58cff49e192e97f1ae05f8dbc` (also recorded in `UPSTREAM`)
Project: `worlds/copperline/workspace/dbt/copperline_analytics`, the dbt warehouse of
de-bench's "copperline" world (a home, hardware and outdoor retailer). de-bench's tasks
(Airflow DAGs, tickets and their checks) and the world's second, legacy dbt project
(`northwave_reporting`) are not vendored.

`models/`, `macros/` (except `macros/dagbench/`), `tests/`, `models/sources.yml`,
`UPSTREAM` and all of `utils/astronomer_gen/` are generated. To update upstream, check
out a new commit, update `UPSTREAM_COMMIT` in `utils/astronomer_vendor.py`, and run:

    .venv/bin/python utils/astronomer_vendor.py <path to de-bench checkout>

Every rewrite asserts that it matched, so upstream changes fail loudly.

## The DAG

142 models: 66 views, 75 tables and 1 incremental (`fct_order_line`), over 46 source
tables. One table, `ops_build_manifest`, is built from the dbt graph at compile time and
reads no data. Staging
views (one per source table) feed shared and per-team intermediate views, which feed
conformed dimensions and six teams' marts (commerce, customer, finance, growth, supply,
platform). The longest chain is 9 models. The hubs are `int_orders_enriched` and
`dim_date` (10 consumers each), `int_net_sales_lines` (8) and `dim_product` (7).

## Changes from upstream (done by `utils/astronomer_vendor.py`)

- **Models, macros and tests.** `seeds/` (`markets.csv`, read by nothing), the committed
  `manifest/` and the project files are not vendored.
- **Sources.** `models/shared/staging/_sources.yml` becomes `models/sources.yml`, every
  table read from `main` (DuckDB) or the target schema (Postgres) as
  `src_<schema>__<table>`, the name `generate_data.py` gives the generator's
  `<schema>.<table>` (`raw.*` and `ops.*`). Freshness settings are dropped.
- **Run audit dropped.** The `on-run-start` / `on-run-end` hooks (`macros/audit.sql`)
  write wall-clock rows about the dbt invocation into `ops_run.dbt_run_events`, and
  `ops_audit_log` (incremental) pairs them. All three are removed: they record the run,
  not the data.
- **Snapshots as models.** `dim_product` reads the snapshots `snap_product_price` and
  `snap_product_attrs`, so upstream's build order is `dbt snapshot` then `dbt run`.
  Each becomes a table model in `models/shared/snapshots/` that builds what a first
  `dbt snapshot` writes: the snapshot query plus `dbt_scd_id`, `dbt_updated_at`,
  `dbt_valid_from` (the `updated_at` column for the timestamp strategy, `var('ds')` for
  the check strategy) and a NULL `dbt_valid_to`. `dbt run` builds the whole DAG from cold.
- **dbt-utils 0.8.6 -> 1.3.3, dbt-core 1.6 -> current.** `dbt_utils.surrogate_key` ->
  `generate_surrogate_key`; with `surrogate_key_treat_nulls_as_empty_strings` (kept) it
  hashes the same string, so every key keeps upstream's value.
- **One schema.** Upstream lands models in the `+schema` names verbatim (`stg`, `int`,
  `marts`, `ops_analytics`) through its own `generate_schema_name`; that macro is removed
  and `macros/dagbench/generate_schema_name.sql` puts everything in the target schema.
- **Fixed "now".** Upstream already reads "today" only from `var('ds')` (default: the
  `WORLD_TODAY` env var, else 2026-06-15); `dbt_project.yml` pins it to 2026-06-15, the
  generator's `today`.
- **Ties.** Two places pick rows over orderings with duplicates, so the result differed
  between engines and between two DuckDB runs. A unique last key is added:
  `int_orders_enriched` (best payment intent per order: `intent_id`; order refs are
  recycled, so candidates tie) and `agg_customer_rfm` (quintiles over tied counts:
  `customer_id`).
- **Postgres portability.** DuckDB-only SQL becomes a `macros/dagbench/portability.sql`
  macro that renders upstream's SQL on DuckDB:
  - `date_diff`, `strftime`, `json_extract_string`, `string_split`, `list_contains`,
    `isodow`, `dayname`. On Postgres `date_diff` is `dbt.datediff` cast to bigint
    (DuckDB's type; dbt's is double precision for sub-day parts).
  - The two `ASOF LEFT JOIN`s in `int_inventory_position_daily` (the movement log's
    running total at the row's day, less the one at its snapshot's day) become, on
    Postgres only, a running sum of the day's movements over the rows carried from one
    snapshot. Equivalent because the carried rows are every calendar day from the
    snapshot (or the window start, before which there are no movements); see the comment
    in the script.
  - `macros/money.sql` (`to_base_cents`, `apply_bps`): DuckDB divides decimals in double
    precision and casts to bigint rounding half to even; Postgres divides in numeric and
    rounds half away from zero, so every negative half-cent tie (all USD credits) was a
    cent apart. The quotient is cast to `double precision`, DuckDB's own type.
  - `DESC` -> `DESC NULLS LAST` (DuckDB's default; Postgres sorts NULLs first).

## Hand-written

- `dbt_project.yml`: upstream's materializations and tags (without `+schema`), `ds`
  and upstream's vars.
- `macros/dagbench/`: the macros above.
- `load_postgres.py`: loads text columns with `COLLATE "C"`, so string sorts and
  comparisons (`string_agg(... order by ...)`, SKU ranges) are bytewise as in DuckDB.
- `utils/astronomer_data.py`: source data, see SPEC.md.

## Verified

At sf=0.05 and sf=1, `dbt run` builds all 142 models on DuckDB and on Postgres, and every
model has the same row count, per-column non-null counts, numeric sums and text-value
checksums on both engines, except `ops_build_manifest.target_schema`, which records the
schema name. At sf=1 (16 threads) `dbt run --full-refresh` takes ~2.5 minutes on DuckDB
and ~27.5 on Postgres, where four models run concurrently for most of it:
`int_promo_exposure` (~26 minutes, the critical path), `int_inventory_position_daily`,
`int_gl_postings_unified` and `fct_order_line` (14-17 minutes each). A run that leaves
`fct_order_line` incremental takes ~21 minutes on Postgres.

## Known gaps

- **Data tests.** Upstream's 622 tests are vendored. 12 fail by design (de-bench's
  `DBT-NOTES.md`: 9 encode rules a later "quality contract" retired, 3 find real
  disagreements between feeds it plants for agents to investigate). `dbt run`, not
  `dbt build`, is what's verified; `dbt build` skips everything downstream of a failing
  test.
- **Planted faults.** The world deliberately contains the kinds of mess de-bench's
  tickets are about (recycled order references, a quarter where two payment processors
  both settle the same payments, a customer re-key, a SKU spelled two ways across feeds).
  The models handle them as upstream does; they are part of the workload, not bugs.
- **Incremental model.** `fct_order_line` accumulates across runs on a database that
  isn't rebuilt (its window is the 14 days before `ds`); compare with `--full-refresh`.
- **Numeric types.** Upstream relies on DuckDB's typing (`sum`, `avg`, decimal division),
  which Postgres types as unconstrained `numeric`. Left as-is, as in p11-p13.
