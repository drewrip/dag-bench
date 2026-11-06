# p12_stripe: vendored dbt_stripe

Upstream: https://github.com/fivetran/dbt_stripe (dbt package `stripe`, Apache-2.0)
Version: 1.10.2, commit `7f5d6ae0d474b72b10ed765b7062725f1e5771b1` (also recorded in `UPSTREAM`)

`models/`, `macros/` (except `macros/dagbench/`), `models/sources.yml` and `UPSTREAM` are
generated. To update upstream, check out a new commit and run:

    .venv/bin/python utils/stripe_vendor.py <path to dbt_stripe checkout>

## Changes from upstream (done by `utils/stripe_vendor.py`)

- **Everything vendored.** All of `models/` and `macros/`. `analyses/`, `integration_tests/`
  (seeds and consistency tests) and `docs/` are not.
- **Sources.** `models/staging/src_stripe.yml` becomes `models/sources.yml` (where
  `load_postgres.py` looks), reading `main` on DuckDB and the target schema on Postgres.
  `plan` and `subscription_history` are dropped: we feed `price` and `subscription`
  (`stripe__using_price: true`, `stripe__using_subscription_history: false`).
- **Project rename.** `stripe` -> `p12_stripe` for the package's own macro calls
  (`stripe.convert_values(` ...) and dispatch namespace. The source name (`stripe`), the
  `stripe__*` vars and fivetran_utils' `package_name='stripe'` are unchanged.
- **Fixed date window.** Upstream sizes `int_stripe__date_spine` and the month spine in
  `stripe__subscription_item_mrr_report` from `current_date` plus run-time queries
  (`dbt_utils.get_single_value`, only under `dbt run/build`; `dbt compile` falls back to
  "last month to today"). Both are replaced by the generated data's window, vars
  `dagbench_start_date` / `dagbench_as_of_date`, so compiled SQL doesn't depend on the day
  or on run vs compile. "This month" in `stripe__customer_overview` (`current_timestamp`) is
  `dagbench_as_of_date` too. The script asserts no `current_date` / `current_timestamp` /
  `get_single_value` is left.

## Hand-written

- `dbt_project.yml`: feature vars (all upstream features on, plus credit notes), the window
  vars, upstream materializations (staging tables over `tmp` views, intermediate tables with
  two ephemeral, mart tables).
- `macros/dagbench/generate_schema_name`: every relation in `target.schema` (upstream uses
  `<schema>_stripe` / `<schema>_stg_stripe`).
- `utils/stripe_data.py`: source data, see SPEC.md. Upstream has only ~260 rows of seeds.

## Known gaps

- Upstream uses `dbt.type_numeric()` and sums of bigints, which Postgres types as
  unconstrained `numeric` (136 columns, flagged by `utils/check_numeric_precision.py
  --postgres`). They are left as-is, as in p11_tuva.
- So DuckDB and Postgres agree on every built table (row counts and numeric column sums)
  except `stripe__subscription_item_mrr_report.month_discount_applied`, which differs by a
  few ppm: it's a ratio of numerics, and Postgres numeric division rounds differently.
