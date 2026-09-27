# dag-bench


## Benchmarks

This repository contains 10 dbt projects that are used to benchmark SQL transformation engines.

## Numeric types

Every numeric column the benchmark produces must have an explicit precision and scale. Arbitrary-precision `NUMERIC` isn't standard SQL and has no Arrow equivalent. Postgres returns an unconstrained `NUMERIC` from `round()`, `sum()`, `avg()`, `min`/`max`, `extract()` and numeric arithmetic, so wrap the outermost expression of such a column in a cast: `cast(round(x, 2) as numeric(10,2))`, or `cast(sum(n) as bigint)` for integer-valued results.

`utils/check_numeric_precision.py` lints the models for `numeric`/`decimal` written without `(p,s)`. With `--postgres`, it also fails on any unconstrained numeric column in the built Postgres schemas. Run it after `DBT_TARGET=postgres dbt build`.
