# dag-bench


## Benchmarks

This repository contains 10 dbt projects (`p01`–`p10`) that are used to benchmark SQL transformation engines, plus 6 microbenchmarks (`m01`–`m06`).

## Microbenchmarks

Microbenchmarks are minimal DAGs that each test one optimizer capability. They reuse p01_iot's generated data, with sources `readings` (S_1) and `devices` (S_2). Models are named `a_*`, `b_*`, … after their role.

- `m01_bad_view_mat`: view A feeds tables B, C and D. A is a wide, non-reducing join, and each consumer's selective filter is pushed into the scan, so materializing A is a bad choice.
- `m02_hidden_view_mat`: view A feeds tables B, C and D. A is an expensive window + aggregate with a tiny output, so materializing it is a good choice. Each consumer's filter is pushed below A's window and aggregate, so no consumer plan contains A's subplan, its cost or its cardinality.
- `m03_good_shared_subplan`: tables A and B both read S_1 and S_2. They are written in different SQL styles, but after normalization they share a scan/filter/join and a `(model, day)` pre-aggregate that subsumes both. Materializing that pre-aggregate pays off.
- `m04_bad_shared_subplan`: same topology as m03. A and B share a join + `(device, hour)` aggregate that barely reduces its input. Each model's filter is pushed into the scans, so materializing the shared part is a bad choice.
- `m05_good_small_view_mat`: view A feeds tables B, C and D. A is an expensive `(site, week)` aggregate (distinct counts and percentiles) that is ~300x smaller than its input. Each consumer filters only on aggregate outputs, which cannot be pushed below the group by, so materializing A is a good choice.
- `m06_bad_small_view_mat`: view A feeds tables B, C and D. A is a very selective join (v1-firmware gateways, ~1/160 of readings) with a tiny device side. Each consumer's own filter on a readings column is pushed into the scan, and the surviving rows are streamed through the join, so materializing A is a bad choice. `readings` is not clustered, so each inlined consumer still scans it. Inlining wins on DuckDB (columnar, narrow scans), but on Postgres three full row-store scans cost more than materializing A once.

## Numeric types

Every numeric column the benchmark produces must have an explicit precision and scale. Arbitrary-precision `NUMERIC` isn't standard SQL and has no Arrow equivalent. Postgres returns an unconstrained `NUMERIC` from `round()`, `sum()`, `avg()`, `min`/`max`, `extract()` and numeric arithmetic, so wrap the outermost expression of such a column in a cast: `cast(round(x, 2) as numeric(10,2))`, or `cast(sum(n) as bigint)` for integer-valued results.

`utils/check_numeric_precision.py` lints the models for `numeric`/`decimal` written without `(p,s)`. With `--postgres`, it also fails on any unconstrained numeric column in the built Postgres schemas. Run it after `DBT_TARGET=postgres dbt build`.
