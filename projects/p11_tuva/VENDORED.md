# p11_tuva: vendored Tuva Core

Upstream: https://github.com/tuva-health/tuva-core (dbt project `the_tuva_project`, Apache-2.0)
Commit: `465927a0512e96ad480eb8c466a0c01598878b75` (also recorded in `seed_manifest.json`)
Data assets: `s3://tuva-public-resources/tuva-core/1.0.0/`

`models/`, `macros/`, `models/sources.yml`, `models/input_layer_mapping/` and
`seed_manifest.json` are generated. To update upstream, check out a new commit and run:

    .venv/bin/python utils/tuva_vendor.py <path to tuva-core checkout>

That regenerates everything except `macros/dagbench/`, then re-applies the
Postgres fixes below.

## Changes from upstream (done by `utils/tuva_vendor.py`)

- **Core only.** `models/data_quality`, `models/parity`, every `*_unit_tests.yml`,
  the root `tests/` singular tests, `integration_tests/` and `scripts/` are not vendored.
  Domain vars are set in `dbt_project.yml` (claims, clinical, provider attribution on).
- **Seeds become sources.** The 86 seeds (headers-only CSVs whose rows upstream loads from
  S3 in a `post_hook`) are downloaded by `generate_data.py` into `data/warehouse.duckdb`.
  Every `ref('<seed>')` is rewritten to `source('tuva_seeds', '<seed>')` (synthetic inputs:
  `source('tuva', ...)`), and the seed loader macros are dropped. Column types come from the
  upstream seed YAML, rendered for DuckDB, in `seed_manifest.json`.
- **Input Layer mappings.** Upstream expects the parent project to provide `patient`,
  `medical_claim`, ... models. `models/input_layer_mapping/` is a copy of upstream's
  `integration_tests/models` reading `source('tuva', 'synthetic_data__*')`, minus the
  integration-only `x_*`/`ext_*` extension fixture columns. They are views.
- **Project rename.** `the_tuva_project` -> `p11_tuva` everywhere (macro namespaces, dispatch).
- **Postgres patches** (`PATCHES` in the script, exact-text replacements that must each match
  once): `provider_attribution__provider_ranking` casts a bare `null as performance_year` to
  int (Postgres types it text and the union fails).
- **Overridden macros.** Upstream definitions of any macro redefined in `macros/dagbench/`
  are deleted.

## Hand-written (`macros/dagbench/`)

- `generate_schema_name`: every relation goes in `target.schema`, like our other projects
  (upstream fans out to `input_layer`, `core`, `terminology`, ...).
- `generate_alias_name`: relation name = node name. Upstream aliases (`core__patient` ->
  `core.patient`) would collide once schemas collapse.
- `postgres_compat.sql`: `postgres__try_to_cast_int/_date/_datetime` return null on bad
  input like DuckDB's `try_cast` (upstream's raise, e.g. HCPCS `1036F` -> integer).

Upstream model materializations (193 tables, 82 views, 3 ephemeral) are deliberately kept
as-is: the point of this project is a real-world DAG.
