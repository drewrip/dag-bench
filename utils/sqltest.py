#!/usr/bin/env python3
"""Build a flattened, fully-inlined SQL benchmark from the dbt projects in projects/.

For every dbt project we treat every model file under models/ as a "view" and
manually inline (via sqlglot) all `ref()`/`source()` references in reverse
topological order, so that every sink model (a model no other model selects
from) ends up as a single deeply-nested query that only touches the raw
source tables. Source table DDL is pulled from the DuckDB `CREATE TABLE`
statements embedded in the Rust data generator (dbgen/src/pXX_*.rs).

Output layout:

    dag-bench-sqltest/
        p01_iot/
            schema.sql
            q1.sql
            q2.sql
            ...
        p02_adtech/
            ...
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

DIALECT = "duckdb"

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = REPO_ROOT / "projects"
DBGEN_SRC_DIR = REPO_ROOT / "dbgen" / "src"
OUTPUT_DIR = REPO_ROOT / "dag-bench-sqltest"

# --------------------------------------------------------------------------
# Jinja resolution
#
# The models in this repo only ever use `{{ ref(...) }}`, `{{ source(...) }}`,
# and two dbt_utils cross-db helpers (`datediff`, `date_trunc`). There is no
# other control-flow jinja (no `{% %}` blocks), so plain regex substitution
# is sufficient to turn each model file into plain SQL.
# --------------------------------------------------------------------------

REF_RE = re.compile(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}")
SOURCE_RE = re.compile(r"\{\{\s*source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}")
_QARG = r"""(?:"([^"]*)"|'([^']*)')"""

DATEDIFF_RE = re.compile(
    r"\{\{\s*datediff\(\s*" + _QARG + r"\s*,\s*" + _QARG + r"\s*,\s*" + _QARG + r"\s*\)\s*\}\}",
    re.DOTALL,
)
DATE_TRUNC_RE = re.compile(
    r"\{\{\s*date_trunc\(\s*" + _QARG + r"\s*,\s*" + _QARG + r"\s*\)\s*\}\}",
    re.DOTALL,
)


def _arg(match: "re.Match[str]", pos: int) -> str:
    """Return capture group `pos` (1-indexed pair) regardless of which quote style matched."""
    return match.group(2 * pos - 1) or match.group(2 * pos)


def _expand_datediff(match: "re.Match[str]") -> str:
    first, second, datepart = _arg(match, 1), _arg(match, 2), _arg(match, 3)
    if datepart == "week":
        day_diff = f"date_diff('day', ({first})::timestamp, ({second})::timestamp)"
        return (
            f"({day_diff} // 7 + case "
            f"when date_part('dow', ({first})::timestamp) <= date_part('dow', ({second})::timestamp) then "
            f"case when {first} <= {second} then 0 else -1 end "
            f"else case when {first} <= {second} then 1 else 0 end end)"
        )
    return f"date_diff('{datepart}', ({first})::timestamp, ({second})::timestamp)"


def _expand_date_trunc(match: "re.Match[str]") -> str:
    datepart, date = _arg(match, 1), _arg(match, 2)
    return f"date_trunc('{datepart}', {date})"


def resolve_jinja(raw_sql: str) -> str:
    sql = DATEDIFF_RE.sub(_expand_datediff, raw_sql)
    sql = DATE_TRUNC_RE.sub(_expand_date_trunc, sql)
    # Fully qualify source tables with the `main` schema. Since we inline every
    # upstream model's body verbatim, a bare source table name (e.g. `accounts`)
    # can end up nested inside a downstream CTE of the very same name (a common
    # dbt idiom: `with accounts as (select * from {{ ref('stg_accounts') }})`),
    # which DuckDB then misreads as a self-referencing CTE. Schema-qualifying
    # the source table makes it unambiguous.
    sql = SOURCE_RE.sub(lambda m: f"main.{m.group(2)}", sql)
    sql = REF_RE.sub(lambda m: m.group(1), sql)
    if "{{" in sql or "{%" in sql:
        raise ValueError(f"unresolved jinja remains:\n{sql}")
    return sql


# --------------------------------------------------------------------------
# Model discovery
# --------------------------------------------------------------------------


class Model:
    def __init__(self, name: str, path: Path, raw_sql: str):
        self.name = name
        self.path = path
        self.raw_sql = raw_sql
        self.refs = set(REF_RE.findall(raw_sql))


def find_dbt_projects() -> list[Path]:
    return sorted(
        p for p in PROJECTS_DIR.iterdir() if (p / "dbt_project.yml").is_file()
    )


def collect_models(project_dir: Path) -> dict[str, Model]:
    models: dict[str, Model] = {}
    for sql_path in sorted((project_dir / "models").rglob("*.sql")):
        name = sql_path.stem
        raw_sql = sql_path.read_text()
        if name in models:
            raise ValueError(f"duplicate model name {name!r} in {project_dir}")
        models[name] = Model(name, sql_path, raw_sql)
    return models


def topological_order(models: dict[str, Model]) -> list[str]:
    """Kahn's algorithm; returns upstream-first order."""
    remaining = {name: set(m.refs) for name, m in models.items()}
    order: list[str] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            raise ValueError(f"cycle detected among models: {sorted(remaining)}")
        for name in ready:
            order.append(name)
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return order


def find_sinks(models: dict[str, Model]) -> list[str]:
    referenced: set[str] = set()
    for m in models.values():
        referenced.update(m.refs)
    return sorted(name for name in models if name not in referenced)


# --------------------------------------------------------------------------
# Inlining
# --------------------------------------------------------------------------


def inline_model(model: Model, registry: dict[str, exp.Expression]) -> exp.Expression:
    """Parse a model's (jinja-resolved) SQL and substitute every `ref()`
    table reference with a copy of the already-fully-inlined upstream
    expression, wrapped as a subquery under the same local alias."""
    sql = resolve_jinja(model.raw_sql)
    expression = sqlglot.parse_one(sql, dialect=DIALECT)

    for scope in traverse_scope(expression):
        for alias, source in list(scope.sources.items()):
            if isinstance(source, exp.Table) and source.name in registry:
                inlined = registry[source.name].copy()
                subquery = inlined.subquery(alias)
                source.replace(subquery)

    return expression


def build_registry(models: dict[str, Model]) -> dict[str, exp.Expression]:
    registry: dict[str, exp.Expression] = {}
    for name in topological_order(models):
        registry[name] = inline_model(models[name], registry)
    return registry


# --------------------------------------------------------------------------
# Source table DDL, pulled from the Rust data generator
# --------------------------------------------------------------------------


SCHEMA_SQL_CALL_RE = re.compile(
    r"crate::common::schema_sql\(\s*\"(.*?)\"\s*,\s*no_constraints\s*,?\s*\)", re.DOTALL
)


def extract_schema_ddl(project_dir: Path) -> str:
    rust_path = DBGEN_SRC_DIR / f"{project_dir.name}.rs"
    if not rust_path.is_file():
        raise FileNotFoundError(f"no dbgen source found for {project_dir.name}: {rust_path}")
    rust_src = rust_path.read_text()
    match = SCHEMA_SQL_CALL_RE.search(rust_src)
    if not match:
        raise ValueError(f"could not find schema_sql(...) call in {rust_path}")
    batch = match.group(1)
    statements = [s.strip() for s in batch.split(";") if s.strip()]
    creates = [s for s in statements if s.upper().startswith("CREATE TABLE")]
    if not creates:
        raise ValueError(f"no CREATE TABLE statements found in {rust_path}")
    ddl = ";\n\n".join(" ".join(s.split()) for s in creates) + ";\n"
    return ddl


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def process_project(project_dir: Path) -> None:
    project_name = project_dir.name
    out_dir = OUTPUT_DIR / project_name
    out_dir.mkdir(parents=True, exist_ok=True)

    models = collect_models(project_dir)
    sinks = find_sinks(models)
    if not sinks:
        raise ValueError(f"no sink models found in {project_name}")

    registry = build_registry(models)

    schema_ddl = extract_schema_ddl(project_dir)
    (out_dir / "schema.sql").write_text(schema_ddl)

    print(f"{project_name}: {len(models)} models, {len(sinks)} sink(s)")
    for i, sink in enumerate(sinks, start=1):
        query_sql = registry[sink].sql(dialect=DIALECT, pretty=True) + ";\n"
        out_path = out_dir / f"q{i}.sql"
        out_path.write_text(query_sql)
        print(f"  q{i}.sql <- {sink}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for project_dir in find_dbt_projects():
        process_project(project_dir)


if __name__ == "__main__":
    main()
