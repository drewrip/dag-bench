#!/usr/bin/env python3
"""Vendor Snowflake-Labs/data-eng-bench's base retail dbt project into projects/p13_snowflake.

Usage: utils/retail_vendor.py <path to data-eng-bench checkout>

Vendors `base-image/dbt_models_duckdb` (the shared warehouse every task builds
on), not the tasks. Re-running regenerates models/, macros/ (except
macros/dagbench/) and UPSTREAM from the checkout. Files we own
(dbt_project.yml, profiles.yml, packages.yml, generate_data.py,
load_postgres.py, VENDORED.md, macros/dagbench/) are left alone. Needs
retail.duckdb to resolve sources and column lists; it is downloaded (and
cached) by utils/retail_data.py. See projects/p13_snowflake/VENDORED.md.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.retail_data import UPSTREAM_COMMIT, download_retail, physical_name, raw_tables  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DST = REPO_ROOT / "projects" / "p13_snowflake"
UPSTREAM_PROJECT = Path("base-image") / "dbt_models_duckdb"
SOURCE_SCHEMA = "{{ 'main' if target.type == 'duckdb' else target.schema }}"

# Seeds the models ref(); read from retail.duckdb's `main` as sources.
SEEDS = ["seed_manual_currency_corrections"]

# Exact-text fixes, applied after the generic rewrites: (file, old, new[, count]).
# Each must match exactly `count` (default 1) times.
_UNION_STAR = ("AS _source_system,\n        *\n    FROM",
               "AS _source_system,\n        _id, _loaded_at, _source_system AS _source_system_1, _source_table, _row_hash\n    FROM")
TEXT_REPLACEMENTS = [
    # A `_COPY` of STG_CONTACTS that refs the model by its lower-case name; dbt
    # node names are case-sensitive, so upstream's ref never resolves.
    ("models/staging/raw_sfdc/stg_contacts_COPY.sql", "ref('stg_contacts')", "ref('STG_CONTACTS')"),
    # Duplicate output columns, which DuckDB renames `<col>_1` and Postgres
    # rejects: spelled out with DuckDB's names.
    ("models/marts/dimensions/dim_dim_employee.sql", "-- Attributes\n        employee_id,",
     "-- Attributes\n        employee_id AS employee_id_1,"),
    ("models/marts/dimensions/dim_dim_channel.sql", "-- Attributes\n        channel_id,",
     "-- Attributes\n        channel_id AS channel_id_1,"),
    *[(f"models/intermediate/joins/int_{u}_unioned.sql", *_UNION_STAR, 2) for u in ("campaigns", "refunds", "subscriptions")],
    # Upstream runs as target `dev`, which takes this model's dev-only branches
    # (a 90-day filter and a row limit). Pinned, so every target builds the same SQL.
    ("models/marts/time_series/customers/ts_customers__new_customers_weekly.sql",
     "{% if target.name == 'dev' %}", "{% if true %}{# dag-bench: was target.name == 'dev' #}", 2),
    ("models/marts/procurement/fct_supplier_orders.sql", "round(sa.min_order_value, 2) as min_order_value,",
     "round(sa.min_order_value, 2) as min_order_value_1,"),
    # round(<double>, n) without a visible cast (see DOUBLE_CAST).
    ("models/marts/core/dim_exchange_rates.sql", "round(rate, 6) AS rate,",
     "{{ dagbench_round_double('rate', '6') }} AS rate,"),
    # QUALIFY (DuckDB only) as a filtered subquery.
    ("models/marts/customer/dim_customer_master.sql",
     """default_shipping as (
    select
        customer_id,
        city as shipping_city,
        state_province as shipping_state,
        country_code as shipping_country,
        postal_code as shipping_postal_code
    from customer_addresses
    where is_default_shipping
    qualify row_number() over (partition by customer_id order by created_at DESC NULLS LAST) = 1
)""",
     """default_shipping as (
    select customer_id, shipping_city, shipping_state, shipping_country, shipping_postal_code
    from (
        select
            customer_id,
            city as shipping_city,
            state_province as shipping_state,
            country_code as shipping_country,
            postal_code as shipping_postal_code,
            row_number() over (partition by customer_id order by created_at DESC NULLS LAST) as _rn
        from customer_addresses
        where is_default_shipping
    ) ranked
    where _rn = 1
)"""),
    # range() (DuckDB, end-exclusive) as generate_series (both, end-inclusive).
    ("models/marts/core/dim_exchange_rates.sql",
     """    SELECT CAST(range AS DATE) AS rate_date
    FROM range(DATE '2020-01-01', {{ dagbench_current_date() }} + INTERVAL '1 day', INTERVAL '1 day')""",
     """    SELECT CAST(d AS DATE) AS rate_date
    FROM generate_series(CAST('2020-01-01' AS TIMESTAMP), CAST({{ dagbench_current_date() }} AS TIMESTAMP), INTERVAL '1 day') AS spine(d)"""),
]

# Staging template 1 (279 models): dedupe into `cleaned`, then select explicit
# columns from it in `renamed`. Dropping EXCLUDE doesn't change the output.
EXCLUDE_ROW_NUM = "SELECT * EXCLUDE (row_num) FROM deduplicated WHERE row_num = 1"
# Staging template 2 (185 models): `select * exclude (_rn)` is the model's
# output, over `source` = `select * from {{ source(...) }}`. Spelled out as the
# source table's columns.
EXCLUDE_RN = "select * exclude (_rn)"
# A few models read raw tables by hard-coded name instead of source().
HARDCODED_TABLE = re.compile(r'"([A-Z_]+)"\."([A-Z_]+)"')
SOURCE_CTE = re.compile(r"with source as \(\s*select \* from \{\{ source\('([^']+)', '([^']+)'\) \}\}\s*\)", re.I)


def copy_tree(src: Path, dst: Path) -> None:
    for path in sorted(src.rglob("*")):
        if path.is_dir():
            continue
        out = dst / path.relative_to(src)
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, out)


class Retail:
    """Lookups against retail.duckdb."""

    def __init__(self, path: Path) -> None:
        self.con = duckdb.connect(str(path), read_only=True)
        self.tables = {(s.lower(), t.lower()): (s, t) for s, t in raw_tables(self.con)}
        # main.X alias views: `CREATE VIEW X AS SELECT * FROM <SCHEMA>.X;`
        self.aliases = {}
        for name, sql in self.con.execute(
            "select view_name, sql from duckdb_views() where schema_name = 'main' and not internal"
        ).fetchall():
            m = re.fullmatch(r"CREATE VIEW \S+ AS SELECT \* FROM (\w+)\.(\w+);", sql.strip())
            if m and (m[1].lower(), m[2].lower()) in self.tables:
                self.aliases[name.lower()] = self.tables[(m[1].lower(), m[2].lower())]

    def resolve(self, schema: str, identifier: str) -> tuple[str, str] | None:
        """Where retail.duckdb's dbt run finds `<schema>.<identifier>`. dbt ignores a
        table-level `schema:` in source YAML, so enterprise_db (`schema: main`)
        reads main's alias views."""
        key = (schema.lower(), identifier.lower())
        if key in self.tables:
            return self.tables[key]
        if schema.lower() == "main":
            return self.aliases.get(identifier.lower())
        return None

    def columns(self, schema: str, table: str) -> list[str]:
        return [c.lower() for (c,) in self.con.execute(
            "select column_name from duckdb_columns() where schema_name = ? and table_name = ? order by column_index",
            [schema, table],
        ).fetchall()]


def write_sources(retail: Retail) -> dict[tuple[str, str], tuple[str, str]]:
    """Move every `sources:` block into models/sources.yml (where load_postgres.py
    looks), each table pointing at its physical name in our warehouse's target
    schema. Returns (source, table) -> retail (schema, table)."""
    resolved: dict[tuple[str, str], tuple[str, str]] = {}
    unresolved: list[str] = []
    sources_out = []
    for yml in sorted((DST / "models").rglob("*.yml")):
        data = yaml.safe_load(yml.read_text()) or {}
        if "sources" not in data:
            continue
        for source in data.pop("sources"):
            tables = []
            for table in source.get("tables") or []:
                identifier = table.get("identifier", table["name"])
                hit = retail.resolve(source.get("schema", source["name"]), identifier)
                if hit is None:
                    unresolved.append(f"{source['name']}.{table['name']}")
                    continue
                resolved[(source["name"], table["name"])] = hit
                out = {"name": table["name"], "identifier": physical_name(*hit)}
                if table.get("description"):
                    out["description"] = table["description"]
                tables.append(out)
            sources_out.append({"name": source["name"], "schema": SOURCE_SCHEMA, "tables": tables})
        if any(v for k, v in data.items() if k != "version"):
            yml.write_text(yaml.safe_dump(data, sort_keys=False, width=200, allow_unicode=True))
        else:
            yml.unlink()
    models = {p.stem.lower() for p in (DST / "models").rglob("*.sql")}
    clash = sorted(models & {physical_name(*hit) for hit in resolved.values()})
    assert not clash, f"physical source names clash with models: {clash[:10]}"
    names = [s["name"] for s in sources_out]
    assert len(names) == len(set(names)), "duplicate source names"
    sources_out.append({"name": "seeds", "schema": SOURCE_SCHEMA,
                        "description": "Upstream seeds, as loaded into retail.duckdb.",
                        "tables": [{"name": s, "identifier": physical_name("main", s)} for s in SEEDS]})
    for s in SEEDS:
        resolved[("seeds", s)] = retail.resolve("main", s)
    text = yaml.safe_dump({"version": 2, "sources": sources_out}, sort_keys=False, width=200, allow_unicode=True)
    (DST / "models" / "sources.yml").write_text(
        "# Generated by utils/retail_vendor.py from upstream's _sources.yml files.\n"
        "# Raw tables written by generate_data.py (utils/retail_data.py).\n" + text
    )
    print(f"{len(resolved)} source tables; not in retail.duckdb (dropped): {len(unresolved)} {unresolved[:10]}")
    return resolved


def check_unresolved_sources(resolved: dict) -> None:
    missing = set()
    for path in (DST / "models").rglob("*.sql"):
        text = re.sub(r"\{#.*?#\}", "", path.read_text(), flags=re.S)
        if re.search(r"enabled\s*=\s*false", text):
            continue
        for src, tbl in re.findall(r"source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", text):
            if (src, tbl) not in resolved:
                missing.add((str(path.relative_to(DST)), src, tbl))
    assert not missing, f"enabled models read undeclared or missing sources: {sorted(missing)[:10]}"


# --- Postgres portability -------------------------------------------------
# DuckDB-only constructs become calls to macros/dagbench/portability.sql, which
# render upstream's SQL on DuckDB. Each rewrite parses the call's arguments
# (balanced parens, quotes) and passes them to the macro as Jinja strings.

def _scan_call(text: str, open_paren: int) -> tuple[int, list[str]]:
    """End (index after ')') and top-level comma-split args of the call whose
    '(' is at `open_paren`."""
    depth, quote, start, args = 0, None, open_paren + 1, []
    for i in range(open_paren, len(text)):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                args.append(text[start:i])
                return i + 1, [a.strip() for a in args]
        elif c == "," and depth == 1:
            args.append(text[start:i])
            start = i + 1
    raise ValueError(f"unbalanced call at {text[open_paren - 20:open_paren + 40]!r}")


def jinja_string(sql: str) -> str:
    """`sql` (which may contain `{{ expr }}`) as a Jinja string expression."""
    assert "{%" not in sql, sql
    parts = []
    for i, piece in enumerate(re.split(r"\{\{(.*?)\}\}", sql)):
        if i % 2:
            parts.append(f"({piece.strip()})")
        elif piece:
            q = '"' if '"' not in piece else "'"
            assert q not in piece, piece
            # Jinja string literals unescape backslashes (regexes like '^\d+$').
            parts.append(q + piece.replace("\\", "\\\\") + q)
    return " ~ ".join(parts) or '""'


def rewrite_calls(text: str, name_re: str, build) -> tuple[str, int]:
    """Replace every call `name(...)` (outside Jinja tags) with build(args)."""
    pattern = re.compile(r"(?<![\w.])(?:" + name_re + r")\s*\(", re.I)
    out, pos, n = [], 0, 0
    while m := pattern.search(text, pos):
        if text.rfind("{{", 0, m.start()) > text.rfind("}}", 0, m.start()):
            out.append(text[pos:m.end()])
            pos = m.end()
            continue
        end, args = _scan_call(text, m.end() - 1)
        out.append(text[pos:m.start()])
        out.append(build(args))
        pos, n = end, n + 1
    out.append(text[pos:])
    return "".join(out), n


def _datediff(args: list[str]) -> str:
    part, first, second = args
    assert re.fullmatch(r"'?\w+'?", part), args
    part = "'" + part.strip("'").lower() + "'"  # macros/ has Snowflake-style DATEDIFF(DAY, ...)
    return f"{{{{ dagbench_datediff({part}, {jinja_string(first)}, {jinja_string(second)}) }}}}"


def _try_cast(args: list[str]) -> str:
    (arg,) = args
    m = re.fullmatch(r"(.*)\s+as\s+(.+)", arg, re.I | re.S)  # the last top-level AS
    assert m, arg
    return f"{{{{ dagbench_try_cast({jinja_string(m[1])}, {jinja_string(m[2])}) }}}}"


def _strptime(try_: bool):
    def build(args: list[str]) -> str:
        expr, fmt = args
        return f"{{{{ dagbench_strptime({jinja_string(expr)}, {jinja_string(fmt)}, try={try_}) }}}}"
    return build


def _simple(macro: str, nargs: int):
    def build(args: list[str]) -> str:
        assert len(args) == nargs, (macro, args)
        return f"{{{{ {macro}({', '.join(jinja_string(a) for a in args)}) }}}}"
    return build


def _hash(args: list[str]) -> str:
    (arg,) = args
    return f"{{{{ dagbench_hash({jinja_string(arg)}) }}}}"


# `INTERVAL (<n>) DAY` (DuckDB) -> `(<n>) * INTERVAL '1 day'` (both).
INTERVAL_EXPR = re.compile(r"\bINTERVAL\s*\(", re.I)


def rewrite_interval_expr(text: str) -> tuple[str, int]:
    out, pos, n = [], 0, 0
    while m := INTERVAL_EXPR.search(text, pos):
        end, (arg,) = _scan_call(text, m.end() - 1)
        unit = re.match(r"\s*(DAY|MONTH|YEAR|HOUR)S?\b", text[end:], re.I)
        assert unit, text[m.start():end + 20]
        out += [text[pos:m.start()], f"({arg}) * INTERVAL '1 {unit[1].lower()}'"]
        pos, n = end + unit.end(), n + 1
    out.append(text[pos:])
    return "".join(out), n


WALL_CLOCK = [
    (re.compile(r"\bcurrent_localtimestamp\(\)", re.I), "dagbench_current_timestamp()"),
    (re.compile(r"\bcurrent_timestamp\b(\(\))?", re.I), "dagbench_current_timestamp()"),
    (re.compile(r"\bcurrent_date\b(\(\))?", re.I), "dagbench_current_date()"),
]


def pin_wall_clock(text: str, pattern: re.Pattern, macro: str) -> tuple[str, int]:
    """`{{ macro }}` in SQL; inside a Jinja tag (a double-quoted SQL string passed
    to a macro, e.g. date_spine's end_date) the string is split around the call."""
    def sub(m: re.Match) -> str:
        opened = text.rfind("{{", 0, m.start())
        if opened <= text.rfind("}}", 0, m.start()):
            return "{{ " + macro + " }}"
        assert text.count('"', opened, m.start()) % 2 == 1, text[opened:m.end()]
        return '" ~ ' + macro + ' ~ "'
    return pattern.subn(sub, text)


def _round_double(args: list[str]) -> str:
    return f"{{{{ dagbench_round_double({', '.join(jinja_string(a) for a in args)}) }}}}"


# round(<double>, n): Postgres only rounds numerics. Recognized by a cast to
# float/double in the first argument.
DOUBLE_CAST = re.compile(r"::\s*(float|double)|\bas\s+(float|double)\b", re.I)
# DuckDB accepts a trailing comma in the select list.
TRAILING_COMMA = re.compile(r",((?:\s*--[^\n]*)?\s*\n\s*)(from\b)", re.I)


# extract(dayofweek ...) is DuckDB's spelling of `dow` (0 = Sunday on both).
EXTRACT_DAYOFWEEK = re.compile(r"\b(extract\s*\(\s*)dayofweek(\s+from)\b", re.I)
DATEDIFF_DIVIDED = re.compile(r"(\{\{ dagbench_datediff\((?:[^{}]|\{\{[^}]*\}\})*?\) \}\})(\s*/(?!/))")
# DESC sorts NULLs last in DuckDB, first in Postgres (which changes LIMIT and
# ROW_NUMBER picks). Made explicit, models only (a macro parses ' DESC' strings).
DESC_NULLS = re.compile(r"\bdesc\b(?!\s+nulls)", re.I)
# dim_exchange_rates fakes rates with RANDOM(): a new value every run. Seeded
# on (rate_date, currency) instead, the same on both engines.
RANDOM_RATES = ("models/marts/core/dim_exchange_rates.sql", ["EUR", "GBP", "CAD", "AUD", "JPY"])
QUARTER_INTERVAL = re.compile(r"\binterval\s+'(\d+)\s+quarters?'", re.I)
# `DOUBLE` is DuckDB's name; both accept `DOUBLE PRECISION`.
DOUBLE_TYPE = re.compile(r"\b(as\s+|::\s*)double\b(?!\s+precision)", re.I)


# A bare DECIMAL/NUMERIC is DECIMAL(18,3) in DuckDB, unconstrained in Postgres.
UNSIZED_DECIMAL = re.compile(r"(::\s*|\bas\s+)(decimal|numeric)\b(?!\s*\()", re.I)


def portable(text: str, counts: dict) -> str:
    text, n = UNSIZED_DECIMAL.subn(r"\1decimal(18,3)", text)
    counts["decimal"] = counts.get("decimal", 0) + n
    text, n = TRAILING_COMMA.subn(r"\1\2", text)
    counts["trailing_comma"] += n
    text, n = DOUBLE_TYPE.subn(r"\1double precision", text)
    counts["double"] = counts.get("double", 0) + n
    text, n = EXTRACT_DAYOFWEEK.subn(r"\1dow\2", text)
    counts["extract_dow"] = counts.get("extract_dow", 0) + n
    # Postgres intervals have no quarter unit.
    text, n = QUARTER_INTERVAL.subn(lambda m: f"interval '{3 * int(m[1])} months'", text)
    counts["quarter"] = counts.get("quarter", 0) + n
    for pattern, macro in WALL_CLOCK:
        text, n = pin_wall_clock(text, pattern, macro)
        counts["wall_clock"] += n
    text, n = rewrite_calls(text, r"hash", _hash)
    counts["hash"] += n
    text, n = rewrite_interval_expr(text)
    counts["interval"] += n
    text, n = rewrite_calls(text, r"date_diff|datediff", _datediff)
    counts["datediff"] += n
    # DuckDB's `/` on integers is float division; Postgres truncates. Upstream
    # divides date_diff results (avg days between orders); same value on DuckDB.
    text, n = DATEDIFF_DIVIDED.subn(r"cast(\1 as double precision)\2", text)
    counts["datediff_division"] = counts.get("datediff_division", 0) + n
    text, n = rewrite_calls(text, r"try_cast", _try_cast)
    counts["try_cast"] += n
    for name, build in [("try_strptime", _strptime(True)), ("strptime", _strptime(False)),
                        ("regexp_matches", _simple("dagbench_regexp_matches", 2)),
                        ("epoch_ms", _simple("dagbench_epoch_ms", 1)),
                        *[(f, _simple(f"dagbench_{f}", 1)) for f in ("dayofweek", "dayname", "monthname", "last_day")]]:
        text, n = rewrite_calls(text, name, build)
        counts[name] = counts.get(name, 0) + n

    def round_call(args: list[str]) -> str:
        if len(args) == 2 and DOUBLE_CAST.search(args[0]):
            counts["round_double"] += 1
            return _round_double(args)
        return "round(" + ", ".join(args) + ")"
    text, _ = rewrite_calls(text, r"round", round_call)
    return text


def rewrite_models(retail: Retail, resolved: dict) -> None:
    counts = {"row_num": 0, "_rn": 0, "hardcoded": 0, "wall_clock": 0, "hash": 0, "interval": 0,
              "datediff": 0, "try_cast": 0, "trailing_comma": 0, "round_double": 0}
    for path in sorted((DST / "models").rglob("*.sql")):
        text = path.read_text()
        new = text
        for seed in SEEDS:
            new = new.replace(f"ref('{seed}')", f"source('seeds', '{seed}')")
        for schema, table in set(HARDCODED_TABLE.findall(new)):
            assert (schema.lower(), table) in resolved, (path, schema, table)
            new = new.replace(f'"{schema}"."{table}"', f"{{{{ source('{schema.lower()}', '{table}') }}}}")
            counts["hardcoded"] += 1
        if EXCLUDE_ROW_NUM in new:
            assert new.count(EXCLUDE_ROW_NUM) == 1, path
            assert not re.search(r"select\s+\*\s+from\s+cleaned|cleaned\.\*", new, re.I), path
            new = new.replace(EXCLUDE_ROW_NUM, "SELECT * FROM deduplicated WHERE row_num = 1")
            counts["row_num"] += 1
        if EXCLUDE_RN in new:
            assert new.count(EXCLUDE_RN) == 1, path
            m = SOURCE_CTE.search(new)
            assert m, f"{path}: unexpected source CTE"
            assert "_rn" not in retail.columns(*resolved[(m[1], m[2])]), path
            cols = ",\n        ".join(retail.columns(*resolved[(m[1], m[2])]))
            new = new.replace(EXCLUDE_RN, "select\n        " + cols)
            counts["_rn"] += 1
        new = portable(new, counts)
        new, n = DESC_NULLS.subn("DESC NULLS LAST", new)
        counts["desc_nulls_last"] = counts.get("desc_nulls_last", 0) + n
        if new != text:
            path.write_text(new)
    # Tenure years are doubles (median, stddev, and date_diff / 365.0 once
    # DATEDIFF_DIVIDED casts it), so every round() over one needs the macro.
    path = DST / "models/marts/hr/rpt_average_tenure.sql"
    text, n = rewrite_calls(path.read_text(), r"round",
                            lambda a: _round_double(a) if "tenure" in a[0] else "round(" + ", ".join(a) + ")")
    path.write_text(text)
    assert "round(dm.min_tenure_years" not in text and "round(dm.median_tenure_years" not in text
    rel, currencies = RANDOM_RATES
    path = DST / rel
    text = path.read_text()
    assert text.count("RANDOM()") == len(currencies), rel
    for cur in currencies:
        assert f"'{cur}' AS from_currency" in text, (rel, cur)
        text = text.replace("RANDOM()", f"{{{{ dagbench_random(\"cast(rate_date as varchar) || '{cur}'\") }}}}", 1)
    path.write_text(text)
    leftovers = [str(p.relative_to(DST)) for p in (DST / "models").rglob("*.sql") if re.search(r"\brandom\(", p.read_text(), re.I)]
    assert not leftovers, f"RANDOM() left in {leftovers}"
    for path in sorted((DST / "macros").rglob("*.sql")):
        if "dagbench" in path.parts:
            continue
        text = path.read_text()
        new = portable(text, counts)
        if new != text:
            path.write_text(new)
    for rel, old, new, *count in TEXT_REPLACEMENTS:
        path = DST / rel
        text = path.read_text()
        assert text.count(old) == (count[0] if count else 1), (rel, old)
        path.write_text(text.replace(old, new))
    leftovers = [str(p.relative_to(DST)) for p in (DST / "models").rglob("*.sql")
                 if re.search(r"\bexclude\s*\(", p.read_text(), re.I)]
    assert not leftovers, f"EXCLUDE left in {leftovers}"
    print(f"rewrites: {counts}")


def main() -> None:
    src = Path(sys.argv[1]).resolve()
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=src, check=True,
                         capture_output=True, text=True).stdout.strip()
    assert sha == UPSTREAM_COMMIT, f"checkout is at {sha}; update UPSTREAM_COMMIT and RETAIL_SHA256 in utils/retail_data.py"
    project = src / UPSTREAM_PROJECT

    for sub in ("models", "macros"):
        for child in (DST / sub).glob("*") if (DST / sub).exists() else []:
            if child.name == "dagbench":
                continue
            shutil.rmtree(child) if child.is_dir() else child.unlink()

    copy_tree(project / "models", DST / "models")
    copy_tree(project / "macros", DST / "macros")

    retail = Retail(download_retail())
    resolved = write_sources(retail)
    rewrite_models(retail, resolved)
    check_unresolved_sources(resolved)
    (DST / "UPSTREAM").write_text(f"https://github.com/Snowflake-Labs/data-eng-bench {sha} {UPSTREAM_PROJECT}\n")
    n = len(list((DST / "models").rglob("*.sql")))
    print(f"Vendored data-eng-bench {UPSTREAM_PROJECT} ({sha[:7]}, {n} models) into {DST.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
