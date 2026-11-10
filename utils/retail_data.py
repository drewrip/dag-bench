"""Source data for projects/p13_snowflake.

data-eng-bench ships no generator: its dbt project reads one fixed DuckDB file,
`base-image/database/retail.duckdb` (Git LFS, ~489 MB on disk, but the largest
table has ~77k rows). This module uses it as-is; it does not scale with sf.

retail.duckdb keeps raw tables in 19 upper-case schemas (RAW_SAP, ORDERS, ...)
plus, in `main`, 7 `_lookup_*` tables and upstream's 9 loaded seeds. `main`
also holds a prebuilt copy of every model and 571 alias views (`main.X` ->
`<schema>.X`); both are dropped. The warehouse we build puts every raw table
in `main` as `src_<schema>__<table>` (lower case), with lower-case column
names so unquoted references resolve the same way on Postgres.
utils/retail_vendor.py points every dbt source at those names via
`physical_name()`.

The download is cached in dbgen/.cache/retail_<sha8>.duckdb, the assembled
warehouse in dbgen/.cache/p13_snowflake_v<ASSEMBLY_VERSION>.duckdb.
"""
from __future__ import annotations

import hashlib
import shutil
import sys
import urllib.request
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = REPO_ROOT / "dbgen" / ".cache"
OUTPUT_PATH = Path("data/warehouse.duckdb")

UPSTREAM_COMMIT = "a3278ad102829a6084dde086244a0ef665a8011c"
RETAIL_SHA256 = "bd2bb1b3a7cf62aa94c191527aa30db5bf415aea97db718a1e0c11c59fc2ec2d"
RETAIL_URL = (
    "https://media.githubusercontent.com/media/Snowflake-Labs/data-eng-bench/"
    f"{UPSTREAM_COMMIT}/base-image/database/retail.duckdb"
)
# Upstream's seeds, already loaded into retail.duckdb's `main` (its
# product_categories seed never was). Models read them as sources.
SEED_TABLES = (
    "country_codes", "currency_codes", "customer_tiers", "order_statuses", "payment_methods",
    "regions", "seed_manual_currency_corrections", "shipment_methods", "store_types",
)
# Bump when the assembled warehouse changes, so stale caches are rebuilt.
ASSEMBLY_VERSION = 2


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def physical_name(schema: str, table: str) -> str:
    """Name in our warehouse's `main` of retail.duckdb's `<schema>.<table>`.
    Prefixed so it can't collide with a model (upstream has models named
    e.g. `raw_legacy__glacct`); retail_vendor.py asserts that."""
    return f"src_{schema.lower()}__{table.lower()}"


def raw_tables(con: duckdb.DuckDBPyConnection, db: str | None = None) -> list[tuple[str, str]]:
    """(schema, table) of every raw table in retail.duckdb, attached as `db`
    (default: the connection's own database)."""
    return con.execute(
        "select schema_name, table_name from duckdb_tables() where database_name = coalesce(?, current_database()) "
        "and (schema_name <> 'main' or starts_with(table_name, '_lookup_') or table_name in ?) order by 1, 2",
        [db, list(SEED_TABLES)],
    ).fetchall()


def download_retail() -> Path:
    path = CACHE_DIR / f"retail_{RETAIL_SHA256[:8]}.duckdb"
    if path.exists():
        return path
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".duckdb.partial")
    print(f"Downloading {RETAIL_URL}")
    digest = hashlib.sha256()
    with urllib.request.urlopen(RETAIL_URL) as response, tmp_path.open("wb") as out:
        while chunk := response.read(1 << 20):
            digest.update(chunk)
            out.write(chunk)
    if digest.hexdigest() != RETAIL_SHA256:
        tmp_path.unlink()
        raise SystemExit(f"retail.duckdb checksum mismatch: got {digest.hexdigest()}, want {RETAIL_SHA256}")
    tmp_path.rename(path)
    return path


def assemble(retail: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    con = duckdb.connect(str(output_path))
    try:
        con.execute(f"attach {sql_literal(str(retail))} as retail (read_only)")
        seen: dict[str, tuple[str, str]] = {}
        for schema, table in raw_tables(con, "retail"):
            name = physical_name(schema, table)
            assert name not in seen, (name, seen[name], (schema, table))
            seen[name] = (schema, table)
            columns = con.execute(
                "select column_name, data_type from duckdb_columns() "
                "where database_name = 'retail' and schema_name = ? and table_name = ? order by column_index",
                [schema, table],
            ).fetchall()
            # All-NULL columns are typed "NULL", which has no Postgres equivalent.
            select = ", ".join(
                (f"cast({quote_ident(c)} as varchar)" if t == '"NULL"' else quote_ident(c))
                + f" as {quote_ident(c.lower())}"
                for c, t in columns
            )
            con.execute(
                f"create table {quote_ident(name)} as select {select} "
                f"from retail.{quote_ident(schema)}.{quote_ident(table)}"
            )
        assert all(("main", s) in seen.values() for s in SEED_TABLES)
        print(f"Assembled {len(seen)} raw tables")
    finally:
        con.close()


def run_retail(project_dir: Path) -> None:
    """generate_data.py entry point, run from the project directory (the
    dbgen/project_runner.py contract). An sf argument and --no-constraints are
    accepted and ignored: the data is fixed and has no keys."""
    positional = [a for a in sys.argv[1:] if not a.startswith("--")]
    if positional:
        print(
            f"WARNING: p13_snowflake does not scale with sf; it uses data-eng-bench's fixed "
            f"retail.duckdb as-is. Ignoring sf={positional[0]}.",
            file=sys.stderr,
        )
    cache_path = CACHE_DIR / f"p13_snowflake_v{ASSEMBLY_VERSION}.duckdb"
    output_path = project_dir / OUTPUT_PATH

    if output_path.exists():
        print(f"Deleting existing duckdb file: {output_path}")
        output_path.unlink()

    if cache_path.exists():
        print(f"Using cached duckdb file: {cache_path}")
    else:
        retail = download_retail()
        tmp_path = cache_path.with_suffix(".duckdb.partial")
        assemble(retail, tmp_path)
        tmp_path.rename(cache_path)
        print(f"Cached generated duckdb file to: {cache_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cache_path, output_path)
