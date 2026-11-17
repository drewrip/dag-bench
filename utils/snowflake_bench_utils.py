"""Snowflake side of dag-bench: connections and the tiny-data loader.

Every entry point must run under the budget wrapper:

  python -m utils.snowflake_budget run --estimate-min 2 -- python projects/p01_iot/load_snowflake.py

Logging is kept at WARNING and the connection parameters are never printed.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path

import duckdb
import yaml

from utils.postgres_bench_utils import find_repo_root, load_yaml, read_source_tables

for _name in ("snowflake", "snowflake.connector", "botocore", "boto3", "urllib3"):
    logging.getLogger(_name).setLevel(logging.WARNING)

DATABASE = "CMU"
# DREWRIPBERGER can only read CMU; CMUADMIN (via database role CMU.ADMIN) can
# create schemas. Everything we create lives in DAGBENCH_* schemas.
ROLE = os.environ.get("DAGBENCH_SF_ROLE", "CMUADMIN")
FINGERPRINT_VERSION = "dagbench-loader-v1"
SESSION_PARAMETERS = {
    "STATEMENT_TIMEOUT_IN_SECONDS": 120,
    "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS": 300,
    "QUERY_TAG": "dagbench",
    "TIMEZONE": "UTC",  # match the DuckDB reference build
    "WEEK_START": 0,
    "WEEK_OF_YEAR_POLICY": 0,
}
# The reference DuckDB build for the comparison uses this file (profile output
# `duckdb_tiny`), so the benchmark's own data/warehouse.duckdb is left alone.
TINY_DUCKDB = "data/tiny/warehouse.duckdb"


def connect(schema: str | None = None, database: str = DATABASE):
    """A connector session with our timeouts and query tag. Never log the params."""
    import snowflake.connector

    from utils.snowflake_secret import connection_params

    params = connection_params()
    params["database"] = database
    params["role"] = ROLE
    if schema:
        params["schema"] = schema
    return snowflake.connector.connect(
        **params,
        session_parameters=dict(SESSION_PARAMETERS),
        client_session_keep_alive=False,
        login_timeout=60,
        application="dagbench",
    )


def snowflake_schema(project_dir: Path) -> str:
    """The `schema` of the project's `snowflake` profile output, upper-cased."""
    profile = load_yaml(project_dir / "profiles.yml")
    out = next(iter(profile.values()))["outputs"]["snowflake"]
    return str(out["schema"]).upper()


# --------------------------------------------------------------------------- types

_INT_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER"}
_WIDE_INT_TYPES = {"HUGEINT", "UBIGINT", "UHUGEINT"}


def snowflake_type(duck_type: str) -> tuple[str, str | None]:
    """(Snowflake column type, DuckDB cast for the parquet export or None)."""
    t = duck_type.upper().strip()
    if t in _INT_TYPES:
        return "NUMBER(38,0)", "BIGINT" if t.startswith("U") else None
    if t in _WIDE_INT_TYPES:
        # DuckDB writes HUGEINT to parquet as DOUBLE; go through DECIMAL(38,0) instead.
        return "NUMBER(38,0)", "DECIMAL(38,0)"
    m = re.fullmatch(r"(?:DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", t)
    if m:
        return f"NUMBER({m.group(1)},{m.group(2)})", None
    if t in {"DOUBLE", "FLOAT", "REAL", "FLOAT8", "FLOAT4"}:
        return "FLOAT", "DOUBLE" if t in {"FLOAT", "REAL", "FLOAT4"} else None
    if t in {"VARCHAR", "TEXT", "STRING"} or t.startswith("VARCHAR(") or t.startswith("ENUM"):
        return "VARCHAR", None if t == "VARCHAR" else "VARCHAR"
    if t in ("UUID", "JSON"):  # JSON: its text, as DuckDB's models read it
        return "VARCHAR", "VARCHAR"
    if t in {"BOOLEAN", "BOOL"}:
        return "BOOLEAN", None
    if t == "DATE":
        return "DATE", None
    if t in {"TIMESTAMP", "TIMESTAMP_MS", "TIMESTAMP_S", "DATETIME"}:
        return "TIMESTAMP_NTZ(9)", None if t == "TIMESTAMP" else "TIMESTAMP"
    if t == "TIMESTAMP_NS":
        return "TIMESTAMP_NTZ(9)", None
    if t in {"TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"}:
        return "TIMESTAMP_TZ", None
    if t == "TIME":
        return "TIME", None
    if t in {"BLOB", "BYTEA"}:
        return "BINARY", None
    raise ValueError(f"no Snowflake mapping for DuckDB type {duck_type!r}")


def quote_upper(name: str) -> str:
    """The identifier Snowflake resolves an unquoted `name` to, quoted (safe for reserved words)."""
    return '"' + name.upper().replace('"', '""') + '"'


def duck_quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------------- inventory

def table_inventory(con: duckdb.DuckDBPyConnection, tables: list[str]) -> list[dict]:
    inv = []
    for table in tables:
        cols = con.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = 'main' and table_name = ? order by ordinal_position",
            [table],
        ).fetchall()
        if not cols:
            raise RuntimeError(f"source table main.{table} not found in DuckDB")
        rows = con.execute(f"select count(*) from main.{duck_quote(table)}").fetchone()[0]
        inv.append({"table": table, "rows": rows, "columns": [[c, t] for c, t in cols]})
    return inv


def fingerprint(inventory: list[dict]) -> str:
    payload = json.dumps([FINGERPRINT_VERSION, sorted(inventory, key=lambda r: r["table"])], sort_keys=True)
    return "dagbench:" + hashlib.sha256(payload.encode()).hexdigest()


def current_schema_comment(cur, schema: str) -> str | None:
    cur.execute(f"show schemas like '{schema}' in database {DATABASE}")
    names = [d[0] for d in cur.description]
    for row in cur.fetchall():
        rec = dict(zip(names, row))
        if rec["name"].upper() == schema.upper():
            return rec.get("comment") or ""
    return None


# ------------------------------------------------------------------------ loader

LOAD_WORKERS = 8  # parallel sessions for per-table DDL and COPY (p13 has ~580 tables)


def run_parallel(schema: str, statements: list[str], workers: int = LOAD_WORKERS) -> None:
    """Execute independent statements over `workers` sessions in CMU.<schema>."""
    if len(statements) <= 4:
        workers = 1
    import threading
    from concurrent.futures import ThreadPoolExecutor

    local = threading.local()
    conns = []
    lock = threading.Lock()

    def one(sql: str) -> None:
        if not hasattr(local, "cur"):
            con = connect(schema=schema)
            with lock:
                conns.append(con)
            local.cur = con.cursor()
        local.cur.execute(sql)

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one, statements))
    finally:
        for con in conns:
            con.close()

def load_project_sources_to_snowflake(
    project_dir: Path,
    duckdb_path: str = TINY_DUCKDB,
    schema: str | None = None,
    sources_project_dir: Path | None = None,
    force: bool = False,
    schema_only: bool = False,
) -> None:
    """Load the project's source tables from `duckdb_path` into CMU.<schema>.

    Skips (no compute) when the schema COMMENT already holds the fingerprint of
    exactly these tables, row counts and column types.

    schema_only: only CREATE the (empty) tables when they don't exist yet; DDL
    runs on cloud services, so this is free. It lets `dbt run --empty` (rung 4)
    resolve the sources before any data is loaded."""
    project_dir = Path(project_dir).resolve()
    sources_dir = Path(sources_project_dir or project_dir).resolve()
    schema = (schema or snowflake_schema(project_dir)).upper()
    if not schema.startswith("DAGBENCH_"):
        raise SystemExit(f"refusing to load into {schema}: only DAGBENCH_* schemas are ours")
    db_file = project_dir / duckdb_path
    if not db_file.exists():
        raise SystemExit(f"{db_file} not found; run `python -m utils.snowflake_tiny prepare {project_dir.name}` first")
    tables = read_source_tables(sources_dir / "models" / "sources.yml")

    duck = duckdb.connect(str(db_file), read_only=True)
    inventory = table_inventory(duck, tables)
    fp = fingerprint(inventory)
    total_rows = sum(r["rows"] for r in inventory)
    print(f"[{schema}] {len(inventory)} source tables, {total_rows:,} rows from {db_file.relative_to(find_repo_root(project_dir))}")

    sf = connect()
    cur = sf.cursor()
    try:
        comment = current_schema_comment(cur, schema)
        if schema_only:
            cur.execute(f"create schema if not exists {DATABASE}.{schema}")
            cur.execute(f"use schema {DATABASE}.{schema}")
            run_parallel(schema, [
                f"create table if not exists {quote_upper(rec['table'])} "
                f"({', '.join(f'{quote_upper(n)} {snowflake_type(t)[0]}' for n, t in rec['columns'])})"
                for rec in inventory])
            print(f"[{schema}] schema-only: {len(inventory)} source tables exist (no data loaded)")
            return
        if comment == fp and not force:
            print(f"[{schema}] fingerprint matches; load skipped (no compute)")
            return
        cur.execute(f"create schema if not exists {DATABASE}.{schema}")
        cur.execute(f"use schema {DATABASE}.{schema}")
        stage = f"@~/dagbench/{schema.lower()}/"
        tmp = Path(tempfile.mkdtemp(prefix=f"dagbench_sf_{schema.lower()}_"))
        try:
            ddl = []
            for rec in inventory:
                table, cols = rec["table"], rec["columns"]
                select = []
                col_ddl = []
                for name, dtype in cols:
                    sf_type, cast = snowflake_type(dtype)
                    col_ddl.append(f"{quote_upper(name)} {sf_type}")
                    select.append(f"cast({duck_quote(name)} as {cast}) as {duck_quote(name)}" if cast else duck_quote(name))
                out = tmp / f"{table}.parquet"
                duck.execute(
                    f"copy (select {', '.join(select)} from main.{duck_quote(table)}) "
                    f"to '{out}' (format parquet, compression zstd)"
                )
                ddl.append((table, col_ddl))
            # Upload everything in one PUT (cloud services; no warehouse).
            cur.execute(f"remove {stage}")
            cur.execute(f"put 'file://{tmp}/*.parquet' {stage} auto_compress=false overwrite=true parallel=16")
            run_parallel(schema, [f"create or replace table {quote_upper(table)} ({', '.join(col_ddl)})"
                                  for table, col_ddl in ddl])
            # The only statements that need the warehouse: back to back, over a few sessions.
            run_parallel(schema, [
                f"copy into {quote_upper(table)} from {stage}{table}.parquet "
                "file_format = (type = parquet use_logical_type = true binary_as_text = false) "
                "match_by_column_name = case_insensitive on_error = abort_statement"
                for table, _ in ddl])
            # count(*) on a table is answered from metadata (no warehouse).
            # Row counts from metadata in one statement (no warehouse).
            cur.execute(f"select table_name, row_count from {DATABASE}.information_schema.tables "
                        f"where table_schema = '{schema}' and table_type = 'BASE TABLE'")
            counts = {name.upper(): n for name, n in cur.fetchall()}
            bad = [f"{rec['table']}: snowflake {counts.get(rec['table'].upper())} != duckdb {rec['rows']}"
                   for rec in inventory if counts.get(rec["table"].upper()) != rec["rows"]]
            if bad:
                raise RuntimeError(f"[{schema}] row count mismatch after load: " + "; ".join(bad))
            cur.execute(f"comment on schema {DATABASE}.{schema} is '{fp}'")
            print(f"[{schema}] loaded {len(ddl)} tables, {total_rows:,} rows; fingerprint stored")
        finally:
            try:
                cur.execute(f"remove {stage}")
            except Exception as exc:  # noqa: BLE001 - cleanup is best effort
                print(f"[{schema}] warning: could not clean {stage}: {type(exc).__name__}")
            shutil.rmtree(tmp, ignore_errors=True)
    finally:
        cur.close()
        sf.close()
        duck.close()


def main_for(project_file: str, **kwargs) -> None:
    """Entry point used by each project's load_snowflake.py."""
    import argparse

    ap = argparse.ArgumentParser(description="Load tiny source data into Snowflake")
    ap.add_argument("--force", action="store_true", help="reload even when the fingerprint matches")
    ap.add_argument("--schema-only", action="store_true", help="only create empty source tables (free DDL)")
    ap.add_argument("--duckdb", default=TINY_DUCKDB, help=f"source DuckDB file (default {TINY_DUCKDB})")
    args = ap.parse_args()
    if os.environ.get("DAGBENCH_SF_WRAPPED") != "1":
        raise SystemExit("run this under `python -m utils.snowflake_budget run -- ...`")
    load_project_sources_to_snowflake(Path(project_file).resolve().parent, duckdb_path=args.duckdb,
                                      force=args.force, schema_only=args.schema_only, **kwargs)
