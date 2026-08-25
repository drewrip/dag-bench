from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import yaml


TYPE_MAPPING = {
    "BIGINT": "BIGINT",
    "BLOB": "BYTEA",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "DATE": "DATE",
    "DOUBLE": "DOUBLE PRECISION",
    "FLOAT": "DOUBLE PRECISION",
    "HUGEINT": "NUMERIC(38,0)",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "REAL": "REAL",
    "SMALLINT": "SMALLINT",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "TIMESTAMP_NS": "TIMESTAMP",
    "TIMESTAMP_MS": "TIMESTAMP",
    "TIMESTAMP_S": "TIMESTAMP",
    "TINYINT": "SMALLINT",
    "UBIGINT": "NUMERIC(20,0)",
    "UHUGEINT": "NUMERIC(38,0)",
    "UINTEGER": "BIGINT",
    "USMALLINT": "INTEGER",
    "UTINYINT": "SMALLINT",
    "UUID": "UUID",
    "VARCHAR": "TEXT",
}


# Rows are streamed straight from DuckDB into Postgres over the binary COPY
# protocol via DuckDB's `postgres` extension, so the CSV round-trip is only a
# fallback for environments where the extension cannot be loaded.
DEFAULT_WORKERS = min(8, (os.cpu_count() or 4))
# Splitting a table below this many rows costs more in connection setup than it
# saves in parallelism.
MIN_CHUNK_ROWS = 500_000
# Each worker only feeds a single COPY stream; more scan threads than this just
# oversubscribe the CPU once several workers are running.
THREADS_PER_WORKER = 2


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def find_repo_root(start: Path) -> Path:
    current = start.resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError(f"Could not locate repository root from {start}")


def load_yaml(path: Path) -> dict:
    with path.open() as handle:
        return yaml.safe_load(handle)


def read_postgres_output(profile_path: Path) -> dict:
    profile = load_yaml(profile_path)
    profile_name = next(iter(profile))
    return profile[profile_name]["outputs"]["postgres"]


def read_source_tables(sources_path: Path) -> list[str]:
    data = load_yaml(sources_path)
    tables: list[str] = []
    for source in data.get("sources", []):
        for table in source.get("tables", []):
            tables.append(table["name"])
    return tables


def psql_env(pg_config: dict) -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = str(pg_config["password"])
    return env


def run_psql(pg_config: dict, sql: str) -> None:
    cmd = [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(pg_config["host"]),
        "-p",
        str(pg_config["port"]),
        "-U",
        str(pg_config["user"]),
        "-d",
        str(pg_config["dbname"]),
        "-c",
        sql,
    ]
    subprocess.run(cmd, check=True, env=psql_env(pg_config))


def run_psql_file(pg_config: dict, sql_path: Path) -> None:
    cmd = [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(pg_config["host"]),
        "-p",
        str(pg_config["port"]),
        "-U",
        str(pg_config["user"]),
        "-d",
        str(pg_config["dbname"]),
        "-f",
        str(sql_path),
    ]
    subprocess.run(cmd, check=True, env=psql_env(pg_config))


def ensure_database(pg_config: dict) -> None:
    admin_cmd = [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(pg_config["host"]),
        "-p",
        str(pg_config["port"]),
        "-U",
        str(pg_config["user"]),
        "-d",
        "postgres",
        "-tAc",
        f"select 1 from pg_database where datname = '{pg_config['dbname']}';",
    ]
    existing = subprocess.run(
        admin_cmd,
        check=True,
        capture_output=True,
        text=True,
        env=psql_env(pg_config),
    )
    if existing.stdout.strip() != "1":
        create_cmd = [
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            str(pg_config["host"]),
            "-p",
            str(pg_config["port"]),
            "-U",
            str(pg_config["user"]),
            "-d",
            "postgres",
            "-c",
            f"create database {quote_ident(pg_config['dbname'])};",
        ]
        subprocess.run(create_cmd, check=True, env=psql_env(pg_config))


def map_type(duckdb_type: str) -> str:
    normalized = duckdb_type.strip().upper()
    if normalized.startswith("DECIMAL") or normalized.startswith("NUMERIC"):
        return normalized
    if normalized.startswith("VARCHAR"):
        return "TEXT"
    return TYPE_MAPPING.get(normalized, "TEXT")


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    count = con.execute(
        """
        select count(*)
        from information_schema.tables
        where table_name = ?
          and table_schema in ('main', 'public')
        """,
        [table_name],
    ).fetchone()[0]
    return count > 0


def duckdb_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, str]]:
    rows = con.execute(f"pragma table_info({quote_ident(table_name)})").fetchall()
    return [(row[1], row[2]) for row in rows]


def export_csv(con: duckdb.DuckDBPyConnection, table_name: str, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy {quote_ident(table_name)} to '{csv_path.as_posix()}' (format csv, header true)"
    )


def create_table_sql(
    schema: str,
    table_name: str,
    columns: list[tuple[str, str]],
    unlogged: bool = False,
) -> str:
    column_sql = ", ".join(
        f"{quote_ident(column_name)} {map_type(column_type)}"
        for column_name, column_type in columns
    )
    qualified_table = f"{quote_ident(schema)}.{quote_ident(table_name)}"
    kind = "unlogged table" if unlogged else "table"
    return (
        f"drop table if exists {qualified_table} cascade; "
        f"create {kind} {qualified_table} ({column_sql});"
    )


def create_table(
    pg_config: dict,
    schema: str,
    table_name: str,
    columns: list[tuple[str, str]],
    unlogged: bool = False,
) -> None:
    run_psql(pg_config, create_table_sql(schema, table_name, columns, unlogged))


def load_csv_into_postgres(pg_config: dict, schema: str, table_name: str, csv_path: Path) -> None:
    qualified_table = f"{quote_ident(schema)}.{quote_ident(table_name)}"
    sql = (
        f"\\copy {qualified_table} "
        f"from '{csv_path.resolve().as_posix()}' with (format csv, header true)"
    )
    run_psql(pg_config, sql)


def libpq_dsn(pg_config: dict) -> str:
    """Build a libpq connection string for DuckDB's postgres extension."""
    fields = [
        ("host", pg_config.get("host")),
        ("port", pg_config.get("port")),
        ("user", pg_config.get("user")),
        ("password", pg_config.get("password")),
        ("dbname", pg_config.get("dbname")),
    ]
    parts = []
    for key, value in fields:
        if value is None or str(value) == "":
            continue
        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"{key}='{escaped}'")
    return " ".join(parts)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_bridge(duckdb_file: Path, dsn: str, threads: int) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with the warehouse and Postgres both attached.

    The root database is in-memory rather than the warehouse file itself: a
    connection opened read-only refuses writes to *every* attached database,
    including Postgres.
    """
    con = duckdb.connect(config={"threads": threads})
    # Several of these run at once; their progress bars would interleave.
    con.execute("set enable_progress_bar = false")
    con.execute("install postgres; load postgres;")
    con.execute(f"attach {sql_literal(duckdb_file.as_posix())} as wh (read_only)")
    con.execute(f"attach {sql_literal(dsn)} as pg (type postgres)")
    return con


def postgres_extension_available() -> bool:
    con = duckdb.connect()
    try:
        con.execute("install postgres; load postgres;")
        return True
    except duckdb.Error:
        return False
    finally:
        con.close()


def is_base_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Only base tables expose `rowid`, which is what chunked loads range over."""
    row = con.execute(
        """
        select table_type
        from information_schema.tables
        where table_name = ?
          and table_schema in ('main', 'public')
        """,
        [table_name],
    ).fetchone()
    return bool(row) and row[0] == "BASE TABLE"


def plan_chunks(
    table_name: str,
    row_count: int,
    splittable: bool,
    workers: int,
    min_chunk_rows: int,
) -> list[tuple[str, int | None, int | None]]:
    """Split one table into disjoint `rowid` ranges that can load concurrently."""
    if not splittable or row_count <= min_chunk_rows:
        return [(table_name, None, None)]
    chunks = max(1, min(workers, row_count // min_chunk_rows))
    step = -(-row_count // chunks)
    plan: list[tuple[str, int | None, int | None]] = []
    for index in range(chunks):
        low = index * step
        high = min((index + 1) * step, row_count)
        if low >= high:
            break
        plan.append((table_name, low, high))
    return plan


def load_chunk(
    duckdb_file: Path,
    dsn: str,
    schema: str,
    job: tuple[str, int | None, int | None],
    threads: int,
) -> None:
    table_name, low, high = job
    con = open_bridge(duckdb_file, dsn, threads)
    try:
        source = f"select * from wh.main.{quote_ident(table_name)}"
        if low is not None:
            source += f" where rowid >= {low} and rowid < {high}"
        con.execute(
            f"insert into pg.{quote_ident(schema)}.{quote_ident(table_name)} {source}"
        )
    finally:
        con.close()


def load_tables_via_extension(
    pg_config: dict,
    duckdb_file: Path,
    con: duckdb.DuckDBPyConnection,
    schema: str,
    tables: list[str],
    unlogged: bool,
    workers: int,
    min_chunk_rows: int,
) -> None:
    """Stream every table into Postgres over binary COPY, in parallel."""
    row_counts = {
        table_name: con.execute(
            f"select count(*) from {quote_ident(table_name)}"
        ).fetchone()[0]
        for table_name in tables
    }

    ddl = " ".join(
        create_table_sql(schema, table_name, duckdb_columns(con, table_name), unlogged)
        for table_name in tables
    )
    run_psql(pg_config, ddl)

    # Largest tables first so their chunks are queued before the small ones and
    # no worker is left holding a long tail.
    jobs: list[tuple[str, int | None, int | None]] = []
    for table_name in sorted(tables, key=lambda name: row_counts[name], reverse=True):
        if row_counts[table_name] == 0:
            continue
        jobs.extend(
            plan_chunks(
                table_name,
                row_counts[table_name],
                is_base_table(con, table_name),
                workers,
                min_chunk_rows,
            )
        )

    if not jobs:
        return

    dsn = libpq_dsn(pg_config)
    threads = max(1, (os.cpu_count() or 4) // min(workers, len(jobs)))
    threads = min(threads, THREADS_PER_WORKER)
    with ThreadPoolExecutor(max_workers=min(workers, len(jobs))) as pool:
        futures = [
            pool.submit(load_chunk, duckdb_file, dsn, schema, job, threads)
            for job in jobs
        ]
        for future in futures:
            future.result()


def load_tables_via_csv(
    pg_config: dict,
    con: duckdb.DuckDBPyConnection,
    schema: str,
    tables: list[str],
    csv_dir: Path,
    unlogged: bool,
) -> None:
    """Fallback for hosts where the DuckDB postgres extension cannot load."""
    for table_name in tables:
        create_table(pg_config, schema, table_name, duckdb_columns(con, table_name), unlogged)
        csv_path = csv_dir / f"{table_name}.csv"
        export_csv(con, table_name, csv_path)
        load_csv_into_postgres(pg_config, schema, table_name, csv_path)


def load_project_sources_to_postgres(
    project_dir: Path,
    duckdb_path: str,
    workers: int | None = None,
    unlogged: bool | None = None,
    use_extension: bool | None = None,
) -> None:
    project_dir = project_dir.resolve()
    repo_root = find_repo_root(project_dir)
    profile_path = project_dir / "profiles.yml"
    sources_path = project_dir / "models" / "sources.yml"
    pg_config = read_postgres_output(profile_path)
    schema = str(pg_config["schema"])
    duckdb_file = (project_dir / duckdb_path).resolve()
    csv_dir = project_dir / "data" / "postgres_csv"

    if workers is None:
        workers = max(1, env_int("DAGBENCH_PG_WORKERS", DEFAULT_WORKERS))
    if unlogged is None:
        # These are throwaway benchmark fixtures rebuilt by `prepare.py`, and
        # skipping WAL is by far the largest single win in the load.
        unlogged = env_flag("DAGBENCH_PG_UNLOGGED", True)
    if use_extension is None:
        use_extension = env_flag("DAGBENCH_PG_EXTENSION", True)
    min_chunk_rows = max(1, env_int("DAGBENCH_PG_CHUNK_ROWS", MIN_CHUNK_ROWS))

    if not duckdb_file.exists():
        raise FileNotFoundError(f"DuckDB database not found: {duckdb_file}")

    if shutil.which("psql") is None:
        raise RuntimeError("psql is required to load data into Postgres")

    if use_extension and not postgres_extension_available():
        print("DuckDB postgres extension unavailable; falling back to CSV load")
        use_extension = False

    ensure_database(pg_config)
    run_psql(pg_config, f"create schema if not exists {quote_ident(schema)};")
    run_psql_file(pg_config, repo_root / "utils" / "bootstrap_postgres.sql")

    source_tables = read_source_tables(sources_path)

    con = duckdb.connect(str(duckdb_file), read_only=True)
    try:
        loaded_tables = [name for name in source_tables if table_exists(con, name)]

        if not loaded_tables:
            raise RuntimeError(
                f"No source tables from {sources_path} were found in {duckdb_file}"
            )

        if use_extension:
            load_tables_via_extension(
                pg_config,
                duckdb_file,
                con,
                schema,
                loaded_tables,
                unlogged,
                workers,
                min_chunk_rows,
            )
        else:
            load_tables_via_csv(pg_config, con, schema, loaded_tables, csv_dir, unlogged)
    finally:
        con.close()

    method = "duckdb postgres extension" if use_extension else "csv"
    persistence = "unlogged" if unlogged else "logged"
    print(
        f"Loaded {len(loaded_tables)} table(s) into {pg_config['dbname']}.{schema} "
        f"via {method} ({persistence} tables, {workers} worker(s))"
    )
    print(", ".join(loaded_tables))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel COPY streams (default: $DAGBENCH_PG_WORKERS or %d)" % DEFAULT_WORKERS,
    )
    parser.add_argument(
        "--logged",
        dest="unlogged",
        action="store_false",
        default=None,
        help="Create WAL-logged tables instead of the faster UNLOGGED default",
    )
    parser.add_argument(
        "--csv",
        dest="use_extension",
        action="store_false",
        default=None,
        help="Force the legacy CSV export + psql \\copy path",
    )
    args = parser.parse_args()

    load_project_sources_to_postgres(
        Path(args.project_dir),
        args.duckdb_path,
        workers=args.workers,
        unlogged=args.unlogged,
        use_extension=args.use_extension,
    )


if __name__ == "__main__":
    main()
