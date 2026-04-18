from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
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


def create_table(pg_config: dict, schema: str, table_name: str, columns: list[tuple[str, str]]) -> None:
    column_sql = ", ".join(
        f"{quote_ident(column_name)} {map_type(column_type)}"
        for column_name, column_type in columns
    )
    run_psql(
        pg_config,
        f"drop table if exists {quote_ident(schema)}.{quote_ident(table_name)} cascade; "
        f"create table {quote_ident(schema)}.{quote_ident(table_name)} ({column_sql});",
    )


def load_csv_into_postgres(pg_config: dict, schema: str, table_name: str, csv_path: Path) -> None:
    qualified_table = f"{quote_ident(schema)}.{quote_ident(table_name)}"
    sql = (
        f"\\copy {qualified_table} "
        f"from '{csv_path.resolve().as_posix()}' with (format csv, header true)"
    )
    run_psql(pg_config, sql)


def load_project_sources_to_postgres(project_dir: Path, duckdb_path: str) -> None:
    project_dir = project_dir.resolve()
    repo_root = find_repo_root(project_dir)
    profile_path = project_dir / "profiles.yml"
    sources_path = project_dir / "models" / "sources.yml"
    pg_config = read_postgres_output(profile_path)
    schema = str(pg_config["schema"])
    duckdb_file = (project_dir / duckdb_path).resolve()
    csv_dir = project_dir / "data" / "postgres_csv"

    if not duckdb_file.exists():
        raise FileNotFoundError(f"DuckDB database not found: {duckdb_file}")

    if shutil.which("psql") is None:
        raise RuntimeError("psql is required to load data into Postgres")

    ensure_database(pg_config)
    run_psql(pg_config, f"create schema if not exists {quote_ident(schema)};")
    run_psql_file(pg_config, repo_root / "projects" / "_shared" / "bootstrap_postgres.sql")

    source_tables = read_source_tables(sources_path)
    loaded_tables: list[str] = []

    con = duckdb.connect(str(duckdb_file), read_only=True)
    try:
        for table_name in source_tables:
            if not table_exists(con, table_name):
                continue
            columns = duckdb_columns(con, table_name)
            export_csv(con, table_name, csv_dir / f"{table_name}.csv")
            create_table(pg_config, schema, table_name, columns)
            load_csv_into_postgres(pg_config, schema, table_name, csv_dir / f"{table_name}.csv")
            loaded_tables.append(table_name)
    finally:
        con.close()

    if not loaded_tables:
        raise RuntimeError(
            f"No source tables from {sources_path} were found in {duckdb_file}"
        )

    print(f"Loaded {len(loaded_tables)} table(s) into {pg_config['dbname']}.{schema}")
    print(", ".join(loaded_tables))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--duckdb-path", required=True)
    args = parser.parse_args()

    load_project_sources_to_postgres(Path(args.project_dir), args.duckdb_path)


if __name__ == "__main__":
    main()
