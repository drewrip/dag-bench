#!/usr/bin/env python3
"""
Export all tables from the 'main' schema of a DuckDB database to CSV files.
Each table is saved as <table_name>.csv in the same directory as the .duckdb file.

Usage:
    python duckcsv.py <path_to_database.duckdb>
"""

import sys
import os
import duckdb


def export_tables_to_csv(db_path: str) -> None:
    db_path = os.path.abspath(db_path)

    if not os.path.isfile(db_path):
        print(f"Error: file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    if not db_path.endswith(".duckdb"):
        print(
            f"Warning: '{db_path}' does not have a .duckdb extension.", file=sys.stderr
        )

    output_dir = os.path.dirname(db_path)

    con = duckdb.connect(db_path, read_only=True)

    try:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()

        if not tables:
            print("No tables found in the 'main' schema.")
            return

        print(f"Found {len(tables)} table(s) in schema 'main':\n")

        for (table_name,) in tables:
            csv_path = os.path.join(output_dir, f"{table_name}.csv")

            # Use DuckDB's native COPY … TO for fast, correct CSV export
            con.execute(
                f'COPY (SELECT * FROM main."{table_name}") '
                f"TO '{csv_path}' (HEADER, DELIMITER ',')"
            )

            row_count = con.execute(
                f'SELECT COUNT(*) FROM main."{table_name}"'
            ).fetchone()[0]

            print(f"  ✓  {table_name}  →  {csv_path}  ({row_count:,} row(s))")

        print("\nDone.")

    finally:
        con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python duckcsv.py <path_to_database.duckdb>")
        sys.exit(1)

    export_tables_to_csv(sys.argv[1])
