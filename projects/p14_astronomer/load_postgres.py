#!/usr/bin/env python3
from pathlib import Path
import sys


def main() -> None:
    current = Path(__file__).resolve().parent
    root = current
    while root != root.parent and not (root / ".git").exists():
        root = root.parent
    sys.path.insert(0, str(root))

    from utils.postgres_bench_utils import load_project_sources_to_postgres

    # Text columns collate as "C": upstream's models sort and compare strings
    # (string_agg(... order by ...), row_number() over string keys), and DuckDB
    # compares bytes, while the benchmark database's default collation doesn't.
    load_project_sources_to_postgres(current, "data/warehouse.duckdb", text_collation="C")


if __name__ == "__main__":
    main()
