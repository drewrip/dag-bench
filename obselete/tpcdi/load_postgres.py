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

    load_project_sources_to_postgres(current, "tpcdi.duckdb")


if __name__ == "__main__":
    main()
