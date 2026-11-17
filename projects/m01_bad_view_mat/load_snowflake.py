#!/usr/bin/env python3
"""Microbenchmarks read p01_iot's tables on Snowflake (sources.yml points at
DAGBENCH_P01_IOT), so this loads p01's tiny input; it is a free no-op when
p01's schema already holds it."""
from pathlib import Path
import sys


def main() -> None:
    root = Path(__file__).resolve().parent
    while root != root.parent and not (root / ".git").exists():
        root = root.parent
    sys.path.insert(0, str(root))

    from utils.snowflake_bench_utils import main_for

    main_for(str(root / "projects" / "p01_iot" / "load_snowflake.py"))


if __name__ == "__main__":
    main()
