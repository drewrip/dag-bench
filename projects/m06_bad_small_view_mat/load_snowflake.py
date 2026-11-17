#!/usr/bin/env python3
"""m04 and m06 select nothing on p01's tiny input below sf=0.1, so on
Snowflake they share their own p1 sf=0.1 copy in DAGBENCH_P01_IOT_SF0_1
(sources.yml points there). Whichever of the two loads second is a free
no-op (same fingerprint)."""
from pathlib import Path
import sys


def main() -> None:
    root = Path(__file__).resolve().parent
    while root != root.parent and not (root / ".git").exists():
        root = root.parent
    sys.path.insert(0, str(root))

    from utils.snowflake_bench_utils import main_for

    main_for(__file__, schema="DAGBENCH_P01_IOT_SF0_1")


if __name__ == "__main__":
    main()
