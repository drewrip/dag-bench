#!/usr/bin/env python3
from pathlib import Path
import sys


def main() -> None:
    root = Path(__file__).resolve().parent
    while root != root.parent and not (root / ".git").exists():
        root = root.parent
    sys.path.insert(0, str(root))

    from utils.snowflake_bench_utils import main_for

    main_for(__file__)


if __name__ == "__main__":
    main()
