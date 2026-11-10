#!/usr/bin/env python3
import sys
from pathlib import Path


def main() -> None:
    project_root = next(parent for parent in Path(__file__).resolve().parents if (parent / "dbgen").is_dir())
    sys.path.insert(0, str(project_root))

    from utils.retail_data import run_retail

    # data-eng-bench's fixed retail.duckdb, as-is (sf is ignored); see utils/retail_data.py.
    run_retail(Path(__file__).resolve().parent)


if __name__ == "__main__":
    main()
