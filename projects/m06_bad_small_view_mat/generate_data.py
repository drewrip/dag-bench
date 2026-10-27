#!/usr/bin/env python3
import sys
from pathlib import Path


def main() -> None:
    project_root = next(parent for parent in Path(__file__).resolve().parents if (parent / "dbgen").is_dir())
    sys.path.insert(0, str(project_root))

    from dbgen.project_runner import run_project

    # Microbenchmarks reuse p01_iot's generated warehouse (and its dbgen cache).
    run_project("p01_iot")


if __name__ == "__main__":
    main()
