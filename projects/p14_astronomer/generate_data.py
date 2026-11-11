#!/usr/bin/env python3
import sys
from pathlib import Path


def main() -> None:
    project_root = next(parent for parent in Path(__file__).resolve().parents if (parent / "dbgen").is_dir())
    sys.path.insert(0, str(project_root))

    from utils.astronomer_data import run_astronomer

    # de-bench's copperline world generator (utils/astronomer_data.py) instead of a dbgen generator.
    run_astronomer(Path(__file__).resolve().parent)


if __name__ == "__main__":
    main()
