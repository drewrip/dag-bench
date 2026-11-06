#!/usr/bin/env python3
import sys
from pathlib import Path


def main() -> None:
    project_root = next(parent for parent in Path(__file__).resolve().parents if (parent / "dbgen").is_dir())
    sys.path.insert(0, str(project_root))

    from utils.tuva_data import run_tuva

    # Tuva's synthetic payload replicated x sf instead of a dbgen generator.
    run_tuva(Path(__file__).resolve().parent)


if __name__ == "__main__":
    main()
