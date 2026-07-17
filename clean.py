#!/usr/bin/env python3
"""Recursively delete .duckdb files inside data/ directories under projects/."""

import sys
from pathlib import Path

PROJECTS_ROOT = Path("projects")


def clean() -> int:
    deleted = 0

    for project_dir in PROJECTS_ROOT.iterdir():
        if not project_dir.is_dir():
            continue

        for data_dir in project_dir.glob("data"):
            if not data_dir.is_dir():
                continue

            for duckdb_file in data_dir.rglob("*.duckdb"):
                duckdb_file.unlink()
                print(f"Deleted: {duckdb_file}")
                deleted += 1

    print(f"\nTotal deleted: {deleted}")
    return deleted


if __name__ == "__main__":
    if not PROJECTS_ROOT.is_dir():
        print(f"Error: {PROJECTS_ROOT} not found", file=sys.stderr)
        sys.exit(1)

    clean()
