#!/usr/bin/env python3
"""Fail if the benchmark uses NUMERIC/DECIMAL without an explicit precision.

Arbitrary-precision NUMERIC isn't standard SQL and has no Arrow equivalent, so
every numeric column the benchmark produces must carry (p,s).

  * Default: lint every project's models for `numeric`/`decimal` written
    without `(p,s)`.
  * --postgres: additionally query each project's Postgres schema for numeric
    columns with no precision. This catches the implicit cases the lint can't
    see: Postgres gives round(), sum(), avg(), extract(), ... a bare NUMERIC
    result unless the expression is wrapped in an outermost cast.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.postgres_bench_utils import psql_env, read_postgres_output  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
PROJECTS = sorted(p for p in (ROOT / "projects").glob("[pm][0-9][0-9]_*") if p.is_dir())

UNSIZED = re.compile(r"\b(numeric|decimal)\b(?!\s*\(\s*\d+\s*,\s*\d+\s*\))", re.IGNORECASE)
LINE_COMMENT = re.compile(r"--.*$")


def lint_models() -> list[str]:
    findings = []
    for project in PROJECTS:
        for path in sorted((project / "models").rglob("*.sql")):
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                if UNSIZED.search(LINE_COMMENT.sub("", line)):
                    findings.append(f"{path.relative_to(ROOT)}:{lineno}: {line.strip()}")
    return findings


def check_postgres() -> list[str]:
    findings = []
    for project in PROJECTS:
        pg = read_postgres_output(project / "profiles.yml")
        sql = (
            "select table_name || '.' || column_name from information_schema.columns "
            f"where table_schema = '{pg['schema']}' "
            "and data_type = 'numeric' and numeric_precision is null "
            "order by table_name, ordinal_position"
        )
        cmd = [
            "psql", "-v", "ON_ERROR_STOP=1", "-At",
            "-h", str(pg["host"]), "-p", str(pg["port"]),
            "-U", str(pg["user"]), "-d", str(pg["dbname"]),
            "-c", sql,
        ]
        out = subprocess.run(cmd, check=True, env=psql_env(pg), capture_output=True, text=True)
        findings += [f"{pg['schema']}.{col}" for col in out.stdout.split()]
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--postgres",
        action="store_true",
        help="also check the built Postgres schemas for unconstrained numeric columns",
    )
    args = parser.parse_args()

    failed = False
    for label, findings in [("models", lint_models())] + (
        [("postgres", check_postgres())] if args.postgres else []
    ):
        if findings:
            failed = True
            print(f"{label}: {len(findings)} numeric type(s) without precision")
            for finding in findings:
                print(f"  {finding}")
        else:
            print(f"{label}: ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
