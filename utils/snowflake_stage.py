#!/usr/bin/env python3
"""Run the Snowflake testing ladder for a stage (a list of projects).

  python -m utils.snowflake_stage free p01_iot ...            # rungs 1-3, $0
  python -m utils.snowflake_stage sf [--rungs 4-7] p01_iot ... # paid rungs, ONE budgeted run
  python -m utils.snowflake_stage retry p01_iot ...           # `dbt retry` on snowflake, budgeted

Rungs:
  1  utils/check_compiled_unchanged.sh check   (dev + postgres compiled SQL unchanged)
  2  utils/check_snowflake_compat.py           (lint + offline snowflake compile + sqlglot)
  3  tiny input + `dbt build --target duckdb_tiny` (reference outputs)
  4  empty source tables (free DDL) + `dbt run --empty --target snowflake`
  5  load the tiny input (skipped for free when the fingerprint matches)
  6  `dbt build --target snowflake`
  7  utils/compare_snowflake.py (leaf diffs)

`sf` wraps all of its Snowflake work in a single `utils.snowflake_budget run`,
so the stage pays for one warehouse resume and one suspend tail. A project that
fails a rung skips its later rungs; the others carry on.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
DBT = str(ROOT / ".venv" / "bin" / "dbt")

# Projects whose data tests fail by design (p14: de-bench's planted faults and
# retired rules; see its VENDORED.md), so `dbt build` would skip everything
# downstream. They run `dbt run --full-refresh` (must pass), then `dbt test`
# (may fail); rung 6 passes only if Snowflake's failing tests, with their
# failure counts, equal the DuckDB reference's.
RUN_THEN_TEST = {"p14_astronomer"}
# Projects whose upstream data tests aren't maintained (p13: VENDORED.md says
# `dbt run` is what's verified): `dbt run --full-refresh` only.
RUN_ONLY = {"p13_snowflake"}

# Rough warehouse minutes per project and paid rung, for the budget estimate.
EST_MIN = {4: 0.3, 5: 0.5, 6: 2.0, 7: 0.5}
EST_MIN_PROJECT = {"p13_snowflake": {4: 6, 5: 4, 6: 15, 7: 8}, "p11_tuva": {4: 3, 5: 3, 6: 8, 7: 4}}


def sh(cmd: list[str], cwd: Path = ROOT, env: dict | None = None) -> bool:
    print(f"$ {' '.join(cmd)}   (in {cwd.relative_to(ROOT) if cwd != ROOT else '.'})", flush=True)
    return subprocess.run(cmd, cwd=cwd, env=env).returncode == 0


def rung_list(spec: str) -> list[int]:
    out = []
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out += list(range(int(a), int(b or a) + 1))
    return out


def names(projects: list[str]) -> list[str]:
    return [p.strip("/").split("/")[-1] for p in projects]


def test_outcomes(pdir: Path, target_path: str) -> dict[str, tuple]:
    import json

    rr = json.loads((pdir / target_path / "run_results.json").read_text())
    return {r["unique_id"]: (r["status"], r.get("failures")) for r in rr["results"] if r["unique_id"].startswith("test.")}


# Big projects run with more threads on Snowflake: billing is per warehouse
# second, so concurrency lowers the cost (the profiles default to 4).
SF_THREADS = {"p13_snowflake": 8, "p11_tuva": 8}


def build(pdir: Path, target: str, target_path: str) -> bool:
    """`dbt build`, or run + test for RUN_THEN_TEST projects."""
    base = ["--target", target, "--target-path", target_path]
    if target == "snowflake" and pdir.name in SF_THREADS:
        base += ["--threads", str(SF_THREADS[pdir.name])]
    if pdir.name in RUN_ONLY:
        return sh([DBT, "run", "--full-refresh", *base], pdir)
    if pdir.name not in RUN_THEN_TEST:
        return sh([DBT, "build", *base], pdir)
    if not sh([DBT, "run", "--full-refresh", *base], pdir):
        return False
    sh([DBT, "test", *base], pdir)  # failures expected; compared below
    out = test_outcomes(pdir, target_path)
    (pdir / target_path / "test_outcomes.json").write_text(__import__("json").dumps(out, indent=1, sort_keys=True))
    bad = sorted(u for u, (st, _) in out.items() if st not in ("pass", "fail", "warn"))
    if bad:
        print(f"{pdir.name}: tests that errored (not just failed): {bad[:10]}")
        return False
    print(f"{pdir.name}: {sum(st == 'fail' for st, _ in out.values())} test(s) fail (allowed; compared across engines)")
    if target == "snowflake":
        import json

        ref = json.loads((pdir / "target/duckdb_tiny/test_outcomes.json").read_text())
        ref = {k: tuple(v) for k, v in ref.items()}
        diff = sorted(set(ref.items()) ^ set(out.items()))
        if diff:
            print(f"{pdir.name}: test outcomes differ from DuckDB (status, failures):")
            for u in sorted({u for u, _ in diff}):
                print(f"    {u}: duckdb {ref.get(u)} vs snowflake {out.get(u)}")
            return False
        print(f"{pdir.name}: test outcomes identical to DuckDB")
    return True


def run_free(projects: list[str], rungs: list[int]) -> dict[str, str]:
    status = {p: "ok" for p in projects}
    if 1 in rungs and not sh(["bash", "utils/check_compiled_unchanged.sh", "check", *projects]):
        print("rung 1 FAILED: compiled dev/postgres SQL changed; stopping the stage")
        return {p: "rung 1" for p in projects}
    if 2 in rungs:
        for p in projects:
            if not sh([PY, "utils/check_snowflake_compat.py", p]):
                status[p] = "rung 2"
    if 3 in rungs:
        for p in [p for p in projects if status[p] == "ok"]:
            pdir = ROOT / "projects" / p
            if not (sh([PY, "-m", "utils.snowflake_tiny", "prepare", p])
                    and build(pdir, "duckdb_tiny", "target/duckdb_tiny")):
                status[p] = "rung 3"
    return status


def run_sf_inner(projects: list[str], rungs: list[int]) -> dict[str, str]:
    status = {p: "ok" for p in projects}

    def step(p: str, rung: int, cmd: list[str], cwd: Path) -> None:
        if status[p] != "ok":
            return
        if not sh(cmd, cwd):
            status[p] = f"rung {rung}"

    for p in projects:
        pdir = ROOT / "projects" / p
        if 4 in rungs:
            step(p, 4, [PY, "load_snowflake.py", "--schema-only"], pdir)
            step(p, 4, [DBT, "run", "--empty", "--target", "snowflake", "--target-path", "target/snowflake",
                        *(["--threads", str(SF_THREADS[p])] if p in SF_THREADS else [])], pdir)
    for p in projects:
        if 5 in rungs:
            step(p, 5, [PY, "load_snowflake.py"], ROOT / "projects" / p)
    for p in projects:
        if 6 in rungs and status[p] == "ok" and not build(ROOT / "projects" / p, "snowflake", "target/snowflake"):
            status[p] = "rung 6"
    if 7 in rungs:
        todo = [p for p in projects if status[p] == "ok"]
        for p in todo:
            step(p, 7, [PY, "utils/compare_snowflake.py", p], ROOT)
    return status


def summary(status: dict[str, str]) -> int:
    print("\n=== stage summary ===")
    for p, s in status.items():
        print(f"  {'PASS' if s == 'ok' else 'FAIL at ' + s:16} {p}")
    sh([PY, "-m", "utils.snowflake_budget", "status"])
    return 0 if all(s == "ok" for s in status.values()) else 1


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["free", "sf", "retry", "_sf_inner"])
    ap.add_argument("--rungs", default=None)
    ap.add_argument("projects", nargs="+")
    args = ap.parse_args(argv)
    projects = names(args.projects)

    if args.mode == "free":
        return summary(run_free(projects, rung_list(args.rungs or "1-3")))

    if args.mode == "_sf_inner":
        if os.environ.get("DAGBENCH_SF_WRAPPED") != "1":
            raise SystemExit("internal mode; use `sf`")
        status = run_sf_inner(projects, rung_list(args.rungs or "4-7"))
        print("\n=== snowflake rungs ===")
        for p, s in status.items():
            print(f"  {'PASS' if s == 'ok' else 'FAIL at ' + s:16} {p}")
        return 0 if all(s == "ok" for s in status.values()) else 1

    if args.mode == "retry":
        est = EST_MIN[6] * len(projects)
        cmd = [PY, "-m", "utils.snowflake_budget", "run", "--estimate-min", f"{est:.1f}",
               "--label", f"retry {' '.join(projects)}", "--"]
        rc = 0
        for p in projects:
            # One budgeted run per project keeps `dbt retry` scoped to its own target dir.
            rc |= subprocess.run(cmd + [DBT, "retry", "--target-path", "target/snowflake"],
                                 cwd=ROOT / "projects" / p).returncode
        return rc

    rungs = rung_list(args.rungs or "4-7")
    est = sum(EST_MIN_PROJECT.get(p, EST_MIN).get(r, 0) for p in projects for r in rungs)
    return subprocess.run([PY, "-m", "utils.snowflake_budget", "run", "--estimate-min", f"{est:.1f}",
                           "--label", f"stage rungs {args.rungs or '4-7'}: {' '.join(projects)}", "--",
                           PY, "-m", "utils.snowflake_stage", "_sf_inner", "--rungs", args.rungs or "4-7",
                           *projects], cwd=ROOT).returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
