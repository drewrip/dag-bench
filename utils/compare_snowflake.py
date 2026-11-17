#!/usr/bin/env python3
"""Leaf-model diff: Snowflake vs the DuckDB reference build (rung 7 of utils/snowflake_stage.py).

  python -m utils.snowflake_budget run --estimate-min 2 -- python utils/compare_snowflake.py p01_iot ...

Expects both builds of each project:
  DuckDB:    dbt build --target duckdb_tiny --target-path target/duckdb_tiny
  Snowflake: dbt build --target snowflake   --target-path target/snowflake

Leaf models are the project's models that no other model reads (from
target/snowflake/manifest.json). For each leaf:
- always: row count, and per column count / count(distinct) (non-float) /
  min / max / sum (numeric);
- when the leaf has <= --max-rows rows: an order-insensitive multiset diff of
  all rows: column names lower-cased, numbers compared with relative tolerance
  --tol (absolute below 1e-9), timestamps to the microsecond, a timestamp at
  midnight equals the same date, strings exact.
Numeric columns also accept a difference of one unit in the last place of
their declared scale, listed in the report as ties, not hidden:
- DuckDB DECIMAL(p,s): its avg() over DECIMAL is a DOUBLE, so an exact tie
  such as 78.335 rounds to 78.33 there but to 78.34 on Snowflake (and Postgres);
- a Snowflake NUMBER(p,s) where DuckDB has a DOUBLE: Snowflake divides and
  averages in fixed point (1/28 -> 0.035714), DuckDB in floating point.
vars.dagbench_compare_tolerance ({column: relative tolerance}) loosens --tol
for named columns, e.g. ratios over Snowflake's avg(int), which is a
NUMBER with 6 decimals where DuckDB's is a DOUBLE.
Columns in vars.dagbench_volatile_columns (dbt_project.yml; default
report_ts, generated_at, created_at) are ignored. Columns in
vars.dagbench_unordered_list_columns ({column: separator}) hold string_agg
results without ORDER BY: their elements are sorted before comparing, and
min/max/count(distinct) are skipped for them.

Writes errors/snowflake_diff/<project>.md; exits 1 on any mismatch.
"""
from __future__ import annotations

import argparse
import bisect
import datetime as dt
import json
import math
import re
import os
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.snowflake_bench_utils import TINY_DUCKDB, connect  # noqa: E402

DEFAULT_VOLATILE = ["report_ts", "generated_at", "created_at"]
NUMERIC_DUCK = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER",
                "UBIGINT", "UHUGEINT", "DECIMAL", "DOUBLE", "FLOAT", "REAL")
FLOAT_DUCK = ("DOUBLE", "FLOAT", "REAL")


# ------------------------------------------------------------------ manifest

def leaf_models(manifest_path: Path, project: str) -> list[dict]:
    m = json.loads(manifest_path.read_text())
    models = {uid: n for uid, n in m["nodes"].items()
              if n["resource_type"] == "model" and n["package_name"] == project
              and n["config"].get("materialized") != "ephemeral" and n["config"].get("enabled", True)}
    has_child = set()
    for n in models.values():
        for dep in n["depends_on"]["nodes"]:
            if dep in models:
                has_child.add(dep)
    return sorted((n for uid, n in models.items() if uid not in has_child), key=lambda n: n["name"])


def project_vars(pdir: Path) -> dict:
    return yaml.safe_load((pdir / "dbt_project.yml").read_text()).get("vars") or {}


def volatile_columns(pdir: Path) -> set[str]:
    vals = project_vars(pdir).get("dagbench_volatile_columns", DEFAULT_VOLATILE)
    return {v.lower() for v in vals}


def unordered_list_columns(pdir: Path) -> dict[str, str]:
    return {k.lower(): v for k, v in (project_vars(pdir).get("dagbench_unordered_list_columns") or {}).items()}


def list_value(v):
    """DuckDB LIST (a Python list) and Snowflake ARRAY (JSON text) alike."""
    if isinstance(v, str):
        v = json.loads(v)
    return None if v is None else ("list", tuple(norm(x) for x in v))


def sort_list(v, sep: str):
    return v if not isinstance(v, str) else sep.join(sorted(v.split(sep)))


# --------------------------------------------------------------- normalizing

def norm(v):
    """Comparable form of a cell."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, (int, float)):
        return v  # 3 == 3.0 and they hash alike, so exact matches still pair up
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)
        if v.time() == dt.time(0):
            return ("date", v.date().isoformat())
        return ("ts", v.isoformat(timespec="microseconds"))
    if isinstance(v, dt.date):
        return ("date", v.isoformat())
    if isinstance(v, dt.timedelta):
        return v.total_seconds()  # DuckDB INTERVAL; Snowflake has seconds (dagbench_ts_diff)
    if isinstance(v, dt.time):
        return ("time", v.isoformat(timespec="microseconds"))
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    return str(v)


def sort_key(row: tuple) -> tuple:
    out = []
    for v in row:
        if v is None:
            out.append((0, ""))
        elif isinstance(v, bool):
            out.append((1, str(v)))
        elif isinstance(v, (int, float)):
            out.append((2, f"{v:.6g}" if isinstance(v, float) else f"{float(v):.6g}"))
        else:
            out.append((3, str(v)))
    return tuple(out)


def decimal_ulp(dtype: str) -> float:
    m = re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", dtype.upper().strip())
    return 10.0 ** -int(m.group(2)) if m and int(m.group(2)) > 0 else 0.0


def close(a, b, tol: float, ulp: float = 0.0) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
            return True
        diff = abs(float(a) - float(b))
        return diff <= 1e-9 or diff <= tol * max(abs(float(a)), abs(float(b))) or diff <= ulp * (1 + 1e-9)
    return a == b


def rows_equal(ra: tuple, rb: tuple, tol, ulps: list[float] | None = None) -> bool:
    ulps = ulps or [0.0] * len(ra)
    tols = tol if isinstance(tol, list) else [tol] * len(ra)
    return all(close(a, b, t, u) for a, b, t, u in zip(ra, rb, tols, ulps))


def multiset_diff(da: list[tuple], db: list[tuple], tol,
                  ulps: list[float] | None = None) -> tuple[list, list, list]:
    """(rows only in duckdb, rows only in snowflake, (duckdb, snowflake) pairs
    that match only within one decimal ulp), with numeric tolerance."""
    ca, cb = Counter(da), Counter(db)
    exact = ca & cb
    ra = sorted((ca - exact).elements(), key=sort_key)
    rb = sorted((cb - exact).elements(), key=sort_key)
    keys_b = [sort_key(r) for r in rb]
    only_a, used, ties = [], [False] * len(rb), []
    for row in ra:
        # Rows equal within tolerance sort next to each other (keys use 6 significant digits).
        pos = bisect.bisect_left(keys_b, sort_key(row))
        window = range(max(0, pos - 25), min(len(rb), pos + 25))
        hit = next((j for j in window if not used[j] and rows_equal(row, rb[j], tol)), None)
        if hit is None and ulps and any(ulps):
            hit = next((j for j in window if not used[j] and rows_equal(row, rb[j], tol, ulps)), None)
            if hit is not None:
                ties.append((row, rb[hit]))
        if hit is None:
            only_a.append(row)
        else:
            used[hit] = True
    only_b = [r for r, u in zip(rb, used) if not u]
    return only_a, only_b, ties


# ------------------------------------------------------------------- engines

def duck_columns(con, rel: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(f"describe {rel}").fetchall()]


def duck_rel(node: dict) -> str:
    return node["relation_name"]


def sf_columns(cur, rel: str) -> list[str]:
    cur.execute(f"select * from {rel} limit 0")
    return [d[0] for d in cur.description]


def sf_scales(cur) -> dict[str, int]:
    """Scale of each fixed-point (NUMBER) column of the last result, by lower-case name."""
    return {d[0].lower(): d[5] for d in cur.description if d[1] == 0 and d[5]}


def fingerprint_sql(rel: str, cols: list[tuple[str, str]], sf: bool, lists: dict | None = None) -> str:
    parts = ["count(*)"]
    for name, dtype in cols:
        q = f'"{name.upper()}"' if sf else f'"{name}"'
        base = dtype.upper().split("(")[0]
        parts.append(f"count({q})")
        if name.lower() in (lists or {}) or dtype.endswith("[]"):
            continue  # order-dependent text / arrays: only the full-row diff applies
        if base not in FLOAT_DUCK:
            parts.append(f"count(distinct {q})")
        parts.append(f"min({q})")
        parts.append(f"max({q})")
        if base in NUMERIC_DUCK:
            parts.append(f"sum(cast({q} as {'float' if sf else 'double'}))")
    return f"select {', '.join(parts)} from {rel}"


# ---------------------------------------------------------------------- main

def compare_project(project: str, max_rows: int, tol: float, workers: int = 8) -> tuple[bool, list[str]]:
    pdir = ROOT / "projects" / project
    sf_manifest = pdir / "target" / "snowflake" / "manifest.json"
    dk_manifest = pdir / "target" / "duckdb_tiny" / "manifest.json"
    for p in (sf_manifest, dk_manifest):
        if not p.exists():
            return False, [f"missing {p.relative_to(ROOT)}; build both targets first"]
    dk_nodes = {n["unique_id"]: n for n in json.loads(dk_manifest.read_text())["nodes"].values()}
    leaves = leaf_models(sf_manifest, project)
    vol = volatile_columns(pdir)
    max_rows = max(max_rows, int(project_vars(pdir).get("dagbench_compare_max_rows", 0)))
    col_tol = {k.lower(): float(v) for k, v in (project_vars(pdir).get("dagbench_compare_tolerance") or {}).items()}
    lists = unordered_list_columns(pdir)
    duck = duckdb.connect(str(pdir / TINY_DUCKDB), read_only=True)
    report = [f"# Snowflake vs DuckDB leaf diff: {project}", "",
              f"{len(leaves)} leaf models; tolerance {tol}; full-row diff up to {max_rows} rows; "
              f"ignored columns: {', '.join(sorted(vol)) or '-'}; "
              f"unordered list columns: {', '.join(sorted(lists)) or '-'}; "
              f"column tolerances: {col_tol or '-'}", ""]
    nondet = {m.lower() for m in project_vars(pdir).get("dagbench_nondeterministic_models", [])}
    unstable = {m.lower() for m in project_vars(pdir).get("dagbench_unstable_models", [])}

    def check_leaf(node: dict, duck, sf_cur) -> tuple[list[str], list[str]]:
        name = node["name"]
        dk_rel = duck_rel(dk_nodes[node["unique_id"]])
        sf_rel = node["relation_name"]
        problems: list[str] = []
        notes: list[str] = []
        dcols_all = duck_columns(duck, dk_rel)
        scols_all = sf_columns(sf_cur, sf_rel)
        scales = sf_scales(sf_cur)
        dnames = [c.lower() for c, _ in dcols_all]
        snames = [c.lower() for c in scols_all]
        if sorted(dnames) != sorted(snames):
            problems.append(f"column sets differ: duckdb-only {sorted(set(dnames) - set(snames))}, "
                            f"snowflake-only {sorted(set(snames) - set(dnames))}")
        cols = [(c, t) for c, t in dcols_all if c.lower() in set(snames) and c.lower() not in vol]
        if name.lower() in unstable:
            # Even the row count differs between two runs on one engine (VENDORED.md).
            notes.append("unstable upstream (row count differs between runs): columns only")
            return problems, notes
        if name.lower() in nondet:
            # Upstream picks rows over orderings with ties (VENDORED.md): only the row count is defined.
            n_d = duck.execute(f"select count(*) from {dk_rel}").fetchone()[0]
            sf_cur.execute(f"select count(*) from {sf_rel}")
            n_s = sf_cur.fetchone()[0]
            if n_d != n_s:
                problems.append(f"row count: duckdb {n_d} vs snowflake {n_s}")
            notes.append("nondeterministic upstream (ties): row count and columns only")
            return problems, notes
        def ulp(c: str, t: str) -> float:
            """One unit in the last place of the coarser of the two declared scales:
            a DuckDB DECIMAL(p,s), or a Snowflake NUMBER(p,s) where DuckDB has a
            DOUBLE (Snowflake divides and averages in fixed point, 6+ decimals)."""
            u = decimal_ulp(t)
            if t.upper().split("(")[0] in FLOAT_DUCK + ("DECIMAL",) and c.lower() in scales:
                u = max(u, 10.0 ** -scales[c.lower()])
            return u

        if not cols:
            problems.append("no comparable columns")
        else:
            dfp = duck.execute(fingerprint_sql(dk_rel, cols, sf=False, lists=lists)).fetchone()
            sf_cur.execute(fingerprint_sql(sf_rel, cols, sf=True, lists=lists))
            sfp = sf_cur.fetchone()
            nrows = dfp[0]
            if dfp[0] != sfp[0]:
                problems.append(f"row count: duckdb {dfp[0]} vs snowflake {sfp[0]}")
            labels, allow = ["count(*)"], [0.0]
            for c, t in cols:
                base = t.upper().split("(")[0]
                u = ulp(c, t)
                if c.lower() in lists or t.endswith("[]"):
                    labels += [f"count({c})"]
                    allow += [0.0]
                    continue
                labels += [f"count({c})"] + ([f"count(distinct {c})"] if base not in FLOAT_DUCK else []) \
                    + [f"min({c})", f"max({c})"] + ([f"sum({c})"] if base in NUMERIC_DUCK else [])
                # count(distinct) can move by the number of rounding ties; sums by rows * ulp.
                allow += [0.0] + ([0.0] if base not in FLOAT_DUCK else []) + [u, u] \
                    + ([u * nrows] if base in NUMERIC_DUCK else [])
            lab_tol = [max(tol, max((col_tol.get(c.lower(), 0.0) for c, _ in cols if lab.endswith(f"({c})")), default=0.0))
                       for lab in labels]
            # A sum's tolerance scales with its column's magnitude, not with the sum,
            # which can be ~0 (deviations from a mean).
            dval = dict(zip(labels, dfp))
            for i, (lab, c_t) in enumerate((lab, next(((c, t) for c, t in cols if lab == f"sum({c})"), None)) for lab in labels):
                if c_t and lab.startswith("sum("):
                    c = c_t[0]
                    mags = [abs(float(v)) for v in (dval.get(f"min({c})"), dval.get(f"max({c})")) if v is not None]
                    allow[i] = max(allow[i], lab_tol[i] * nrows * max(mags, default=0.0))
            for lab, a, b, u, t in zip(labels, dfp, sfp, allow, lab_tol):
                if lab.startswith("count(distinct") and any(ulp(c, t) for c, t in cols if lab == f"count(distinct {c})"):
                    u = max(1.0, 0.01 * nrows)
                if not close(norm(a), norm(b), t, u):
                    problems.append(f"{lab}: duckdb {a!r} vs snowflake {b!r}")
            if nrows <= max_rows and dfp[0] == sfp[0]:
                sel_d = ", ".join(f'"{c}"' for c, _ in cols)
                sel_s = ", ".join(f'"{c.upper()}"' for c, _ in cols)
                seps = [lists.get(c.lower()) for c, _ in cols]
                arrays = [t.endswith("[]") for _, t in cols]

                def prep(r):
                    return tuple(list_value(v) if arr else norm(sort_list(v, sep) if sep else v)
                                 for v, sep, arr in zip(r, seps, arrays))

                drows = [prep(r) for r in duck.execute(f"select {sel_d} from {dk_rel}").fetchall()]
                sf_cur.execute(f"select {sel_s} from {sf_rel}")
                srows = [prep(r) for r in sf_cur.fetchall()]
                tols = [max(tol, col_tol.get(c.lower(), 0.0)) for c, _ in cols]
                only_d, only_s, ties = multiset_diff(drows, srows, tols, [ulp(c, t) for c, t in cols])
                if ties:
                    notes.append(f"{len(ties)} row(s) equal only within one unit of the declared scale "
                                 f"(DOUBLE vs fixed point; see the docstring):")
                    notes += [f"  duckdb {a} / snowflake {b}" for a, b in ties[:5]]
                if only_d or only_s:
                    problems.append(f"rows differ: {len(only_d)} only in duckdb, {len(only_s)} only in snowflake")
                    hdr = " | ".join(c for c, _ in cols)
                    for label, rs in (("duckdb only", only_d), ("snowflake only", only_s)):
                        if rs:
                            problems.append(f"  first {min(5, len(rs))} {label} ({hdr}):")
                            problems += [f"    {r}" for r in rs[:5]]
        return problems, notes

    import threading
    from concurrent.futures import ThreadPoolExecutor

    local = threading.local()
    conns = []

    def worker(node: dict):
        if not hasattr(local, "sf"):
            local.sf = connect()
            conns.append(local.sf)
            local.duck = duck.cursor()
        sf_cur = local.sf.cursor()
        try:
            return check_leaf(node, local.duck, sf_cur)
        except Exception as exc:  # noqa: BLE001 - one leaf's failure is that leaf's result
            return [f"error: {type(exc).__name__}: {str(exc).splitlines()[0][:300]}"], []
        finally:
            sf_cur.close()

    ok_all = True
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(worker, leaves))
    for c in conns:
        c.close()
    for node, (problems, notes) in zip(leaves, results):
        name = node["name"]
        status = "OK" if not problems else "MISMATCH"
        ok_all &= not problems
        if problems or len(leaves) <= 200:
            print(f"  {status:8} {project}.{name}" + (f" ({len(problems)} issue(s))" if problems else ""))
        report.append(f"## {name}: {status}")
        report += [f"- {p}" if not p.startswith("  ") else f"  {p.strip()}" for p in problems]
        report += [f"- note: {n}" if not n.startswith("  ") else f"  {n.strip()}" for n in notes]
        report.append("")
    print(f"  {sum(1 for p, _ in results if not p)}/{len(leaves)} leaves match")
    duck.close()
    out = ROOT / "errors" / "snowflake_diff" / f"{project}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(report))
    return ok_all, report


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("projects", nargs="+")
    ap.add_argument("--max-rows", type=int, default=20_000)
    ap.add_argument("--tol", type=float, default=1e-6)
    ap.add_argument("--workers", type=int, default=8, help="parallel Snowflake sessions (less wall time = fewer credits)")
    args = ap.parse_args(argv)
    if os.environ.get("DAGBENCH_SF_WRAPPED") != "1":
        raise SystemExit("run this under `python -m utils.snowflake_budget run -- ...`")
    failed = []
    for p in args.projects:
        p = p.strip("/").split("/")[-1]
        print(f"[{p}]", flush=True)
        ok, _ = compare_project(p, args.max_rows, args.tol, args.workers)
        if not ok:
            failed.append(p)
    print("compare:", "all leaves match" if not failed else f"MISMATCH in {', '.join(failed)} "
          f"(see errors/snowflake_diff/)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
