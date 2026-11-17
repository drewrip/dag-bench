#!/usr/bin/env python3
"""Tiny reference inputs for the Snowflake logic checks.

  python -m utils.snowflake_tiny prepare p01_iot m01_bad_view_mat ...
  python -m utils.snowflake_tiny prepare --all

Copies each project's fixed tiny input to projects/<p>/data/tiny/warehouse.duckdb.
DuckDB (profile output `duckdb_tiny`) and the Snowflake loader both read that
file, so both engines see identical input. The benchmark's own
data/warehouse.duckdb is never touched. The file name stays `warehouse.duckdb`
so the DuckDB database name in compiled SQL is the same as for `dev`.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CACHE = ROOT / "dbgen" / ".cache"

# project -> cached dbgen file. Smallest scale at which the DuckDB build passes
# (tests include not_empty):
# - p02's generator can't do sf=0.001 (empty-range panic): 0.005 (~228k rows);
# - p09 at 0.001 hits 0/0 -> NaN -> DECIMAL cast error: 0.005;
# - p01 and m01/m02/m03/m05 share p1 sf=0.01 (429k readings); on Snowflake the
#   m-projects read p01's schema, so it is loaded once;
# - m04/m06 select nothing below sf=0.1 (4.3M readings); they share their own
#   schema DAGBENCH_P01_IOT_SF0_1.
TINY = {
    "p01_iot": "p1_sf0_01",
    "p02_adtech": "p2_sf0_005",
    "p03_ecommerce": "p3_sf0_001",
    "p04_fraud": "p4_sf0_001",
    "p05_hr": "p5_sf0_001",
    "p06_logistics": "p6_sf0_001",
    "p07_saas": "p7_sf0_001",
    "p08_healthcare": "p8_sf0_001",
    "p09_gaming": "p9_sf0_005",
    "p10_energy": "p10_sf0_001",
    "p12_stripe": "p12_sf0_01",
    # The generator's invariant pass rejects sf<0.03 (restatement population).
    "p14_astronomer": "p14_v1_sf0_03",
    # Upstream's retail.duckdb as assembled by utils/retail_data.py (581 source
    # tables, ~570k rows), used as-is.
    "p13_snowflake": "p13_snowflake_v2",
    # Tuva's `small` input with pruned reference data (utils/tuva_data.py build_tiny).
    "p11_tuva": "p11_tiny",
    "m01_bad_view_mat": "p1_sf0_01",
    "m02_hidden_view_mat": "p1_sf0_01",
    "m03_good_shared_subplan": "p1_sf0_01",
    "m04_bad_shared_subplan": "p1_sf0_1",
    "m05_good_small_view_mat": "p1_sf0_01",
    "m06_bad_small_view_mat": "p1_sf0_1",
}


def tiny_path(project: str) -> Path:
    return ROOT / "projects" / project / "data" / "tiny" / "warehouse.duckdb"


def prepare(project: str) -> Path:
    if project not in TINY:
        raise SystemExit(f"snowflake_tiny: no tiny input defined for {project} yet")
    if project == "p11_tuva":
        from utils.tuva_data import tiny_path as tuva_tiny  # builds the cache on first use

        tuva_tiny()
    src = CACHE / f"{TINY[project]}.duckdb"
    if not src.exists():
        raise SystemExit(f"snowflake_tiny: {src} missing; generate it with the project's generate_data.py")
    dst = tiny_path(project)
    dst.parent.mkdir(parents=True, exist_ok=True)
    for stale in (dst, dst.with_suffix(".duckdb.wal")):
        stale.unlink(missing_ok=True)
    shutil.copy2(src, dst)
    print(f"{project}: {src.name} -> {dst.relative_to(ROOT)}")
    return dst


def main(argv: list[str]) -> int:
    if not argv or argv[0] != "prepare":
        print(__doc__)
        return 2
    projects = list(TINY) if argv[1:] == ["--all"] else argv[1:]
    for p in projects:
        prepare(p.strip("/").split("/")[-1])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
