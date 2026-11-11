"""Source data for projects/p14_astronomer.

de-bench ships a deterministic world generator (vendored as
utils/astronomer_gen/gen_copperline, see utils/astronomer_vendor.py): a
simulated retailer (`sim_*` schemas) and the extracts its feeds land, in
schemas `raw` and `ops` of one DuckDB file. Upstream has two profiles, `full`
and `small` (fact volumes / 20); we pass a continuous factor instead,
FACTOR_PER_SF * sf (upstream's `small` is 0.05). Entity tables (stores, customers, products, calendars) do
not scale, by upstream's design.

The warehouse we build keeps each table the dbt project reads, in `main` as
`src_<schema>__<table>` (the identifiers in models/sources.yml). The
generator's other outputs (its landing tree of dated files, a second frozen
`northwave.duckdb`) are for de-bench's Airflow DAGs and are discarded.

The generator commits often and is fsync-bound on a journaling filesystem, so
it writes to /dev/shm when that exists (tens of times faster).

Output: <project>/data/warehouse.duckdb, cached as dbgen/.cache/p14_v<ASSEMBLY_VERSION>_sf<sf>.duckdb.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
GEN_DIR = REPO_ROOT / "utils" / "astronomer_gen"
CACHE_DIR = REPO_ROOT / "dbgen" / ".cache"
OUTPUT_PATH = Path("data/warehouse.duckdb")

# Generator factor per unit of sf. sf=1 is upstream's shipped `full` profile:
# a ~1.2 GB warehouse.duckdb, ~40M source rows (9.4M order lines).
FACTOR_PER_SF = 1.0
# Bump when the assembled warehouse changes, so stale caches are rebuilt.
ASSEMBLY_VERSION = 1


def source_tables(project_dir: Path) -> list[str]:
    data = yaml.safe_load((project_dir / "models" / "sources.yml").read_text())
    return [t["identifier"] for s in data["sources"] for t in s["tables"]]


def run_generator(factor: float, work: Path) -> Path:
    db = work / "copperline.duckdb"
    cmd = [sys.executable, "-m", "gen_copperline",
           "--timeline", str(GEN_DIR / "timeline.yaml"),
           "--planted", str(GEN_DIR / "planted.yaml"),
           "--db", str(db), "--landing", str(work / "landing"),
           "--profile", repr(factor)]
    env = {**os.environ, "PYTHONPATH": str(GEN_DIR)}
    subprocess.run(cmd, cwd=GEN_DIR, env=env, check=True)
    return db


def assemble(project_dir: Path, generated: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    con = duckdb.connect(str(output_path))
    try:
        con.execute(f"attach '{generated}' as gen (read_only)")
        for name in source_tables(project_dir):
            assert name.startswith("src_") and "__" in name, name
            schema, table = name[len("src_"):].split("__", 1)
            con.execute(f'create table main."{name}" as select * from gen."{schema}"."{table}"')
            rows = con.execute(f'select count(*) from main."{name}"').fetchone()[0]
            print(f"  {name}: {rows:,} rows")
        con.execute("detach gen")
    finally:
        con.close()


def generate(project_dir: Path, sf: float, output_path: Path) -> None:
    factor = FACTOR_PER_SF * sf
    shm = Path("/dev/shm")
    with tempfile.TemporaryDirectory(prefix="p14gen_", dir=shm if shm.is_dir() else CACHE_DIR) as work:
        print(f"Running the copperline generator at factor {factor:g} (sf={sf})")
        generated = run_generator(factor, Path(work))
        assemble(project_dir, generated, output_path)


def run_astronomer(project_dir: Path) -> None:
    """generate_data.py entry point: `generate_data.py <sf> [--no-constraints]`,
    run from the project directory (the dbgen/project_runner.py contract).
    --no-constraints is accepted and ignored: the sources have no keys."""
    sf = 1.0
    if len(sys.argv) > 1:
        try:
            sf = float(sys.argv[1])
        except ValueError:
            pass
    if sf <= 0:
        raise SystemExit(f"Scale factor must be positive, got {sf}")

    cache_path = CACHE_DIR / f"p14_v{ASSEMBLY_VERSION}_sf{str(sf).replace('.', '_')}.duckdb"
    output_path = project_dir / OUTPUT_PATH

    if output_path.exists():
        print(f"Deleting existing duckdb file: {output_path}")
        output_path.unlink()

    if cache_path.exists():
        print(f"Using cached duckdb file: {cache_path}")
    else:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(".duckdb.partial")
        generate(project_dir, sf, tmp_path)
        tmp_path.rename(cache_path)
        print(f"Cached generated duckdb file to: {cache_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cache_path, output_path)
