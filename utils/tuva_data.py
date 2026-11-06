"""Source data for projects/p11_tuva.

Tuva has no data generator: its Input Layer is fed from Tuva's published
synthetic dataset and its reference data (terminology, value sets, provider
data) from Tuva's public S3 bucket. This module

  1. downloads every seed listed in p11_tuva/seed_manifest.json once into
     dbgen/.cache/tuva_raw_<asset version>.duckdb (the `large` synthetic payload);
  2. scales the 15 synthetic (Input Layer) tables by `sf`, see `scale()`;
  3. writes the result to <project>/data/warehouse.duckdb, caching it as
     dbgen/.cache/p11_sf<sf>.duckdb like dbgen/project_runner.py does.

Reference tables are copied unchanged at every scale factor.
"""
from __future__ import annotations

import json
import math
import shutil
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = REPO_ROOT / "dbgen" / ".cache"
BUCKET = "tuva-public-resources"
SYNTHETIC_SIZE = "large"
OUTPUT_PATH = Path("data/warehouse.duckdb")


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_manifest(project_dir: Path) -> dict:
    return json.loads((project_dir / "seed_manifest.json").read_text())


def download_raw(manifest: dict) -> Path:
    """All seeds as typed DuckDB tables, read the way upstream's
    duckdb__load_seed macro reads them."""
    raw_path = CACHE_DIR / f"tuva_raw_{manifest['asset_version'].replace('.', '_')}.duckdb"
    if raw_path.exists():
        return raw_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = raw_path.with_suffix(".duckdb.partial")
    tmp_path.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp_path))
    try:
        con.execute("install httpfs; load httpfs; set s3_region = 'us-east-1'")
        for name, seed in manifest["seeds"].items():
            obj = seed["object"].format(size=SYNTHETIC_SIZE)
            uri = f"s3://{BUCKET}/tuva-core/{manifest['asset_version']}/{obj}*"
            columns = ", ".join(f"{sql_literal(c)}: {sql_literal(t)}" for c, t in seed["columns"])
            print(f"Downloading {uri}")
            con.execute(
                f"create table {quote_ident(name)} as select * from read_csv({sql_literal(uri)}, "
                f"nullstr = '', quote = '\"', escape = '\"', header = true, columns = {{{columns}}})"
            )
    finally:
        con.close()
    tmp_path.rename(raw_path)
    return raw_path


# The `large` synthetic payload is 1,000 people / 168k medical claim lines, which
# the ~46M rows of reference data dwarf. sf=1 replicates it 100x (100k people,
# ~16.8M medical claim lines), roughly the size of the other projects at sf=1.
REPLICAS_PER_SF = 100

# Identifiers scoped to a single person (and the person itself). Replica r > 0
# gets "_r<r>" appended to each, consistently across tables, so every replica is
# a disjoint, fully joinable copy. Provider-side identifiers (NPIs, TINs,
# practitioner/location ids, payer/plan) are shared and left unchanged, so the
# terminology and provider joins still resolve.
PERSON_SCOPED_IDS = {
    "person_id", "patient_id", "member_id", "subscriber_id", "claim_id", "encounter_id",
    "appointment_id", "source_condition_id", "immunization_id", "lab_result_id",
    "medication_id", "observation_id", "source_procedure_id",
}


def scale(raw_path: Path, manifest: dict, sf: float, output_path: Path) -> None:
    replicas = sf * REPLICAS_PER_SF
    full = math.floor(replicas + 1e-9)
    frac = replicas - full
    # (replica, kept fraction of persons) pairs; the partial replica keeps a
    # hash-sampled subset of people so their claims/eligibility/clinical rows
    # stay together.
    parts = [(r, 1.0) for r in range(full)]
    if frac > 1e-9:
        parts.append((full, frac))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    con = duckdb.connect(str(output_path))
    try:
        con.execute(f"attach {sql_literal(str(raw_path))} as raw (read_only)")
        for name, seed in manifest["seeds"].items():
            table = quote_ident(name)
            column_names = [c for c, _ in seed["columns"]]
            if seed["source"] != "tuva" or "person_id" not in column_names:
                con.execute(f"create table {table} as select * from raw.main.{table}")
                continue
            selects = []
            for r, keep in parts:
                exprs = [
                    f"{quote_ident(c)} || '_r{r}' as {quote_ident(c)}"
                    if r > 0 and c in PERSON_SCOPED_IDS else quote_ident(c)
                    for c in column_names
                ]
                where = ""
                if keep < 1.0:
                    where = (f" where hash(person_id || '_r{r}') % 10000 "
                             f"< {int(round(keep * 10000))}")
                selects.append(f"select {', '.join(exprs)} from raw.main.{table}{where}")
            con.execute(f"create table {table} as " + "\nunion all\n".join(selects))
    finally:
        con.close()


def run_tuva(project_dir: Path) -> None:
    """generate_data.py entry point: `generate_data.py <sf> [--no-constraints]`,
    run from the project directory (the dbgen/project_runner.py contract).
    --no-constraints is accepted and ignored: Tuva's sources have no keys."""
    sf = 1.0
    if len(sys.argv) > 1:
        try:
            sf = float(sys.argv[1])
        except ValueError:
            pass
    if sf <= 0:
        raise SystemExit(f"Scale factor must be positive, got {sf}")

    manifest = load_manifest(project_dir)
    cache_path = CACHE_DIR / f"p11_sf{str(sf).replace('.', '_')}.duckdb"
    output_path = project_dir / OUTPUT_PATH

    if output_path.exists():
        print(f"Deleting existing duckdb file: {output_path}")
        output_path.unlink()

    if cache_path.exists():
        print(f"Using cached duckdb file: {cache_path}")
    else:
        raw_path = download_raw(manifest)
        tmp_path = cache_path.with_suffix(".duckdb.partial")
        print(f"Scaling Tuva synthetic data to sf={sf} ({sf * REPLICAS_PER_SF:g} replicas)")
        scale(raw_path, manifest, sf, tmp_path)
        tmp_path.rename(cache_path)
        print(f"Cached generated duckdb file to: {cache_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cache_path, output_path)
