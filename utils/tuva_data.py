"""Source data for projects/p11_tuva.

Tuva has no data generator: its Input Layer is fed from Tuva's published
synthetic dataset and its reference data (terminology, value sets, provider
data) from Tuva's public S3 bucket. Both are used exactly as published; this
module does not scale them.

Tuva publishes two synthetic payloads (same 1,000 clinical patients; `large`
has ~80x the claims of `small`). The reference data is the same for both.
`generate_data.py` picks one:

  generate_data.py              -> large
  generate_data.py small|large  -> that dataset
  generate_data.py <sf>         -> warns that sf is ignored and picks the
                                   dataset whose NOMINAL_SF is nearest
                                   (log scale; sf < 0.1 -> small)

Downloads are cached once in dbgen/.cache/ (tuva_reference_<version>.duckdb,
tuva_synthetic_<size>_<version>.duckdb), and the assembled warehouse as
dbgen/.cache/p11_<size>.duckdb, which is copied to <project>/data/warehouse.duckdb.
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
OUTPUT_PATH = Path("data/warehouse.duckdb")

# Scale factor each published payload stands for, by medical claim lines (the
# volume that drives the heavy models): small has 2,075, large 167,995 (~1.2%).
NOMINAL_SF = {"small": 0.01, "large": 1.0}
DEFAULT_DATASET = "large"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_manifest(project_dir: Path) -> dict:
    return json.loads((project_dir / "seed_manifest.json").read_text())


def choose_dataset(args: list[str]) -> str:
    """Dataset for `generate_data.py [small|large|<sf>] [--no-constraints]`."""
    positional = [a for a in args if not a.startswith("--")]
    if not positional:
        return DEFAULT_DATASET
    arg = positional[0]
    if arg in NOMINAL_SF:
        return arg
    try:
        sf = float(arg)
    except ValueError:
        raise SystemExit(f"Expected a dataset ({', '.join(NOMINAL_SF)}) or a scale factor, got {arg!r}")
    if sf <= 0:
        raise SystemExit(f"Scale factor must be positive, got {sf}")
    # Nearest on a log scale; ties go to the larger dataset.
    dataset = min(NOMINAL_SF, key=lambda s: (abs(math.log(sf / NOMINAL_SF[s])), -NOMINAL_SF[s]))
    print(
        f"WARNING: p11_tuva does not scale with sf; it uses Tuva's published datasets as-is "
        f"({', '.join(f'{s} ~ sf={n:g}' for s, n in NOMINAL_SF.items())}). "
        f"Using '{dataset}' for sf={sf:g}.",
        file=sys.stderr,
    )
    return dataset


def download(manifest: dict, source: str, path: Path, size: str = "") -> Path:
    """The manifest's seeds from one source ('tuva_seeds' = reference data,
    'tuva' = a synthetic payload) as typed DuckDB tables, read the way
    upstream's duckdb__load_seed macro reads them."""
    if path.exists():
        return path
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".duckdb.partial")
    tmp_path.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp_path))
    try:
        con.execute("install httpfs; load httpfs; set s3_region = 'us-east-1'")
        for name, seed in manifest["seeds"].items():
            if seed["source"] != source:
                continue
            obj = seed["object"].format(size=size)
            uri = f"s3://{BUCKET}/tuva-core/{manifest['asset_version']}/{obj}*"
            columns = ", ".join(f"{sql_literal(c)}: {sql_literal(t)}" for c, t in seed["columns"])
            print(f"Downloading {uri}")
            con.execute(
                f"create table {quote_ident(name)} as select * from read_csv({sql_literal(uri)}, "
                f"nullstr = '', quote = '\"', escape = '\"', header = true, columns = {{{columns}}})"
            )
    finally:
        con.close()
    tmp_path.rename(path)
    return path


def assemble(manifest: dict, dataset: str, output_path: Path) -> None:
    version = manifest["asset_version"].replace(".", "_")
    reference = download(manifest, "tuva_seeds", CACHE_DIR / f"tuva_reference_{version}.duckdb")
    synthetic = download(manifest, "tuva", CACHE_DIR / f"tuva_synthetic_{dataset}_{version}.duckdb", dataset)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    con = duckdb.connect(str(output_path))
    try:
        con.execute(f"attach {sql_literal(str(reference))} as ref (read_only)")
        con.execute(f"attach {sql_literal(str(synthetic))} as syn (read_only)")
        for name, seed in manifest["seeds"].items():
            db = "syn" if seed["source"] == "tuva" else "ref"
            con.execute(f"create table {quote_ident(name)} as select * from {db}.main.{quote_ident(name)}")
    finally:
        con.close()


def run_tuva(project_dir: Path) -> None:
    """generate_data.py entry point, run from the project directory (the
    dbgen/project_runner.py contract). --no-constraints is accepted and
    ignored: Tuva's sources have no keys."""
    dataset = choose_dataset(sys.argv[1:])
    manifest = load_manifest(project_dir)
    cache_path = CACHE_DIR / f"p11_{dataset}.duckdb"
    output_path = project_dir / OUTPUT_PATH

    if output_path.exists():
        print(f"Deleting existing duckdb file: {output_path}")
        output_path.unlink()

    if cache_path.exists():
        print(f"Using cached duckdb file: {cache_path}")
    else:
        print(f"Assembling Tuva's '{dataset}' dataset")
        tmp_path = cache_path.with_suffix(".duckdb.partial")
        assemble(manifest, dataset, tmp_path)
        tmp_path.rename(cache_path)
        print(f"Cached generated duckdb file to: {cache_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cache_path, output_path)
