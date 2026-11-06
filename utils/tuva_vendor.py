#!/usr/bin/env python3
"""Vendor a tuva-core checkout into projects/p11_tuva.

Usage: utils/tuva_vendor.py <path to tuva-core checkout>

Re-running regenerates every vendored file from the checkout. Files we own
(profiles.yml, packages.yml, generate_data.py, load_postgres.py, the input
layer mappings, tests/generic, macros/dagbench/) are left alone. See
projects/p11_tuva/VENDORED.md for the list of transformations.
"""
from __future__ import annotations

import csv
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import jinja2
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DST = REPO_ROOT / "projects" / "p11_tuva"
PROJECT = "p11_tuva"

# Model families that are off in our configuration (data_quality_enabled and
# parity_enabled are false) and are not part of the benchmark DAG.
EXCLUDED_MODEL_DIRS = {"data_quality", "parity"}
# Seed loaders: seeds become sources loaded by generate_data.py.
EXCLUDED_MACROS = {"load_seed.sql", "versioned_seed_paths.sql"}

SEED_FOLDERS = {
    "terminology": "terminology",
    "value_sets": "value-sets",
    "provider_data": "provider-data",
}
SEED_HOOK = re.compile(r"load_versioned_seed\('([a-z_]+)'\s*,\s*'([^']+)'\)")
SYNTHETIC_HOOK = re.compile(r"load_versioned_synthetic_seed\('([^']+)'\)")


def render_duckdb_type(template: str) -> str:
    rendered = jinja2.Template(template).render(target={"type": "duckdb"})
    return " ".join(rendered.split())


def seed_manifest(src: Path) -> dict:
    """Seed name -> {object (bucket-relative path), source, columns[[name, type]]}."""
    seeds = {}
    for yml in sorted((src / "seeds").rglob("*.yml")):
        for seed in yaml.safe_load(yml.read_text())["seeds"]:
            name = seed["name"]
            config = seed.get("config", {})
            hook = config["post_hook"]
            if m := SEED_HOOK.search(hook):
                obj = f"{SEED_FOLDERS[m.group(1)]}/{m.group(2)}"
                source = "tuva_seeds"
            elif m := SYNTHETIC_HOOK.search(hook):
                obj = f"synthetic-data/{{size}}/{m.group(1)}.csv.gz"
                source = "tuva"
            else:
                raise ValueError(f"unrecognized post_hook for {name}: {hook}")
            csv_path = next((src / "seeds").rglob(f"{name}.csv"))
            with csv_path.open() as handle:
                header = next(csv.reader(handle))
            types = config["column_types"]
            columns = [[col, render_duckdb_type(types[col])] for col in header]
            seeds[name] = {"object": obj, "source": source, "columns": columns}
    return seeds


def copy_tree(src: Path, dst: Path, skip) -> None:
    for path in sorted(src.rglob("*")):
        rel = path.relative_to(src)
        if path.is_dir() or skip(rel):
            continue
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, out)


def rewrite(path: Path, seed_names: set[str]) -> None:
    text = path.read_text()
    new = text.replace("the_tuva_project", PROJECT)
    new = re.sub(
        r"ref\(\s*'([a-z0-9_]+)'\s*\)",
        lambda m: f"source('tuva_seeds', '{m.group(1)}')" if m.group(1) in seed_names else m.group(0),
        new,
    )
    if new != text:
        path.write_text(new)


MAPPING_MODELS = [
    "appointment", "condition", "eligibility", "encounter", "immunization", "lab_result",
    "location", "medical_claim", "medication", "observation", "patient", "pharmacy_claim",
    "practitioner", "procedure", "provider_attribution",
]
EXTENSIONS_BLOCK = re.compile(r"\{%-? set tuva_extensions -?%\}.*?\{%-? endset -?%\}\n*", re.S)


def write_input_layer_mappings(src: Path) -> None:
    """The connector side of the Input Layer contract: the integration_tests
    mappings, reading our raw `tuva` source instead of the synthetic seeds and
    without the integration-only extension fixture columns."""
    out_dir = DST / "models" / "input_layer_mapping"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    for name in MAPPING_MODELS:
        text = (src / "integration_tests" / "models" / f"{name}.sql").read_text()
        text = EXTENSIONS_BLOCK.sub("", text)
        text = re.sub(r"^\s*\{\{ tuva_extensions \}\}\n", "", text, flags=re.M)
        text = re.sub(r"^\s*, .* as (x|ext)_[a-z_]+\s*\n", "", text, flags=re.M)
        text = re.sub(r"ref\('the_tuva_project',\s*'(synthetic_data__[a-z_]+)'\)",
                      r"source('tuva', '\1')", text)
        text = text.replace("the_tuva_project", PROJECT)
        if "tuva_extensions" in text or "ref(" in text or re.search(r"\bx_[a-z_]+\b", text):
            raise ValueError(f"unexpected leftover in mapping {name}")
        (out_dir / f"{name}.sql").write_text(text)


# Exact-text fixes to vendored models for Postgres (upstream targets DuckDB,
# Snowflake, BigQuery, ...). Each must match exactly once.
PATCHES = [
    # Bare `null` in a CTE is typed text on Postgres, so the later
    # `yearly union all current_scope` fails (integer vs text).
    ("models/claims_preprocessing/provider_attribution/final/provider_attribution__provider_ranking.sql",
     "    , null as performance_year\n",
     "    , cast(null as {{ dbt.type_int() }}) as performance_year\n"),
]


def apply_patches() -> None:
    for rel, old, new in PATCHES:
        path = DST / rel
        text = path.read_text()
        assert text.count(old) == 1, (rel, old)
        path.write_text(text.replace(old, new))


MACRO_DEF = re.compile(r"\{%-?\s*macro\s+([a-z0-9_]+)\s*\(")


def drop_overridden_macros() -> list[str]:
    """Delete upstream definitions of macros that macros/dagbench/ redefines
    (dbt rejects duplicate macro names within a project)."""
    ours = {m for f in (DST / "macros" / "dagbench").glob("*.sql") for m in MACRO_DEF.findall(f.read_text())}
    dropped = []
    for path in (DST / "macros").rglob("*.sql"):
        if "dagbench" in path.parts:
            continue
        text = path.read_text()
        for name in ours & set(MACRO_DEF.findall(text)):
            block = re.compile(
                r"\{%-?\s*macro\s+" + name + r"\s*\(.*?\{%-?\s*endmacro\s*-?%\}\n*", re.S)
            text, n = block.subn("", text)
            assert n == 1, (path, name)
            dropped.append(name)
        path.write_text(text)
    missing = ours - set(dropped)
    if missing:
        print(f"note: dagbench macros with no upstream counterpart: {sorted(missing)}")
    return dropped


def write_sources(seeds: dict) -> None:
    sources = []
    for source_name in ("tuva", "tuva_seeds"):
        tables = [{"name": n} for n, s in seeds.items() if s["source"] == source_name]
        sources.append({
            "name": source_name,
            "schema": "{{ 'main' if target.type == 'duckdb' else target.schema }}",
            "tables": tables,
        })
    text = yaml.safe_dump({"version": 2, "sources": sources}, sort_keys=False, width=200)
    (DST / "models" / "sources.yml").write_text(
        "# Generated by utils/tuva_vendor.py. Raw tables written by generate_data.py.\n" + text
    )


def main() -> None:
    src = Path(sys.argv[1]).resolve()
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=src, check=True,
                         capture_output=True, text=True).stdout.strip()

    for sub in ("models", "macros"):
        for child in (DST / sub).glob("*") if (DST / sub).exists() else []:
            if child.name == "dagbench":
                continue
            shutil.rmtree(child) if child.is_dir() else child.unlink()

    copy_tree(src / "models", DST / "models",
              lambda rel: rel.parts[0] in EXCLUDED_MODEL_DIRS or rel.name.endswith("unit_tests.yml"))
    copy_tree(src / "macros", DST / "macros",
              lambda rel: rel.name in EXCLUDED_MACROS or rel.name == "generate_schema_name.sql")

    seeds = seed_manifest(src)
    seed_names = set(seeds)
    for path in list((DST / "models").rglob("*")) + list((DST / "macros").rglob("*")):
        if path.suffix in {".sql", ".yml"} and "input_layer_mapping" not in path.parts:
            rewrite(path, seed_names)

    (DST / "seed_manifest.json").write_text(json.dumps(
        {"upstream_sha": sha, "asset_version": "1.0.0", "seeds": seeds}, indent=1) + "\n")
    write_sources(seeds)
    write_input_layer_mappings(src)
    dropped = drop_overridden_macros()
    apply_patches()
    print(f"Vendored tuva-core {sha} into {DST.relative_to(REPO_ROOT)} ({len(seeds)} seeds -> sources; "
          f"replaced upstream macros: {', '.join(sorted(dropped))})")


if __name__ == "__main__":
    main()
