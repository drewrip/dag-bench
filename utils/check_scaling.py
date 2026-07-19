#!/usr/bin/env python3
"""Measure query runtime and operator cardinality scaling across scale factors.

For each benchmark project under projects/, generates data at a handful of
scale factors, replays the compiled model SQL from target/manifest.json in
topological order (materializing views/tables as it goes), and records for
each (project, scale factor) pair:
  - total runtime of the CREATE TABLE queries (via EXPLAIN (ANALYZE, FORMAT JSON))
  - the largest operator cardinality seen across all of those query plans
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = REPO_ROOT / "projects"
SCALE_FACTORS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 2.0]
PLOT_PATH = REPO_ROOT / "utils" / "scaling_plot.png"


def find_projects() -> list[Path]:
    projects = []
    for path in sorted(PROJECTS_DIR.iterdir()):
        if (path / "dbt_project.yml").exists() and (path / "target" / "manifest.json").exists():
            projects.append(path)
    return projects


def topological_model_order(manifest: dict) -> list[str]:
    nodes = manifest["nodes"]
    visited: set[str] = set()
    order: list[str] = []

    def visit(uid: str) -> None:
        if uid in visited:
            return
        visited.add(uid)
        for dep in nodes[uid]["depends_on"]["nodes"]:
            if dep in nodes and nodes[dep]["resource_type"] == "model":
                visit(dep)
        order.append(uid)

    for uid, node in nodes.items():
        if node["resource_type"] == "model":
            visit(uid)
    return order


def generate_data(project_dir: Path, sf: float) -> None:
    subprocess.run(
        [sys.executable, "generate_data.py", str(sf)],
        cwd=project_dir,
        check=True,
        capture_output=True,
        text=True,
    )


def max_operator_cardinality(plan_node: dict) -> int:
    best = plan_node.get("operator_cardinality", 0) or 0
    for child in plan_node.get("children", []):
        best = max(best, max_operator_cardinality(child))
    return best


def output_cardinality(plan_node: dict) -> int:
    """Rows actually written to the table by a CREATE TABLE AS query.

    The CREATE_TABLE_AS operator itself reports a cardinality of 1 (a DDL
    "success" sentinel, not a row count) — the true row count is on its
    child, the root of the underlying SELECT plan. DuckDB uses
    BATCH_CREATE_TABLE_AS instead of CREATE_TABLE_AS for some plans (e.g.
    when the query has an ORDER BY), hence the suffix match.
    """
    if (plan_node.get("operator_name") or "").endswith("CREATE_TABLE_AS"):
        children = plan_node.get("children", [])
        return children[0].get("operator_cardinality", 0) if children else 0
    for child in plan_node.get("children", []):
        found = output_cardinality(child)
        if found:
            return found
    return 0


def run_queries(project_dir: Path, manifest: dict, order: list[str]) -> tuple[float, int, int]:
    duckdb_path = project_dir / "data" / "warehouse.duckdb"
    con = duckdb.connect(str(duckdb_path))
    total_runtime = 0.0
    max_cardinality = 0
    total_output_cardinality = 0
    try:
        for uid in order:
            node = manifest["nodes"][uid]
            relation = node["relation_name"]
            compiled_sql = node["compiled_code"]
            materialized = node["config"]["materialized"]
            if materialized == "view":
                con.execute(f"create or replace view {relation} as {compiled_sql}")
            elif materialized == "table":
                start = time.perf_counter()
                (_, plan_json) = con.execute(
                    f"explain (analyze, format json) create or replace table {relation} as {compiled_sql}"
                ).fetchone()
                # duckdb's self-reported "latency" is unreliable in some builds/environments
                # (observed as always 0.0), so time the query ourselves instead.
                total_runtime += time.perf_counter() - start
                plan = json.loads(plan_json)
                max_cardinality = max(max_cardinality, max_operator_cardinality(plan))
                total_output_cardinality += output_cardinality(plan)
            else:
                raise ValueError(f"Unsupported materialization '{materialized}' for {uid}")
    finally:
        con.close()
    return total_runtime, max_cardinality, total_output_cardinality


def print_table(results: list[tuple[str, float, float, int, int]]) -> None:
    header = (
        f"{'project':<20} {'sf':>5} {'total_runtime_s':>16} "
        f"{'max_cardinality':>16} {'total_output_cardinality':>24}"
    )
    print(header)
    print("-" * len(header))
    last_project = None
    for project_name, sf, runtime, cardinality, output_card in results:
        if last_project is not None and project_name != last_project:
            print()
        print(
            f"{project_name:<20} {sf:>5.1f} {runtime:>16.4f} "
            f"{cardinality:>16,} {output_card:>24,}"
        )
        last_project = project_name


def plot_results(results: list[tuple[str, float, float, int, int]]) -> None:
    df = pd.DataFrame(
        results,
        columns=["project", "sf", "total_runtime_s", "max_cardinality", "total_output_cardinality"],
    )
    projects = sorted(df["project"].unique())

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(len(projects), 3, figsize=(16, 3.2 * len(projects)), squeeze=False)

    for row, project in enumerate(projects):
        project_df = df[df["project"] == project]

        ax_runtime = axes[row][0]
        sns.lineplot(data=project_df, x="sf", y="total_runtime_s", marker="o", ax=ax_runtime)
        ax_runtime.set_title(f"{project} — runtime")
        ax_runtime.set_xlabel("scale factor")
        ax_runtime.set_ylabel("total_runtime_s")

        ax_cardinality = axes[row][1]
        sns.lineplot(data=project_df, x="sf", y="max_cardinality", marker="o", color="darkorange", ax=ax_cardinality)
        ax_cardinality.set_title(f"{project} — max cardinality")
        ax_cardinality.set_xlabel("scale factor")
        ax_cardinality.set_ylabel("max_cardinality")

        ax_output = axes[row][2]
        sns.lineplot(
            data=project_df, x="sf", y="total_output_cardinality", marker="o", color="seagreen", ax=ax_output
        )
        ax_output.set_title(f"{project} — total output cardinality")
        ax_output.set_xlabel("scale factor")
        ax_output.set_ylabel("total_output_cardinality")

    fig.tight_layout()
    fig.savefig(PLOT_PATH, dpi=150)
    plt.close(fig)
    print(f"Saved plot to {PLOT_PATH}")


def main() -> None:
    projects = find_projects()
    results: list[tuple[str, float, float, int, int]] = []
    for project_dir in projects:
        manifest = json.loads((project_dir / "target" / "manifest.json").read_text())
        order = topological_model_order(manifest)
        for sf in SCALE_FACTORS:
            print(f"[{project_dir.name}] generating data at sf={sf} ...")
            generate_data(project_dir, sf)
            print(f"[{project_dir.name}] running {len(order)} model(s) at sf={sf} ...")
            runtime, cardinality, output_card = run_queries(project_dir, manifest, order)
            results.append((project_dir.name, sf, runtime, cardinality, output_card))
            print(
                f"[{project_dir.name}] sf={sf} -> runtime={runtime:.4f}s "
                f"max_cardinality={cardinality} total_output_cardinality={output_card}"
            )

    print()
    print_table(results)
    plot_results(results)


if __name__ == "__main__":
    main()
