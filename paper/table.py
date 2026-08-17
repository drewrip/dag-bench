"""
Generates a LaTeX table summarizing the 10 benchmark dbt projects.

Reads the compiled manifest.json for each projects/pNN_* project (run
`dbt parse` inside a project directory first if its target/manifest.json is
missing or stale) and prints a longtable to stdout.

Usage:
    python3 paper/table.py                          # to stdout
    python3 paper/table.py -o paper/figures/table.tex
"""
import argparse
import json
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.join(REPO_ROOT, 'projects')
OUT_DIR = os.path.join(REPO_ROOT, 'paper', 'figures')

# Project directory (== exact name from SPEC.md) -> short domain description, in table order.
PROJECTS = [
    ('p01_iot', 'IoT fleet monitoring'),
    ('p02_adtech', 'Digital advertising funnel'),
    ('p03_ecommerce', 'Online retail'),
    ('p04_fraud', 'Card/banking fraud detection'),
    ('p05_hr', 'People analytics'),
    ('p06_logistics', 'Supply chain / warehouse logistics'),
    ('p07_saas', 'B2B SaaS product analytics'),
    ('p08_healthcare', 'Health insurance claims'),
    ('p09_gaming', 'Mobile/video game analytics'),
    ('p10_energy', 'Electric utility / smart grid'),
]

# Node types that count as real pipeline stages when computing DAG depth.
DEPTH_RESOURCES = {'model', 'seed', 'snapshot'}


def load_manifest(project_dir):
    path = os.path.join(ROOT_DIR, project_dir, 'target', 'manifest.json')
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} not found; run `dbt parse` inside {ROOT_DIR}/{project_dir} first"
        )
    with open(path) as f:
        return json.load(f)


def max_dag_depth(manifest):
    """Longest chain of model/seed/snapshot nodes, ignoring sources/tests."""
    nodes = manifest['nodes']
    parent_map = manifest['parent_map']
    depth = {}

    def get_depth(node_id):
        if node_id in depth:
            return depth[node_id]
        parents = [
            p for p in parent_map.get(node_id, [])
            if p in nodes and nodes[p]['resource_type'] in DEPTH_RESOURCES
        ]
        depth[node_id] = 1 if not parents else 1 + max(get_depth(p) for p in parents)
        return depth[node_id]

    stage_ids = [nid for nid, n in nodes.items() if n['resource_type'] in DEPTH_RESOURCES]
    if not stage_ids:
        return 0
    return max(get_depth(nid) for nid in stage_ids)


def count_branching_nodes(manifest):
    """Number of model/seed/snapshot nodes with more than one downstream node."""
    nodes = manifest['nodes']
    child_map = manifest['child_map']
    branching = 0
    for node_id, node in nodes.items():
        if node['resource_type'] not in DEPTH_RESOURCES:
            continue
        children = [
            c for c in child_map.get(node_id, [])
            if c in nodes and nodes[c]['resource_type'] in DEPTH_RESOURCES
        ]
        if len(children) > 1:
            branching += 1
    return branching


def project_stats(project_dir):
    manifest = load_manifest(project_dir)
    nodes = manifest['nodes']
    models = {k: v for k, v in nodes.items() if v['resource_type'] == 'model'}
    tables = sum(
        1 for v in models.values()
        if v.get('config', {}).get('materialized') in ('table', 'incremental')
    )
    return {
        'sources': len(manifest['sources']),
        'models': len(models),
        'tables': tables,
        'views': len(models) - tables,
        'depth': max_dag_depth(manifest),
        'branching': count_branching_nodes(manifest),
    }


def escape(text):
    return text.replace('&', r'\&').replace('_', r'\_')


def build_latex(rows):
    header = (
        "\\begin{table}[t]\n"
        "  \\centering\n"
        "  \\caption{Summary of the 10 benchmark projects.}\n"
        "  \\label{tab:projects}\n"
        "  \\begin{tabular}{llrrrrrr}\n"
        "    \\toprule\n"
        "    Project & Domain & Sources & Nodes & Views & Tables & Depth & Branching Nodes \\\\\n"
        "    \\midrule\n"
    )
    body = ""
    for (project_dir, domain), stats in rows:
        body += (
            f"    \\texttt{{{escape(project_dir)}}} & {escape(domain)} & "
            f"{stats['sources']} & {stats['models']} & {stats['views']} & "
            f"{stats['tables']} & {stats['depth']} & {stats['branching']} \\\\\n"
        )
    footer = (
        "    \\bottomrule\n"
        "  \\end{tabular}\n"
        "\\end{table}\n"
    )
    return header + body + footer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '-o', '--output', default=None,
        help='Write LaTeX to this file instead of stdout (e.g. paper/figures/table.tex)',
    )
    args = parser.parse_args()

    rows = [(p, project_stats(p[0])) for p in PROJECTS]
    latex = build_latex(rows)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, 'w') as f:
            f.write(latex)
    else:
        print(latex)


if __name__ == '__main__':
    main()
