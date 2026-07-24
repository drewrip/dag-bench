"""
Builds an interactive HTML dashboard for exploring the dbt projects under projects/.

For every project with a compiled manifest.json, this walks the DAG, pulls out
per-node SQL (raw + compiled), and renders a single self-contained report.html:
an overview of all projects plus a deep-dive per project (DAG viewer, SQL panel,
node table, lineage, and a few structural insights).
"""
import os
import json
import logging
import datetime
from collections import Counter, defaultdict

import duckdb
import networkx as nx
import yaml
from jinja2 import Template

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

ROOT_DIR = 'projects'
OUTPUT_FILE = 'report.html'
APP_NAME = 'dag-bench'

# Resource types we place on the DAG canvas, and the subset that counts as
# "real" pipeline work (excludes sources, which have no SQL of their own).
VISUAL_RESOURCES = {'model', 'seed', 'source', 'snapshot'}
METRIC_RESOURCES = {'model', 'seed', 'snapshot'}

# Categorical palette slots, assigned by resource type (fixed order, never cycled).
TYPE_COLORS = {
    'model': '#2a78d6',      # blue
    'source': '#eb6834',     # orange
    'seed': '#1baf7a',       # aqua
    'snapshot': '#4a3aa7',   # violet
    'test': '#e87ba4',       # magenta
    'analysis': '#eda100',   # yellow
}
DEFAULT_TYPE_COLOR = '#898781'
TABLE_COLOR = '#008300'  # green: overrides the type color for physically materialized tables


def is_physical_table(res_type, materialized):
    """True for nodes that build a real table in the warehouse (vs. a view/ephemeral CTE)."""
    if res_type in ('seed', 'snapshot'):
        return True
    return materialized in ('table', 'incremental')


def node_color(res_type, materialized):
    if is_physical_table(res_type, materialized):
        return TABLE_COLOR
    return TYPE_COLORS.get(res_type, DEFAULT_TYPE_COLOR)


def find_dbt_projects(root_dir):
    """Recursively find dbt projects (dbt_project.yml) with a compiled manifest.json."""
    projects = []
    for root, dirs, files in os.walk(root_dir):
        if 'dbt_project.yml' in files:
            manifest_path = os.path.join(root, 'target', 'manifest.json')
            if os.path.exists(manifest_path):
                projects.append({
                    'name': os.path.basename(root),
                    'path': root,
                    'manifest': manifest_path,
                })
            else:
                logging.warning(f"Project found at {root} but manifest.json is missing in target/")
    return projects


def disambiguate_names(projects):
    """If multiple projects share a name, prepend parent directory names until unique."""
    name_groups = defaultdict(list)
    for p in projects:
        name_groups[p['name']].append(p)

    needs_work = True
    while needs_work:
        needs_work = False
        new_groups = defaultdict(list)
        for name, group in name_groups.items():
            if len(group) > 1:
                for p in group:
                    path_parts = p['path'].strip(os.sep).split(os.sep)
                    current_name_parts = p['name'].split(' / ')
                    if len(path_parts) > len(current_name_parts):
                        parent_idx = -(len(current_name_parts) + 1)
                        p['name'] = f"{path_parts[parent_idx]} / {p['name']}"
                        needs_work = True
                    new_groups[p['name']].append(p)
            else:
                new_groups[name].append(group[0])
        name_groups = new_groups

    return [p for group in name_groups.values() for p in group]


def read_project_config(project_path):
    """Best-effort read of dbt_project.yml / packages.yml for descriptive metadata."""
    info = {'version': None, 'profile': None, 'packages': []}
    try:
        with open(os.path.join(project_path, 'dbt_project.yml')) as f:
            cfg = yaml.safe_load(f) or {}
        info['version'] = cfg.get('version')
        info['profile'] = cfg.get('profile')
    except Exception:
        pass
    try:
        with open(os.path.join(project_path, 'packages.yml')) as f:
            pkgs = yaml.safe_load(f) or {}
        for pkg in pkgs.get('packages', []):
            info['packages'].append(pkg.get('package') or pkg.get('git') or str(pkg))
    except Exception:
        pass
    return info


def get_duckdb_path(project_path):
    """Return the path to the project's duckdb warehouse file, per profiles.yml, if configured."""
    try:
        with open(os.path.join(project_path, 'profiles.yml')) as f:
            profiles = yaml.safe_load(f) or {}
    except Exception:
        return None
    for profile in profiles.values():
        if not isinstance(profile, dict):
            continue
        for output in profile.get('outputs', {}).values():
            if isinstance(output, dict) and output.get('type') == 'duckdb' and output.get('path'):
                return os.path.join(project_path, output['path'])
    return None


def get_existing_relations(con):
    """Set of (database, schema, name) for every table/view actually present in the warehouse."""
    rows = con.execute(
        "SELECT table_catalog, table_schema, table_name FROM information_schema.tables"
    ).fetchall()
    return {tuple(row) for row in rows}


def qualified_name(node_detail):
    database, schema, alias = node_detail['database'], node_detail['schema'], node_detail['alias']
    if not (database and schema and alias):
        return None
    return f'"{database}"."{schema}"."{alias}"'


def build_sample_ref(node_id, node_details, existing_relations, ctes, resolved, in_progress):
    """Return a SQL expression that yields node_id's rows, inlining any dependency that
    isn't actually materialized in the warehouse as a CTE built from its compiled SQL
    (recursively), so a node can be sampled even if it (or an ancestor) was never built.
    Appends (alias, sql) pairs to `ctes` in dependency order as needed. `resolved` caches
    node_id -> ref so a dependency shared by multiple branches is only inlined once.
    """
    if node_id in resolved:
        return resolved[node_id]

    detail = node_details[node_id]
    qname = qualified_name(detail)
    key = (detail['database'], detail['schema'], detail['alias'])
    if qname and key in existing_relations:
        resolved[node_id] = qname
        return qname

    if node_id in in_progress:
        return None  # guard against cycles; shouldn't happen in a DAG
    sql = detail.get('compiled_sql')
    if not sql:
        return None

    in_progress.add(node_id)
    try:
        for parent_id in detail['parents']:
            parent_detail = node_details.get(parent_id)
            if not parent_detail:
                continue
            parent_qname = qualified_name(parent_detail)
            parent_ref = build_sample_ref(parent_id, node_details, existing_relations, ctes, resolved, in_progress)
            if parent_ref and parent_qname and parent_ref != parent_qname:
                sql = sql.replace(parent_qname, parent_ref)
    finally:
        in_progress.discard(node_id)

    # Computed after recursing so the index reflects CTEs already added by dependencies.
    alias_name = f'sample_cte_{len(ctes)}'
    ctes.append((alias_name, sql))
    resolved[node_id] = alias_name
    return alias_name


def sample_rows_for_node(con, node_id, node_details, existing_relations):
    """Fetch up to 10 sample rows for a node from the project's duckdb warehouse.

    Queries the materialized relation directly when it exists; otherwise falls back to
    running the node's (and any un-materialized ancestors') compiled SQL directly, so
    every node can produce a sample as long as its lineage bottoms out at real tables.
    """
    ctes = []
    ref = build_sample_ref(node_id, node_details, existing_relations, ctes, {}, set())
    if ref is None:
        return None, None, 'No table location or compiled SQL known for this node.'

    if ctes:
        with_clause = 'WITH ' + ', '.join(f'{alias} AS ({sql})' for alias, sql in ctes) + ' '
    else:
        with_clause = ''
    query = f'{with_clause}SELECT * FROM {ref} LIMIT 10'

    try:
        result = con.execute(query)
        columns = [d[0] for d in result.description]
        rows = result.fetchall()
        return columns, rows, None
    except Exception as e:
        return None, None, str(e)


def build_graph(all_nodes):
    """Build a DiGraph over VISUAL_RESOURCES, edges pointing from dependency -> dependent."""
    G = nx.DiGraph()
    for node_id, node in all_nodes.items():
        if node.get('resource_type') in VISUAL_RESOURCES:
            G.add_node(node_id, name=node.get('name'), type=node.get('resource_type'))
    for node_id, node in all_nodes.items():
        if node.get('resource_type') in VISUAL_RESOURCES:
            for dep in node.get('depends_on', {}).get('nodes', []):
                if dep in G.nodes:
                    G.add_edge(dep, node_id)
    return G


def compute_levels(G):
    """Assign each node a horizontal layer = 1 + max(level of predecessors)."""
    try:
        levels = {}
        for node in nx.topological_sort(G):
            level = 0
            for pred in G.predecessors(node):
                level = max(level, levels[pred] + 1)
            levels[node] = level
        return levels
    except nx.NetworkXUnfeasible:
        return {n: 0 for n in G.nodes}


def sql_preview(sql, length=80):
    if not sql:
        return ''
    flat = ' '.join(sql.split())
    return flat if len(flat) <= length else flat[:length].rstrip() + '…'


def analyze_project(project):
    logging.info(f"Analyzing project: {project['name']}")
    try:
        with open(project['manifest']) as f:
            manifest = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load manifest for {project['name']}: {e}")
        return None

    nodes = manifest.get('nodes', {})
    sources = manifest.get('sources', {})
    all_nodes = {**nodes, **sources}

    G = build_graph(all_nodes)
    levels = compute_levels(G)
    G_metrics = G.subgraph([n for n, d in G.nodes(data=True) if d['type'] in METRIC_RESOURCES])

    num_nodes = G_metrics.number_of_nodes()
    num_edges = G_metrics.number_of_edges()

    if num_nodes > 0:
        avg_out_degree = sum(dict(G_metrics.out_degree()).values()) / num_nodes
        try:
            depth = nx.dag_longest_path_length(G_metrics)
            longest_path = nx.dag_longest_path(G_metrics)
        except nx.NetworkXUnfeasible:
            logging.warning(f"Project {project['name']} is not a DAG!")
            depth = -1
            longest_path = []
    else:
        avg_out_degree = 0
        depth = 0
        longest_path = []

    type_counts = dict(Counter(d['type'] for _, d in G.nodes(data=True)))
    materialized_counts = dict(Counter(
        all_nodes[n].get('config', {}).get('materialized', 'n/a')
        for n in G_metrics.nodes if all_nodes[n].get('resource_type') == 'model'
    ))

    source_nodes = [n for n, d in G.nodes(data=True) if d['type'] == 'source']
    table_count = 0
    for node_id, node in nodes.items():
        res_type = node.get('resource_type')
        if res_type == 'model':
            if node.get('config', {}).get('materialized') in ('table', 'incremental'):
                table_count += 1
        elif res_type in ('seed', 'snapshot'):
            table_count += 1

    # Structural insights
    degrees = dict(G_metrics.degree())
    most_connected = max(degrees, key=degrees.get) if degrees else None
    orphans = [n for n in G_metrics.nodes if G.degree(n) == 0]
    leaves = [n for n in G_metrics.nodes if G_metrics.out_degree(n) == 0]
    roots = [n for n in G_metrics.nodes if G.in_degree(n) == 0]

    duckdb_path = get_duckdb_path(project['path'])
    duckdb_con = None
    if duckdb_path and os.path.exists(duckdb_path):
        try:
            duckdb_con = duckdb.connect(duckdb_path, read_only=True)
        except Exception as e:
            logging.warning(f"Could not open duckdb warehouse for {project['name']}: {e}")

    # Per-node detail for the SQL/lineage panel
    node_details = {}
    viz_nodes = []
    viz_edges = []
    for n, d in G.nodes(data=True):
        node = all_nodes[n]
        res_type = d['type']
        raw_sql = node.get('raw_code')
        compiled_sql = node.get('compiled_code')
        materialized = node.get('config', {}).get('materialized') if res_type != 'source' else None

        node_details[n] = {
            'id': n,
            'name': node.get('name'),
            'type': res_type,
            'package': node.get('package_name'),
            'file_path': node.get('original_file_path'),
            'database': node.get('database'),
            'schema': node.get('schema'),
            'alias': node.get('alias') or node.get('identifier'),
            'materialized': materialized,
            'tags': node.get('tags', []),
            'description': node.get('description') or node.get('source_description') or '',
            'columns': list(node.get('columns', {}).keys()),
            'raw_sql': raw_sql,
            'compiled_sql': compiled_sql,
            'sql_preview': sql_preview(raw_sql or compiled_sql),
            'parents': sorted(G.predecessors(n)),
            'children': sorted(G.successors(n)),
        }

    existing_relations = get_existing_relations(duckdb_con) if duckdb_con is not None else None
    for n, d in G.nodes(data=True):
        if duckdb_con is not None:
            sample_columns, sample_rows, sample_error = sample_rows_for_node(
                duckdb_con, n, node_details, existing_relations
            )
        else:
            sample_columns, sample_rows, sample_error = None, None, 'No duckdb warehouse found for this project.'
        node_details[n]['sample_columns'] = sample_columns
        node_details[n]['sample_rows'] = sample_rows
        node_details[n]['sample_error'] = sample_error

        res_type = d['type']
        materialized = node_details[n]['materialized']
        color = node_color(res_type, materialized)
        title = f"{res_type} · {node_details[n]['name']}"
        if materialized:
            title += f" · {materialized}"
        viz_nodes.append({
            'id': n,
            'label': d['name'],
            'title': title,
            'color': {'background': color, 'border': color},
            'level': levels.get(n, 0),
            'shape': 'dot',
            'size': 9 if res_type == 'source' else 12,
        })

    for u, v in G.edges():
        viz_edges.append({'from': u, 'to': v})

    if duckdb_con is not None:
        duckdb_con.close()

    project_config = read_project_config(project['path'])

    return {
        'id': project['path'].replace('/', '_').replace('.', '_'),
        'name': project['name'],
        'path': project['path'],
        'version': project_config['version'],
        'packages': project_config['packages'],
        'num_nodes': num_nodes,
        'num_edges': num_edges,
        'avg_out_degree': round(avg_out_degree, 2),
        'depth': depth,
        'source_node_count': len(source_nodes),
        'table_count': table_count,
        'type_counts': type_counts,
        'materialized_counts': materialized_counts,
        'orphan_count': len(orphans),
        'leaf_count': len(leaves),
        'root_count': len(roots),
        'most_connected': node_details[most_connected]['name'] if most_connected else None,
        'longest_path': [node_details[n]['name'] for n in longest_path],
        'longest_path_ids': longest_path,
        'orphans': [node_details[n]['name'] for n in orphans],
        'leaves': [node_details[n]['name'] for n in leaves],
        'roots': [node_details[n]['name'] for n in roots],
        'viz_nodes': viz_nodes,
        'viz_edges': viz_edges,
        'nodes': node_details,
    }


def json_for_html(obj):
    """json.dumps that is safe to embed inside a <script> tag."""
    return (
        json.dumps(obj, default=str)
        .replace('</', '<\\/')
        .replace('<!--', '<\\!--')
    )


HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ app_name }}</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/atom-one-dark.min.css">
<script src="https://unpkg.com/sql-formatter@15/dist/sql-formatter.min.js"></script>
<style>
:root {
  --surface-1: #fcfcfb; --surface-2: #f9f9f7; --surface-3: #ffffff;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #898781;
  --border: rgba(11,11,11,0.10); --gridline: #e1e0d9;
  --accent: #2a78d6; --accent-dim: #cde2fb;
  --sidebar-bg: #f4f4f2; --sidebar-active: #eaf1fb;
  --chip-bg: #eeece6;
  --code-bg: #1e1e1e;
}
:root[data-theme="dark"] {
  --surface-1: #1a1a19; --surface-2: #0d0d0d; --surface-3: #232322;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
  --border: rgba(255,255,255,0.10); --gridline: #2c2c2a;
  --accent: #3987e5; --accent-dim: #184f95;
  --sidebar-bg: #141413; --sidebar-active: #1f2c3d;
  --chip-bg: #2c2c2a;
  --code-bg: #17171a;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --surface-1: #1a1a19; --surface-2: #0d0d0d; --surface-3: #232322;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
    --border: rgba(255,255,255,0.10); --gridline: #2c2c2a;
    --accent: #3987e5; --accent-dim: #184f95;
    --sidebar-bg: #141413; --sidebar-active: #1f2c3d;
    --chip-bg: #2c2c2a;
    --code-bg: #17171a;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; }
body {
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  background: var(--surface-2); color: var(--text-primary);
  display: flex; height: 100vh; overflow: hidden;
}
a { color: var(--accent); text-decoration: none; }
button { font-family: inherit; cursor: pointer; }
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 6px; }

/* ---------- Sidebar ---------- */
#sidebar {
  width: 260px; min-width: 260px; background: var(--sidebar-bg);
  border-right: 1px solid var(--border); display: flex; flex-direction: column;
  overflow-y: auto;
}
#sidebar .brand {
  padding: 18px 16px 10px; font-weight: 700; font-size: 1.05em;
  display: flex; align-items: center; justify-content: space-between;
}
#sidebar .brand small { display: block; font-weight: 400; color: var(--text-muted); font-size: 0.75em; margin-top: 2px; }
#theme-toggle {
  border: 1px solid var(--border); background: var(--surface-3); color: var(--text-secondary);
  border-radius: 6px; padding: 4px 8px; font-size: 0.85em;
}
#sidebar-search {
  margin: 8px 12px; padding: 7px 10px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--surface-3); color: var(--text-primary); font-size: 0.85em; width: calc(100% - 24px);
}
#nav-list { list-style: none; margin: 4px 0; padding: 0 8px; flex: 1; }
#nav-list li { margin-bottom: 2px; }
.nav-item {
  display: flex; align-items: center; justify-content: space-between; gap: 8px;
  padding: 8px 10px; border-radius: 6px; color: var(--text-secondary); font-size: 0.88em;
}
.nav-item:hover { background: var(--surface-3); }
.nav-item.active { background: var(--sidebar-active); color: var(--accent); font-weight: 600; }
.nav-item .n-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nav-item .n-count { font-size: 0.8em; color: var(--text-muted); flex-shrink: 0; }
.nav-item.active .n-count { color: var(--accent); }
#nav-overview { font-weight: 600; }
.nav-divider { border-top: 1px solid var(--border); margin: 8px 10px; }
#sidebar footer { padding: 10px 16px; font-size: 0.72em; color: var(--text-muted); }

/* ---------- Main ---------- */
#main { flex: 1; overflow-y: auto; padding: 24px 32px 48px; }
.panel { display: none; }
.panel.active { display: block; }
h1, h2, h3 { margin: 0 0 6px; }
.page-title { font-size: 1.5em; margin-bottom: 2px; }
.page-sub { color: var(--text-muted); font-size: 0.9em; margin-bottom: 22px; font-family: monospace; }

/* stat tiles */
.stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 26px; }
.stat-tile {
  background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
  padding: 14px 16px;
}
.stat-tile .v { font-size: 1.7em; font-weight: 600; display: block; line-height: 1.15; }
.stat-tile .l { font-size: 0.75em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em; margin-top: 3px; display: block; }

/* card */
.card {
  background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px;
  padding: 20px 22px; margin-bottom: 22px;
}
.card h2 { font-size: 1.05em; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.03em; }

/* overview bar chart */
#overview-chart .bar-row { display: flex; align-items: center; gap: 10px; margin: 7px 0; font-size: 0.85em; }
#overview-chart .bar-label { width: 190px; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-secondary); cursor: pointer; }
#overview-chart .bar-label:hover { color: var(--accent); }
#overview-chart .bar-track { flex: 1; background: var(--gridline); border-radius: 3px; height: 14px; position: relative; }
#overview-chart .bar-fill { background: var(--accent); height: 14px; border-radius: 3px 4px 4px 3px; }
#overview-chart .bar-value { width: 42px; text-align: right; color: var(--text-muted); font-size: 0.85em; flex-shrink: 0; font-variant-numeric: tabular-nums; }

/* summary table */
table.summary { width: 100%; border-collapse: collapse; font-size: 0.87em; }
table.summary th, table.summary td { padding: 9px 10px; text-align: left; border-bottom: 1px solid var(--border); }
table.summary th { color: var(--text-muted); font-weight: 600; text-transform: uppercase; font-size: 0.75em; letter-spacing: 0.03em; cursor: pointer; user-select: none; }
table.summary th:hover { color: var(--accent); }
table.summary tbody tr:hover { background: var(--surface-3); }
table.summary td.num, table.summary th.num { font-variant-numeric: tabular-nums; text-align: right; }
table.summary tr.clickable { cursor: pointer; }

/* chips */
.chip { display: inline-flex; align-items: center; gap: 5px; background: var(--chip-bg); color: var(--text-secondary);
  padding: 3px 10px; border-radius: 999px; font-size: 0.78em; margin: 0 6px 6px 0; }
.chip .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.chip.link { cursor: pointer; }
.chip.link:hover { background: var(--accent-dim); color: var(--accent); }

/* project toolbar */
.toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 14px; }
.toolbar input[type=text] {
  padding: 7px 10px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--surface-3); color: var(--text-primary); font-size: 0.85em; min-width: 200px;
}
.toolbar select {
  padding: 6px 8px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--surface-3); color: var(--text-primary); font-size: 0.85em;
}
.toolbar .type-filter { display: flex; gap: 6px; }
.type-filter .chip { cursor: pointer; border: 1px solid transparent; }
.type-filter .chip.off { opacity: 0.35; }
.toolbar button.ghost {
  border: 1px solid var(--border); background: var(--surface-3); color: var(--text-secondary);
  border-radius: 6px; padding: 6px 12px; font-size: 0.82em;
}
.toolbar button.ghost:hover { color: var(--accent); }

/* shape legend */
.graph-legend { display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin-bottom: 12px; font-size: 0.8em; color: var(--text-secondary); }
.legend-item { display: flex; align-items: center; gap: 6px; }
.legend-swatch { width: 12px; height: 12px; border-radius: 50%; background: var(--text-muted); display: inline-block; flex-shrink: 0; }

/* split view: graph + detail */
.split { display: flex; gap: 16px; align-items: stretch; }
.graph-wrap { flex: 1 1 52%; min-width: 0; background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }
.viz { height: 680px; width: 100%; }
.detail-wrap { flex: 0 0 46%; min-width: 360px; max-width: 640px; background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 12px; padding: 16px 18px; height: 680px; overflow-y: auto; }
.detail-empty { color: var(--text-muted); font-size: 0.88em; text-align: center; margin-top: 60px; }
.detail-name { font-size: 1.1em; font-weight: 700; margin-bottom: 2px; word-break: break-word; }
.detail-meta { color: var(--text-muted); font-size: 0.78em; font-family: monospace; margin-bottom: 12px; }
.detail-tabs { display: flex; gap: 4px; margin-bottom: 10px; border-bottom: 1px solid var(--border); }
.detail-tab { padding: 6px 10px; font-size: 0.8em; color: var(--text-muted); border-bottom: 2px solid transparent; }
.detail-tab.active { color: var(--accent); border-color: var(--accent); font-weight: 600; }
.detail-section { display: none; }
.detail-section.active { display: block; }
pre.sql-block { background: var(--code-bg); border-radius: 8px; padding: 14px 16px; font-size: 0.86em; line-height: 1.55; overflow: auto; max-height: 560px; }
pre.sql-block code { white-space: pre; }
.sql-toggle { display: flex; gap: 6px; margin-bottom: 8px; }
.sql-toggle button {
  border: 1px solid var(--border); background: var(--surface-3); color: var(--text-secondary);
  border-radius: 6px; padding: 4px 10px; font-size: 0.78em;
}
.sql-toggle button.active { background: var(--accent-dim); color: var(--accent); border-color: var(--accent); }
.kv { font-size: 0.85em; margin-bottom: 6px; }
.kv b { color: var(--text-secondary); font-weight: 600; }
.lineage-col h4 { font-size: 0.78em; text-transform: uppercase; color: var(--text-muted); margin: 12px 0 6px; }
.node-empty-note { color: var(--text-muted); font-size: 0.82em; }
.sample-wrap { overflow-x: auto; }
table.sample { border-collapse: collapse; font-size: 0.8em; white-space: nowrap; }
table.sample th, table.sample td { padding: 6px 10px; border-bottom: 1px solid var(--border); text-align: left; }
table.sample th { color: var(--text-muted); font-size: 0.72em; text-transform: uppercase; letter-spacing: 0.03em; background: var(--surface-2); position: sticky; top: 0; }
table.sample td { max-width: 260px; overflow: hidden; text-overflow: ellipsis; font-family: monospace; }

/* node table */
table.nodes { width: 100%; border-collapse: collapse; font-size: 0.83em; margin-top: 10px; }
table.nodes th, table.nodes td { padding: 7px 9px; border-bottom: 1px solid var(--border); text-align: left; }
table.nodes th { color: var(--text-muted); font-size: 0.72em; text-transform: uppercase; letter-spacing: 0.03em; }
table.nodes tr.node-row { cursor: pointer; }
table.nodes tr.node-row:hover { background: var(--surface-3); }
table.nodes tr.node-row.selected { background: var(--accent-dim); }
table.nodes td.sql-cell { color: var(--text-muted); font-family: monospace; font-size: 0.92em; max-width: 340px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* insights */
.insight-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }
.insight-block h4 { font-size: 0.78em; text-transform: uppercase; color: var(--text-muted); margin: 0 0 8px; }
.path-chain { display: flex; flex-wrap: wrap; align-items: center; gap: 4px; font-size: 0.82em; }
.path-chain .seg { color: var(--text-secondary); }
.path-chain .arrow { color: var(--text-muted); }

</style>
</head>
<body>

<nav id="sidebar">
  <div class="brand">
    <div>{{ app_name }}<small id="proj-total"></small></div>
    <button id="theme-toggle" title="Toggle theme">◐</button>
  </div>
  <input id="sidebar-search" type="text" placeholder="Filter projects…">
  <ul id="nav-list">
    <li><div class="nav-item" id="nav-overview" data-target="overview">Overview</div></li>
    <li><div class="nav-divider"></div></li>
  </ul>
  <footer>Generated {{ date }}</footer>
</nav>

<main id="main">
  <section class="panel active" id="panel-overview"></section>
</main>

<script id="report-data" type="application/json">{{ data_json }}</script>

<script>
const DATA = JSON.parse(document.getElementById('report-data').textContent);
const TYPE_COLORS = DATA.type_colors;
const projects = DATA.projects; // {id: project}
const projectOrder = DATA.project_order;

// ---------------- theme ----------------
const activeNetworks = {};

function currentTextColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--text-primary').trim() || '#333';
}

function refreshGraphTheme() {
  const color = currentTextColor();
  Object.values(activeNetworks).forEach(network => {
    network.setOptions({ nodes: { font: { color } } });
  });
}

(function initTheme() {
  const saved = localStorage.getItem('dbt-explorer-theme');
  if (saved) document.documentElement.setAttribute('data-theme', saved);
  document.getElementById('theme-toggle').addEventListener('click', () => {
    const cur = document.documentElement.getAttribute('data-theme') ||
      (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const next = cur === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('dbt-explorer-theme', next);
    refreshGraphTheme();
  });
})();

// ---------------- sidebar nav ----------------
const navList = document.getElementById('nav-list');
document.getElementById('proj-total').textContent = projectOrder.length + ' project' + (projectOrder.length === 1 ? '' : 's');

projectOrder.forEach(id => {
  const p = projects[id];
  const li = document.createElement('li');
  const item = document.createElement('div');
  item.className = 'nav-item';
  item.dataset.target = 'project-' + id;
  item.innerHTML = `<span class="n-name">${p.name}</span><span class="n-count">${p.num_nodes}</span>`;
  li.appendChild(item);
  navList.appendChild(li);
});

function setActiveNav(target) {
  document.querySelectorAll('.nav-item').forEach(el => el.classList.toggle('active', el.dataset.target === target));
}

document.getElementById('sidebar-search').addEventListener('input', (e) => {
  const q = e.target.value.toLowerCase();
  document.querySelectorAll('#nav-list .nav-item[data-target^="project-"]').forEach(el => {
    el.parentElement.style.display = el.querySelector('.n-name').textContent.toLowerCase().includes(q) ? '' : 'none';
  });
});

navList.addEventListener('click', (e) => {
  const item = e.target.closest('.nav-item');
  if (!item) return;
  navigateTo(item.dataset.target);
});

const builtPanels = new Set();

function navigateTo(target) {
  setActiveNav(target);
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  let panel = document.getElementById('panel-' + target);
  if (!panel) {
    panel = document.createElement('section');
    panel.className = 'panel';
    panel.id = 'panel-' + target;
    document.getElementById('main').appendChild(panel);
  }
  panel.classList.add('active');
  if (!builtPanels.has(target)) {
    builtPanels.add(target);
    if (target === 'overview') buildOverview(panel);
    else buildProjectPanel(panel, target.replace('project-', ''));
  }
  window.scrollTo(0, 0);
}

// ---------------- helpers ----------------
function esc(s) {
  return (s ?? '').toString().replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function typeDot(type) {
  return `<span class="dot" style="background:${TYPE_COLORS[type] || '#898781'}"></span>`;
}
function typeChip(type, count) {
  return `<span class="chip">${typeDot(type)} ${type}: ${count}</span>`;
}
function formatSql(sql) {
  if (!sql) return sql;
  if (window.sqlFormatter && typeof sqlFormatter.format === 'function') {
    try { return sqlFormatter.format(sql, { language: 'sql', keywordCase: 'upper' }); } catch (e) { /* fall through */ }
  }
  return sql;
}
function highlightSqlBlock(codeEl) {
  if (!codeEl || !window.hljs) return;
  delete codeEl.dataset.highlighted;
  hljs.highlightElement(codeEl);
}
function buildSampleSection(n) {
  if (n.sample_error) return `<p class="node-empty-note">${esc(n.sample_error)}</p>`;
  if (!n.sample_columns) return `<p class="node-empty-note">No sample data available.</p>`;
  if (!n.sample_rows.length) return `<p class="node-empty-note">Table has no rows.</p>`;
  const head = n.sample_columns.map(c => `<th>${esc(c)}</th>`).join('');
  const rows = n.sample_rows.map(row =>
    `<tr>${row.map(v => `<td>${v === null ? '<span class="node-empty-note">null</span>' : esc(v)}</td>`).join('')}</tr>`
  ).join('');
  return `<div class="sample-wrap"><table class="sample"><thead><tr>${head}</tr></thead><tbody>${rows}</tbody></table></div>`;
}

// ---------------- overview panel ----------------
function buildOverview(panel) {
  const totals = { nodes: 0, models: 0, sources: 0, seeds: 0, snapshots: 0, edges: 0 };
  let depthSum = 0, depthCount = 0;
  projectOrder.forEach(id => {
    const p = projects[id];
    totals.nodes += p.num_nodes;
    totals.edges += p.num_edges;
    totals.models += p.type_counts.model || 0;
    totals.sources += p.source_node_count;
    totals.seeds += p.type_counts.seed || 0;
    totals.snapshots += p.type_counts.snapshot || 0;
    if (p.depth >= 0) { depthSum += p.depth; depthCount++; }
  });
  const avgDepth = depthCount ? (depthSum / depthCount).toFixed(1) : '0';

  const maxNodes = Math.max(...projectOrder.map(id => projects[id].num_nodes), 1);
  const sortedByNodes = [...projectOrder].sort((a, b) => projects[b].num_nodes - projects[a].num_nodes);
  const chartRows = sortedByNodes.map(id => {
    const p = projects[id];
    const pct = Math.round((p.num_nodes / maxNodes) * 100);
    return `<div class="bar-row">
      <div class="bar-label" data-target="project-${id}" title="${esc(p.name)}">${esc(p.name)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
      <div class="bar-value">${p.num_nodes}</div>
    </div>`;
  }).join('');

  const rows = projectOrder.map(id => {
    const p = projects[id];
    return `<tr class="clickable" data-target="project-${id}">
      <td><strong>${esc(p.name)}</strong></td>
      <td class="num">${p.num_nodes}</td>
      <td class="num">${p.num_edges}</td>
      <td class="num">${p.depth}</td>
      <td class="num">${p.avg_out_degree}</td>
      <td class="num">${p.source_node_count}</td>
      <td class="num">${p.table_count}</td>
    </tr>`;
  }).join('');

  panel.innerHTML = `
    <h1 class="page-title">${esc(DATA.app_name)}</h1>
    <p class="page-sub">${projectOrder.length} projects discovered under ${esc(DATA.root_dir)}/</p>

    <div class="stat-grid">
      <div class="stat-tile"><span class="v">${projectOrder.length}</span><span class="l">Projects</span></div>
      <div class="stat-tile"><span class="v">${totals.nodes}</span><span class="l">Total nodes</span></div>
      <div class="stat-tile"><span class="v">${totals.models}</span><span class="l">Models</span></div>
      <div class="stat-tile"><span class="v">${totals.sources}</span><span class="l">Sources</span></div>
      <div class="stat-tile"><span class="v">${totals.edges}</span><span class="l">Edges</span></div>
      <div class="stat-tile"><span class="v">${avgDepth}</span><span class="l">Avg DAG depth</span></div>
    </div>

    <div class="card">
      <h2>Nodes per project</h2>
      <div id="overview-chart">${chartRows}</div>
    </div>

    <div class="card">
      <h2>All projects</h2>
      <table class="summary" id="summary-table">
        <thead><tr>
          <th data-key="name">Project</th>
          <th class="num" data-key="num_nodes">Nodes</th>
          <th class="num" data-key="num_edges">Edges</th>
          <th class="num" data-key="depth">Depth</th>
          <th class="num" data-key="avg_out_degree">Avg out-deg</th>
          <th class="num" data-key="source_node_count">Sources</th>
          <th class="num" data-key="table_count">Tables</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;

  panel.querySelectorAll('[data-target^="project-"]').forEach(el => {
    el.addEventListener('click', () => navigateTo(el.dataset.target));
  });

  let sortState = { key: null, asc: true };
  panel.querySelectorAll('#summary-table th[data-key]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.key;
      sortState.asc = sortState.key === key ? !sortState.asc : true;
      sortState.key = key;
      const sorted = [...projectOrder].sort((a, b) => {
        const pa = projects[a], pb = projects[b];
        let va = key === 'name' ? pa.name : pa[key];
        let vb = key === 'name' ? pb.name : pb[key];
        if (typeof va === 'string') return sortState.asc ? va.localeCompare(vb) : vb.localeCompare(va);
        return sortState.asc ? va - vb : vb - va;
      });
      const tbody = panel.querySelector('#summary-table tbody');
      tbody.innerHTML = sorted.map(id => {
        const p = projects[id];
        return `<tr class="clickable" data-target="project-${id}">
          <td><strong>${esc(p.name)}</strong></td>
          <td class="num">${p.num_nodes}</td>
          <td class="num">${p.num_edges}</td>
          <td class="num">${p.depth}</td>
          <td class="num">${p.avg_out_degree}</td>
          <td class="num">${p.source_node_count}</td>
          <td class="num">${p.table_count}</td>
        </tr>`;
      }).join('');
      tbody.querySelectorAll('[data-target]').forEach(el => el.addEventListener('click', () => navigateTo(el.dataset.target)));
    });
  });
}

// ---------------- project panel ----------------
function buildProjectPanel(panel, id) {
  const p = projects[id];
  const typeChips = Object.entries(p.type_counts).map(([t, c]) => typeChip(t, c)).join('');
  const matChips = Object.entries(p.materialized_counts).map(([m, c]) => `<span class="chip">${esc(m)}: ${c}</span>`).join('');
  const pkgChips = (p.packages || []).map(pkg => `<span class="chip">${esc(pkg)}</span>`).join('') || '<span class="node-empty-note">none declared</span>';

  const typeFilterChips = Object.keys(p.type_counts).map(t =>
    `<span class="chip type-filter-chip" data-type="${t}">${typeDot(t)} ${t}</span>`
  ).join('');

  const pathChain = p.longest_path.length
    ? p.longest_path.map(n => `<span class="seg">${esc(n)}</span>`).join('<span class="arrow"> → </span>')
    : '<span class="node-empty-note">no chain (no models)</span>';

  panel.innerHTML = `
    <h1 class="page-title">${esc(p.name)}</h1>
    <p class="page-sub">${esc(p.path)}${p.version ? ' · v' + esc(p.version) : ''}</p>

    <div class="stat-grid">
      <div class="stat-tile"><span class="v">${p.num_nodes}</span><span class="l">Nodes</span></div>
      <div class="stat-tile"><span class="v">${p.num_edges}</span><span class="l">Edges</span></div>
      <div class="stat-tile"><span class="v">${p.depth}</span><span class="l">Max depth</span></div>
      <div class="stat-tile"><span class="v">${p.avg_out_degree}</span><span class="l">Avg out-deg</span></div>
      <div class="stat-tile"><span class="v">${p.source_node_count}</span><span class="l">Sources</span></div>
      <div class="stat-tile"><span class="v">${p.table_count}</span><span class="l">Tables</span></div>
    </div>

    <div class="card">
      <h2>Resource types</h2>
      ${typeChips}
      <h2 style="margin-top:16px;">Materializations</h2>
      ${matChips}
      <h2 style="margin-top:16px;">Packages</h2>
      ${pkgChips}
    </div>

    <div class="card">
      <h2>DAG explorer</h2>
      <div class="toolbar">
        <input type="text" id="node-search-${id}" placeholder="Search nodes…">
        <div class="type-filter" id="type-filter-${id}">${typeFilterChips}</div>
        <select id="layout-select-${id}">
          <option value="LR">Layout: left → right</option>
          <option value="UD">Layout: top → down</option>
          <option value="physics">Layout: force-directed</option>
        </select>
        <button class="ghost" id="fit-btn-${id}">Fit view</button>
      </div>
      <div class="graph-legend">
        <span class="legend-item"><span class="legend-swatch" style="background:${DATA.table_color}"></span> Table / incremental (physical)</span>
        <span class="legend-item"><span class="legend-swatch" style="background:${TYPE_COLORS.model}"></span> View / ephemeral model</span>
        <span class="legend-item"><span class="legend-swatch" style="background:${TYPE_COLORS.source}"></span> Source</span>
      </div>
      <div class="split">
        <div class="graph-wrap"><div class="viz" id="viz-${id}"></div></div>
        <div class="detail-wrap" id="detail-${id}"><div class="detail-empty">Click a node to inspect its SQL, metadata, and lineage.</div></div>
      </div>
    </div>

    <div class="card">
      <h2>Structural insights</h2>
      <div class="insight-grid">
        <div class="insight-block">
          <h4>Longest dependency chain (${p.depth} hops)</h4>
          <div class="path-chain">${pathChain}</div>
        </div>
        <div class="insight-block">
          <h4>Most-connected node</h4>
          <p>${p.most_connected ? esc(p.most_connected) : '—'}</p>
        </div>
        <div class="insight-block">
          <h4>Terminal outputs (${p.leaves.length})</h4>
          <p>${p.leaves.map(esc).join(', ') || '—'}</p>
        </div>
        <div class="insight-block">
          <h4>Entry-point models (${p.roots.length})</h4>
          <p>${p.roots.map(esc).join(', ') || '—'}</p>
        </div>
        ${p.orphans.length ? `<div class="insight-block"><h4>Orphan nodes (${p.orphans.length})</h4><p>${p.orphans.map(esc).join(', ')}</p></div>` : ''}
      </div>
    </div>

    <div class="card">
      <h2>All nodes</h2>
      <table class="nodes" id="node-table-${id}">
        <thead><tr><th>Name</th><th>Type</th><th>Materialized</th><th>SQL preview</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  `;

  initProjectGraph(id, p);
}

function initProjectGraph(id, p) {
  const nodesDS = new vis.DataSet(p.viz_nodes);
  const edgesDS = new vis.DataSet(p.viz_edges);
  const container = document.getElementById('viz-' + id);
  const network = new vis.Network(container, { nodes: nodesDS, edges: edgesDS }, {
    layout: { hierarchical: { direction: 'LR', sortMethod: 'directed', levelSeparation: 150, nodeSpacing: 110 } },
    edges: { arrows: { to: { enabled: true, scaleFactor: 0.6 } }, color: '#a3a29b', width: 0.6, smooth: { type: 'cubicBezier', roundness: 0.5 } },
    nodes: { shape: 'dot', font: { size: 12, color: currentTextColor() } },
    interaction: { hover: true },
    physics: false,
  });
  activeNetworks[id] = network;

  network.on('click', (params) => {
    if (params.nodes.length) {
      showDetail(id, p, params.nodes[0]);
      highlightRow(id, params.nodes[0]);
    }
  });

  document.getElementById('fit-btn-' + id).addEventListener('click', () => network.fit());

  document.getElementById('layout-select-' + id).addEventListener('change', (e) => {
    const mode = e.target.value;
    if (mode === 'physics') {
      network.setOptions({ layout: { hierarchical: false }, physics: { enabled: true, solver: 'forceAtlas2Based' } });
    } else {
      network.setOptions({ layout: { hierarchical: { direction: mode, sortMethod: 'directed', levelSeparation: 150, nodeSpacing: 110 } }, physics: false });
    }
  });

  const search = document.getElementById('node-search-' + id);
  const activeTypes = new Set(Object.keys(p.type_counts));

  function applyFilters() {
    const q = search.value.toLowerCase();
    const updates = p.viz_nodes.map(n => {
      const detail = p.nodes[n.id];
      const match = detail.name.toLowerCase().includes(q) && activeTypes.has(detail.type);
      return { id: n.id, hidden: !match };
    });
    nodesDS.update(updates);
    renderNodeTable(id, p, q, activeTypes);
  }

  search.addEventListener('input', applyFilters);

  document.querySelectorAll(`#type-filter-${id} .type-filter-chip`).forEach(chip => {
    chip.addEventListener('click', () => {
      const t = chip.dataset.type;
      if (activeTypes.has(t)) { activeTypes.delete(t); chip.classList.add('off'); }
      else { activeTypes.add(t); chip.classList.remove('off'); }
      applyFilters();
    });
  });

  renderNodeTable(id, p, '', activeTypes);
}

function renderNodeTable(id, p, query, activeTypes) {
  const tbody = document.querySelector(`#node-table-${id} tbody`);
  const rows = Object.values(p.nodes)
    .filter(n => n.name.toLowerCase().includes(query || '') && activeTypes.has(n.type))
    .sort((a, b) => a.name.localeCompare(b.name))
    .map(n => `<tr class="node-row" data-id="${esc(n.id)}">
      <td>${typeDot(n.type)} ${esc(n.name)}</td>
      <td>${esc(n.type)}</td>
      <td>${esc(n.materialized || '—')}</td>
      <td class="sql-cell">${esc(n.sql_preview || '—')}</td>
    </tr>`).join('');
  tbody.innerHTML = rows || `<tr><td colspan="4" class="node-empty-note">No nodes match the current filters.</td></tr>`;
  tbody.querySelectorAll('.node-row').forEach(row => {
    row.addEventListener('click', () => {
      showDetail(id, p, row.dataset.id);
      highlightRow(id, row.dataset.id);
    });
  });
}

function highlightRow(id, nodeId) {
  document.querySelectorAll(`#node-table-${id} .node-row`).forEach(r => r.classList.toggle('selected', r.dataset.id === nodeId));
}

function showDetail(id, p, nodeId) {
  const n = p.nodes[nodeId];
  const wrap = document.getElementById('detail-' + id);
  if (!n) { wrap.innerHTML = '<div class="detail-empty">Node not found.</div>'; return; }

  const hasSql = n.raw_sql || n.compiled_sql;
  const initialMode = n.raw_sql ? 'raw' : 'compiled';
  const initialSql = initialMode === 'raw' ? n.raw_sql : formatSql(n.compiled_sql);
  const sqlSection = hasSql ? `
    <div class="sql-toggle">
      <button data-mode="raw" class="${initialMode === 'raw' ? 'active' : ''}" ${n.raw_sql ? '' : 'disabled'}>Raw</button>
      <button data-mode="compiled" class="${initialMode === 'compiled' ? 'active' : ''}" ${n.compiled_sql ? '' : 'disabled'}>Compiled</button>
    </div>
    <pre class="sql-block"><code class="language-sql" id="sql-code-${id}">${esc(initialSql)}</code></pre>
  ` : `<p class="node-empty-note">${n.type === 'source' ? 'Sources have no SQL — see Info tab for connection details.' : 'No SQL available.'}</p>`;

  const parentsHtml = n.parents.length
    ? n.parents.map(pid => `<span class="chip link" data-id="${esc(pid)}">${typeDot(p.nodes[pid].type)} ${esc(p.nodes[pid].name)}</span>`).join('')
    : '<span class="node-empty-note">none</span>';
  const childrenHtml = n.children.length
    ? n.children.map(cid => `<span class="chip link" data-id="${esc(cid)}">${typeDot(p.nodes[cid].type)} ${esc(p.nodes[cid].name)}</span>`).join('')
    : '<span class="node-empty-note">none</span>';

  wrap.innerHTML = `
    <div class="detail-name">${typeDot(n.type)} ${esc(n.name)}</div>
    <div class="detail-meta">${esc(n.type)}${n.materialized ? ' · ' + esc(n.materialized) : ''}${n.file_path ? ' · ' + esc(n.file_path) : ''}</div>
    <div class="detail-tabs">
      <div class="detail-tab active" data-tab="sql">SQL</div>
      <div class="detail-tab" data-tab="info">Info</div>
      <div class="detail-tab" data-tab="lineage">Lineage</div>
      <div class="detail-tab" data-tab="sample">Sample</div>
    </div>
    <div class="detail-section active" data-tab="sql">${sqlSection}</div>
    <div class="detail-section" data-tab="info">
      <div class="kv"><b>Database:</b> ${esc(n.database || '—')}</div>
      <div class="kv"><b>Schema:</b> ${esc(n.schema || '—')}</div>
      <div class="kv"><b>Alias:</b> ${esc(n.alias || '—')}</div>
      <div class="kv"><b>Package:</b> ${esc(n.package || '—')}</div>
      <div class="kv"><b>Tags:</b> ${n.tags.length ? n.tags.map(esc).join(', ') : '—'}</div>
      <div class="kv"><b>Description:</b> ${esc(n.description) || '—'}</div>
      <div class="kv"><b>Columns:</b> ${n.columns.length ? n.columns.map(esc).join(', ') : '—'}</div>
    </div>
    <div class="detail-section" data-tab="lineage">
      <div class="lineage-col">
        <h4>Depends on (${n.parents.length})</h4>${parentsHtml}
        <h4>Used by (${n.children.length})</h4>${childrenHtml}
      </div>
    </div>
    <div class="detail-section" data-tab="sample">${buildSampleSection(n)}</div>
  `;

  if (hasSql) highlightSqlBlock(document.getElementById('sql-code-' + id));

  wrap.querySelectorAll('.detail-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      wrap.querySelectorAll('.detail-tab').forEach(t => t.classList.toggle('active', t === tab));
      wrap.querySelectorAll('.detail-section').forEach(s => s.classList.toggle('active', s.dataset.tab === tab.dataset.tab));
    });
  });

  wrap.querySelectorAll('.sql-toggle button').forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.disabled) return;
      wrap.querySelectorAll('.sql-toggle button').forEach(b => b.classList.toggle('active', b === btn));
      const code = document.getElementById('sql-code-' + id);
      code.textContent = btn.dataset.mode === 'raw' ? n.raw_sql : formatSql(n.compiled_sql);
      highlightSqlBlock(code);
    });
  });

  wrap.querySelectorAll('.chip.link').forEach(chip => {
    chip.addEventListener('click', () => { showDetail(id, p, chip.dataset.id); highlightRow(id, chip.dataset.id); });
  });
}

// boot on the overview panel
builtPanels.add('overview');
buildOverview(document.getElementById('panel-overview'));
</script>
</body>
</html>
"""


def main():
    project_configs = find_dbt_projects(ROOT_DIR)
    if not project_configs:
        print(f"No dbt projects with manifest.json found in {ROOT_DIR}/ directory.")
        return

    project_configs = disambiguate_names(project_configs)

    summaries = []
    for p in project_configs:
        res = analyze_project(p)
        if res:
            summaries.append(res)
    summaries.sort(key=lambda x: x['name'])

    project_order = [p['id'] for p in summaries]
    projects_by_id = {p['id']: p for p in summaries}

    data = {
        'app_name': APP_NAME,
        'root_dir': ROOT_DIR,
        'type_colors': TYPE_COLORS,
        'table_color': TABLE_COLOR,
        'project_order': project_order,
        'projects': projects_by_id,
    }

    template = Template(HTML_TEMPLATE)
    html_output = template.render(
        app_name=APP_NAME,
        data_json=json_for_html(data),
        date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    with open(OUTPUT_FILE, 'w') as f:
        f.write(html_output)

    print(f"\nSuccess! Report generated for {len(summaries)} projects.")
    print(f"Output: {os.path.abspath(OUTPUT_FILE)}")


if __name__ == "__main__":
    main()
