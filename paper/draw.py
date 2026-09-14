"""
Draws publication-quality SVG figures of the dbt DAG for one or more
projects in projects/.

Layout is a from-scratch Sugiyama-style layered layout, in the spirit of
the algorithm used by d3-dag (https://github.com/erikbrinkman/d3-dag):

  1. Layering    - longest-path-from-roots layering, so every edge points
                   strictly downward and layer depth reflects true
                   pipeline depth.
  2. Dummy nodes - edges that skip layers are split into a chain of
                   invisible waypoint nodes, one per intermediate layer,
                   so long edges can be ordered/routed like any other
                   node instead of being drawn as straight lines that
                   cut through unrelated nodes.
  3. Ordering    - median-heuristic crossing reduction (Sugiyama), swept
                   up/down across the layers for several iterations,
                   keeping the ordering with the fewest crossings seen.
  4. Coordinates - iterative barycenter (average-neighbor-position)
                   relaxation within each layer, with a min-separation
                   pass so nodes/waypoints never overlap.
  5. Rendering   - smooth curves through dummy waypoints, rounded-rect
                   nodes, and a print-friendly monochrome-first palette
                   distinguishing sources/staging/marts by shape+color
                   so the figure is legible in black-and-white print.

Positional arguments are paths to dbt projects (any directory containing
target/manifest.json); --project/-p is a shortcut for a benchmark project
in projects/, named by its directory.

Usage:
    python3 paper/draw.py                    # draws all 10 benchmark pipelines
    python3 paper/draw.py -p p01_iot
    python3 paper/draw.py -p p01_iot -p tpch -p tpcds --outdir paper/figures
    python3 paper/draw.py -p synth/multi-sink/p01_ecommerce --direction TB
    python3 paper/draw.py paper/example-dag             # a project by path
    python3 paper/draw.py ~/work/my_dbt_project --pdf   # ...anywhere on disk
    python3 paper/draw.py -p p01_iot --title "IoT pipeline" --font "Times New Roman, serif"
    python3 paper/draw.py paper/example-dag --peek=C    # show model C's SQL beside the DAG

--peek=NODE highlights one model and adds a callout panel with its SQL
(the Jinja source, as in the project), connected to the node by a leader
line routed through the gap between layers so it never crosses a node.

PDF output needs one of: cairosvg (pure Python, `pip install cairosvg`),
rsvg-convert (librsvg), or inkscape; --pdf-converter picks one explicitly.

Output goes to paper/figures/, the "compiled" artifacts dropped straight
into the paper.
"""
import argparse
import difflib
import importlib.util
import json
import os
import random
import re
import shutil
import subprocess
import textwrap

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.join(REPO_ROOT, 'projects')
OUT_DIR = os.path.join(REPO_ROOT, 'paper', 'figures')

# The 10 benchmark pipelines, drawn by default when no projects are given.
DEFAULT_PROJECTS = [
    'p01_iot', 'p02_adtech', 'p03_ecommerce', 'p04_fraud', 'p05_hr',
    'p06_logistics', 'p07_saas', 'p08_healthcare', 'p09_gaming', 'p10_energy',
]

# Resource types treated as real pipeline stages. Tests/docs/analyses are
# noise in a DAG figure and are always excluded.
STAGE_RESOURCES = {'model', 'seed', 'snapshot'}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_manifest(project_dir):
    path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} not found; run `dbt parse` inside {project_dir} first"
        )
    with open(path) as f:
        return json.load(f)


def resolve_benchmark(name):
    """Benchmark shortcut (--project): a directory name under projects/."""
    name = name.strip('/')
    project_dir = os.path.join(ROOT_DIR, name)
    if not os.path.isdir(project_dir):
        raise FileNotFoundError(
            f"no benchmark project {name!r} in {ROOT_DIR}; pass a path instead "
            "if the project lives elsewhere"
        )
    # keep the benchmark's nested name in the figure filename, as before
    return name.replace('/', '__'), name.replace('/', ' / '), project_dir


def resolve_path(path):
    """A dbt project given directly by path."""
    project_dir = os.path.abspath(os.path.expanduser(path))
    if not os.path.isdir(project_dir):
        raise FileNotFoundError(f"{path} is not a directory")
    name = os.path.basename(project_dir.rstrip(os.sep)) or 'project'
    return name, name, project_dir


class Node:
    __slots__ = (
        'uid', 'label', 'kind', 'layer', 'order', 'x', 'y',
        'width', 'height', 'is_dummy', 'materialized',
    )

    def __init__(self, uid, label, kind, materialized=None, is_dummy=False):
        self.uid = uid
        self.label = label
        self.kind = kind  # 'source' | 'model' | 'seed' | 'snapshot'
        self.materialized = materialized
        self.is_dummy = is_dummy
        self.layer = 0
        self.order = 0
        self.x = 0.0
        self.y = 0.0
        self.width = 0.0
        self.height = 0.0


def build_graph(manifest):
    """Returns (nodes: dict[uid, Node], edges: list[(src_uid, dst_uid)])."""
    nodes = {}

    for uid, src in manifest['sources'].items():
        nodes[uid] = Node(uid, src['name'], 'source')

    for uid, n in manifest['nodes'].items():
        if n['resource_type'] not in STAGE_RESOURCES:
            continue
        nodes[uid] = Node(
            uid, n['name'], n['resource_type'],
            materialized=n.get('config', {}).get('materialized'),
        )

    edges = []
    for uid in nodes:
        if uid not in manifest['parent_map']:
            continue
        for parent in manifest['parent_map'][uid]:
            if parent in nodes:
                edges.append((parent, uid))

    return nodes, edges


# ---------------------------------------------------------------------------
# 1. Layering (longest path from roots)
# ---------------------------------------------------------------------------


def assign_layers(nodes, edges):
    children = {uid: [] for uid in nodes}
    indeg = {uid: 0 for uid in nodes}
    for a, b in edges:
        children[a].append(b)
        indeg[b] += 1

    layer = {uid: 0 for uid in nodes}
    # Kahn's algorithm, propagating longest-path depth.
    queue = [uid for uid in nodes if indeg[uid] == 0]
    remaining = dict(indeg)
    order_seen = []
    while queue:
        # deterministic order
        queue.sort()
        uid = queue.pop(0)
        order_seen.append(uid)
        for c in children[uid]:
            layer[c] = max(layer[c], layer[uid] + 1)
            remaining[c] -= 1
            if remaining[c] == 0:
                queue.append(c)

    if len(order_seen) != len(nodes):
        raise ValueError("graph is not a DAG (cycle detected)")

    for uid, l in layer.items():
        nodes[uid].layer = l


# ---------------------------------------------------------------------------
# 2. Dummy nodes for edges that span more than one layer
# ---------------------------------------------------------------------------


def insert_dummy_nodes(nodes, edges):
    """Returns (layered_edges, chains) where layered_edges only ever connect
    adjacent layers, and chains maps an original edge -> ordered list of
    uids (real endpoints + dummy waypoints) for rendering."""
    layered_edges = []
    chains = []
    dummy_count = 0

    for a, b in edges:
        la, lb = nodes[a].layer, nodes[b].layer
        if lb - la == 1:
            layered_edges.append((a, b))
            chains.append([a, b])
            continue

        chain = [a]
        prev = a
        for l in range(la + 1, lb):
            dummy_uid = f"__dummy_{dummy_count}__"
            dummy_count += 1
            d = Node(dummy_uid, '', 'dummy', is_dummy=True)
            d.layer = l
            nodes[dummy_uid] = d
            layered_edges.append((prev, dummy_uid))
            chain.append(dummy_uid)
            prev = dummy_uid
        layered_edges.append((prev, b))
        chain.append(b)
        chains.append(chain)

    return layered_edges, chains


# ---------------------------------------------------------------------------
# 3. Ordering within layers (median heuristic crossing reduction)
# ---------------------------------------------------------------------------


def layers_of(nodes):
    by_layer = {}
    for n in nodes.values():
        by_layer.setdefault(n.layer, []).append(n)
    for l in by_layer:
        by_layer[l].sort(key=lambda n: n.order)
    return by_layer


def count_crossings(layered_edges, order):
    total = 0
    by_src_layer = {}
    for a, b in layered_edges:
        by_src_layer.setdefault(order[a][0], []).append((order[a][1], order[b][1]))
    for l, pairs in by_src_layer.items():
        pairs.sort()
        for i in range(len(pairs)):
            for j in range(i + 1, len(pairs)):
                if pairs[i][1] > pairs[j][1]:
                    total += 1
    return total


def median_value(values):
    if not values:
        return -1.0
    values = sorted(values)
    m = len(values) // 2
    if len(values) % 2 == 1:
        return float(values[m])
    if len(values) == 2:
        return (values[0] + values[1]) / 2.0
    left = values[m - 1] - values[0]
    right = values[-1] - values[m]
    if left + right == 0:
        return (values[m - 1] + values[m]) / 2.0
    return (values[m - 1] * right + values[m] * left) / (left + right)


def reorder_layers(nodes, layered_edges, num_sweeps=8, seed=0):
    rng = random.Random(seed)
    by_layer = layers_of(nodes)
    max_layer = max(by_layer)

    for l in by_layer:
        for i, n in enumerate(by_layer[l]):
            n.order = i

    parents = {uid: [] for uid in nodes}
    children = {uid: [] for uid in nodes}
    for a, b in layered_edges:
        children[a].append(b)
        parents[b].append(a)

    def current_order():
        return {n.uid: (n.layer, n.order) for n in nodes.values()}

    def apply_medians(neighbor_map, layer_range):
        for l in layer_range:
            layer_nodes = by_layer[l]
            keyed = []
            for n in layer_nodes:
                neigh_orders = [nodes[m].order for m in neighbor_map[n.uid]]
                med = median_value(neigh_orders)
                keyed.append((med, n))
            # nodes with no neighbors in the adjacent layer keep relative order
            fixed_order = [n.order for _, n in keyed]
            keyed_indexed = list(zip(keyed, range(len(keyed))))

            def sort_key(item):
                (med, n), idx = item
                return (med if med >= 0 else fixed_order[idx], idx)

            keyed_indexed.sort(key=sort_key)
            for i, ((_, n), _) in enumerate(keyed_indexed):
                n.order = i
            by_layer[l] = [n for (_, n), _ in keyed_indexed]

    best_crossings = count_crossings(layered_edges, current_order())
    best_snapshot = {n.uid: n.order for n in nodes.values()}

    for sweep in range(num_sweeps):
        if sweep % 2 == 0:
            apply_medians(parents, range(1, max_layer + 1))
        else:
            apply_medians(children, range(max_layer - 1, -1, -1))

        crossings = count_crossings(layered_edges, current_order())
        if crossings <= best_crossings:
            best_crossings = crossings
            best_snapshot = {n.uid: n.order for n in nodes.values()}
        if best_crossings == 0:
            break

    for n in nodes.values():
        n.order = best_snapshot[n.uid]
    for l in by_layer:
        by_layer[l].sort(key=lambda n: n.order)

    return by_layer


# ---------------------------------------------------------------------------
# 4. Coordinate assignment
# ---------------------------------------------------------------------------


def assign_coordinates(nodes, by_layer, layered_edges, node_gap, layer_gap,
                        node_width, node_height, dummy_gap, direction):
    """Logical layout is always computed as if stacking layers along y and
    ordering nodes within a layer along x; render_svg's transform() swaps
    the two axes for LR. So the sizes used here must already be swapped:
    order_size is the on-screen extent along the within-layer axis (screen
    y for LR, screen x for TB) and depth_size is the on-screen extent along
    the between-layer axis (screen x for LR, screen y for TB) - otherwise
    layer_gap is sized for the wrong axis and adjacent layers overlap."""
    order_size = node_height if direction == 'LR' else node_width
    depth_size = node_width if direction == 'LR' else node_height

    parents = {uid: [] for uid in nodes}
    children = {uid: [] for uid in nodes}
    for a, b in layered_edges:
        children[a].append(b)
        parents[b].append(a)

    for n in nodes.values():
        if n.is_dummy:
            n.width, n.height = 2.0, 2.0
        else:
            n.width, n.height = order_size, depth_size

    max_layer = max(by_layer)
    for l in range(max_layer + 1):
        x = 0.0
        for n in by_layer[l]:
            n.x = x + n.width / 2.0
            gap = dummy_gap if (n.is_dummy) else node_gap
            x += n.width + gap
        n_layer_width = x - (dummy_gap if by_layer[l][-1].is_dummy else node_gap)
        # center this layer around 0 for now; global centering happens later
        offset = -n_layer_width / 2.0
        for n in by_layer[l]:
            n.x += offset

    layer_pitch = layer_gap + depth_size
    for n in nodes.values():
        n.y = n.layer * layer_pitch

    def enforce_min_sep(layer_nodes):
        layer_nodes = sorted(layer_nodes, key=lambda n: n.x)
        for i in range(1, len(layer_nodes)):
            prev, cur = layer_nodes[i - 1], layer_nodes[i]
            min_gap = (dummy_gap if (prev.is_dummy or cur.is_dummy) else node_gap)
            min_x = prev.x + prev.width / 2.0 + min_gap + cur.width / 2.0
            if cur.x < min_x:
                cur.x = min_x

    iterations = 30
    for it in range(iterations):
        sweep = range(1, max_layer + 1) if it % 2 == 0 else range(max_layer - 1, -1, -1)
        for l in sweep:
            for n in by_layer[l]:
                neigh = parents[n.uid] if it % 2 == 0 else children[n.uid]
                neigh = neigh or (children[n.uid] if it % 2 == 0 else parents[n.uid])
                if neigh:
                    avg = sum(nodes[m].x for m in neigh) / len(neigh)
                    n.x = n.x * 0.25 + avg * 0.75
            enforce_min_sep(by_layer[l])

    all_x = [n.x - n.width / 2.0 for n in nodes.values()]
    all_x_max = [n.x + n.width / 2.0 for n in nodes.values()]
    min_x = min(all_x)
    for n in nodes.values():
        n.x -= min_x

    width = max(all_x_max) - min_x
    height = max_layer * layer_pitch + depth_size
    return width, height


# ---------------------------------------------------------------------------
# 5. Rendering
# ---------------------------------------------------------------------------

PALETTE = {
    'source': {'fill': '#f5f0e6', 'stroke': '#8a7654', 'text': '#3a3226'},
    'seed':   {'fill': '#f5f0e6', 'stroke': '#8a7654', 'text': '#3a3226'},
    'model':  {'fill': '#eef2f7', 'stroke': '#3a5a78', 'text': '#1c2e40'},
    'snapshot': {'fill': '#eef2f7', 'stroke': '#3a5a78', 'text': '#1c2e40'},
    'table':  {'fill': '#b9d6ce', 'stroke': '#24564c', 'text': '#0f231f'},
}


def style_for(n):
    if n.kind == 'source':
        return PALETTE['source']
    if n.materialized in ('table', 'incremental'):
        return PALETTE['table']
    return PALETTE['model']


def escape_xml(s):
    return (
        s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        .replace('"', '&quot;')
    )


def wrap_label(label, max_chars):
    """Split a dbt model name into up to 2 lines, breaking on underscores."""
    if len(label) <= max_chars:
        return [label]
    parts = label.split('_')
    lines, cur = [], ''
    for p in parts:
        cand = f"{cur}_{p}" if cur else p
        if len(cand) > max_chars and cur:
            lines.append(cur)
            cur = p
        else:
            cur = cand
    if cur:
        lines.append(cur)
    if len(lines) > 2:
        lines = [lines[0], '_'.join(lines[1:])]
    return lines[:2]


def rounded_rect_path(x, y, w, h, r):
    return (
        f"M{x+r},{y} H{x+w-r} A{r},{r} 0 0 1 {x+w},{y+r} "
        f"V{y+h-r} A{r},{r} 0 0 1 {x+w-r},{y+h} "
        f"H{x+r} A{r},{r} 0 0 1 {x},{y+h-r} "
        f"V{y+r} A{r},{r} 0 0 1 {x+r},{y} Z"
    )


def edge_path(points, direction):
    """Smooth cubic path through a chain of (x, y) waypoints."""
    if len(points) == 2:
        (x0, y0), (x1, y1) = points
        if direction == 'TB':
            my = (y0 + y1) / 2.0
            return f"M{x0:.2f},{y0:.2f} C{x0:.2f},{my:.2f} {x1:.2f},{my:.2f} {x1:.2f},{y1:.2f}"
        else:
            mx = (x0 + x1) / 2.0
            return f"M{x0:.2f},{y0:.2f} C{mx:.2f},{y0:.2f} {mx:.2f},{y1:.2f} {x1:.2f},{y1:.2f}"

    d = f"M{points[0][0]:.2f},{points[0][1]:.2f} "
    for i in range(len(points) - 1):
        x0, y0 = points[i]
        x1, y1 = points[i + 1]
        if direction == 'TB':
            my = (y0 + y1) / 2.0
            d += f"C{x0:.2f},{my:.2f} {x1:.2f},{my:.2f} {x1:.2f},{y1:.2f} "
        else:
            mx = (x0 + x1) / 2.0
            d += f"C{mx:.2f},{y0:.2f} {mx:.2f},{y1:.2f} {x1:.2f},{y1:.2f} "
    return d.strip()


# ---------------------------------------------------------------------------
# --peek: a callout panel showing one node's SQL
# ---------------------------------------------------------------------------

PEEK_ACCENT = '#c0582b'
PEEK_GAP = 36.0  # between the DAG and the panel
PEEK_FONT_SIZE = 10.5
# advance width of the common monospace faces (Menlo, Consolas, DejaVu Sans
# Mono, Liberation Mono, Courier), used to size the panel to its text
PEEK_CHAR_W = 0.6 * PEEK_FONT_SIZE
PEEK_LINE_H = 14.0
PEEK_PAD = 10.0
PEEK_HEADER_H = 24.0
PEEK_GUTTER_GAP = 12.0

SQL_KEYWORDS = {
    'all', 'and', 'as', 'asc', 'between', 'by', 'case', 'cross', 'current',
    'desc', 'distinct', 'else', 'end', 'except', 'exists', 'filter',
    'following', 'from', 'full', 'group', 'having', 'ilike', 'in', 'inner',
    'intersect', 'interval', 'is', 'join', 'lateral', 'left', 'like', 'limit',
    'not', 'null', 'offset', 'on', 'or', 'order', 'outer', 'over', 'partition',
    'preceding', 'qualify', 'range', 'right', 'row', 'rows', 'select', 'then',
    'unbounded', 'union', 'using', 'when', 'where', 'window', 'with',
}

SQL_TOKEN_RE = re.compile(
    r"(?P<jinja>\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\})"
    r"|(?P<comment>--.*)"
    r"|(?P<string>'(?:[^']|'')*'?)"
    r"|(?P<word>[A-Za-z_][A-Za-z0-9_]*)"
)

# monochrome-first like the node palette: keywords and comments stay
# distinguishable by weight/style alone in black-and-white print
CODE_STYLES = {
    'plain': 'fill="#24292f"',
    'keyword': 'fill="#1f4a7a" font-weight="700"',
    'jinja': f'fill="{PEEK_ACCENT}"',
    'string': 'fill="#4d6b2a"',
    'comment': 'fill="#7a7f87" font-style="italic"',
}


def sql_of(manifest_node):
    # dbt >= 1.3 calls it raw_code; older manifests use raw_sql
    return manifest_node.get('raw_code') or manifest_node.get('raw_sql') or ''


def resolve_peek(manifest, nodes, query, label, max_lines, max_cols, font):
    """Finds the node named by --peek (a model name or unique_id) and
    prepares its SQL as numbered rows for the panel."""
    uids = [query] if query in nodes else [
        uid for uid, n in nodes.items() if n.label == query
    ]
    if not uids:
        close = difflib.get_close_matches(query, sorted({n.label for n in nodes.values()}), n=3)
        hint = f"; did you mean {', '.join(close)}?" if close else ''
        raise ValueError(f"{label}: --peek node {query!r} not found{hint}")

    with_sql = [
        uid for uid in uids
        if uid in manifest['nodes'] and sql_of(manifest['nodes'][uid]).strip()
    ]
    if not with_sql:
        raise ValueError(
            f"{label}: --peek node {query!r} is a {nodes[uids[0]].kind} and has no SQL to show"
        )
    if len(with_sql) > 1:
        raise ValueError(
            f"{label}: --peek {query!r} is ambiguous; pass a unique_id instead: "
            + ', '.join(with_sql)
        )
    uid = with_sql[0]
    mnode = manifest['nodes'][uid]

    lines = [l.rstrip() for l in sql_of(mnode).expandtabs(4).splitlines()]
    first = 0
    while not lines[first]:
        first += 1
    lines = lines[first:]
    while not lines[-1]:
        lines.pop()
    lines = textwrap.dedent('\n'.join(lines)).split('\n')

    # numbered by line in the source file, so the figure can be cross-referenced
    rows = [
        (first + 1 + i, l if len(l) <= max_cols else l[:max_cols - 1] + '…')
        for i, l in enumerate(lines)
    ]
    if len(rows) > max_lines:
        omitted = len(rows) - (max_lines - 1)
        rows = rows[:max_lines - 1] + [(None, f'-- … {omitted} more lines')]

    return {
        'uid': uid,
        'label': mnode['name'],
        'path': mnode.get('original_file_path', ''),
        'rows': rows,
        'font': font,
    }


def peek_panel_size(peek):
    """Returns (width, height, line-number digits) of the panel."""
    digits = len(str(max(n for n, _ in peek['rows'] if n is not None)))
    code_w = max(len(text) for _, text in peek['rows']) * PEEK_CHAR_W
    body_w = 2 * PEEK_PAD + digits * PEEK_CHAR_W + PEEK_GUTTER_GAP + code_w
    # header: bold 11px name + 10px path, estimated like the legend text
    header_w = 2 * PEEK_PAD + 6.6 * len(peek['label']) + 8 + 5.6 * len(peek['path'])
    height = (
        PEEK_HEADER_H + 2 * PEEK_PAD
        + len(peek['rows']) * PEEK_LINE_H - (PEEK_LINE_H - PEEK_FONT_SIZE)
    )
    return max(body_w, header_w), height, digits


def sql_tspans(line, code_x):
    spans = []  # (kind, start column, text)
    pos = 0
    for m in SQL_TOKEN_RE.finditer(line):
        if m.start() > pos:
            spans.append(('plain', pos, line[pos:m.start()]))
        kind = m.lastgroup
        if kind == 'word':
            kind = 'keyword' if m.group().lower() in SQL_KEYWORDS else 'plain'
        spans.append((kind, m.start(), m.group()))
        pos = m.end()
    if pos < len(line):
        spans.append(('plain', pos, line[pos:]))
    # Renderers disagree on whitespace in SVG text (rsvg drops spaces at
    # tspan boundaries, even non-breaking ones), which flattens the SQL's
    # indentation. So no whitespace is emitted at all: every word is placed
    # at its own column.
    return ''.join(
        f'<tspan x="{code_x + (start + w.start()) * PEEK_CHAR_W:.1f}" {CODE_STYLES[kind]}>'
        f'{escape_xml(w.group())}</tspan>'
        for kind, start, text in spans
        for w in re.finditer(r'\S+', text)
    )


def render_peek_panel(parts, peek, x, y, w, h, digits):
    r = 6
    hh = PEEK_HEADER_H
    parts.append('<g>')
    parts.append(f'<path d="{rounded_rect_path(x, y, w, h, r)}" fill="#fcfcfb"/>')
    parts.append(
        f'<path d="M{x:.1f},{y + hh:.1f} V{y + r:.1f} A{r},{r} 0 0 1 {x + r:.1f},{y:.1f} '
        f'H{x + w - r:.1f} A{r},{r} 0 0 1 {x + w:.1f},{y + r:.1f} V{y + hh:.1f} Z" '
        f'fill="#f5ede6"/>'
    )
    parts.append(
        f'<line x1="{x:.1f}" y1="{y + hh:.1f}" x2="{x + w:.1f}" y2="{y + hh:.1f}" '
        f'stroke="#e3d3c6" stroke-width="1"/>'
    )
    parts.append(
        f'<path d="{rounded_rect_path(x, y, w, h, r)}" fill="none" '
        f'stroke="{PEEK_ACCENT}" stroke-width="1.5"/>'
    )
    parts.append(
        f'<text x="{x + PEEK_PAD:.1f}" y="{y + hh / 2.0 + 4:.1f}" font-size="11" fill="#1c1c1c">'
        f'<tspan font-weight="700">{escape_xml(peek["label"])}</tspan>'
        f'<tspan dx="8" font-size="10" fill="#6b7078">{escape_xml(peek["path"])}</tspan>'
        '</text>'
    )

    num_x = x + PEEK_PAD + digits * PEEK_CHAR_W
    code_x = num_x + PEEK_GUTTER_GAP
    parts.append(f'<g font-family="{escape_xml(peek["font"])}" font-size="{PEEK_FONT_SIZE}">')
    for i, (lineno, text) in enumerate(peek['rows']):
        by = y + hh + PEEK_PAD + PEEK_FONT_SIZE * 0.8 + i * PEEK_LINE_H
        if lineno is not None:
            parts.append(
                f'<text x="{num_x:.1f}" y="{by:.1f}" text-anchor="end" '
                f'fill="#a3a8ae">{lineno}</text>'
            )
        if text.strip():
            parts.append(f'<text y="{by:.1f}">{sql_tspans(text, code_x)}</text>')
    parts.append('</g>')
    parts.append('</g>')


def render_svg(nodes, chains, direction, node_width, node_height, layer_gap,
                node_gap, canvas_w, canvas_h, title, project_label, font,
                peek=None):
    margin = 48
    legend_h = 40
    title_h = 34 if title else 0

    order_size = node_height if direction == 'LR' else node_width
    depth_size = node_width if direction == 'LR' else node_height
    # n.y is the *center* of a layer measured from the first layer's center,
    # while canvas_h already budgets a full node along the layer axis, so the
    # layer axis needs half a node of lead-in. Without it the first layer's
    # nodes are clipped by the left (LR) / top (TB) edge and all the slack
    # piles up at the opposite end.
    depth_lead = depth_size / 2.0

    # Extent of the content area in layout space: "order" runs within a
    # layer (n.x) and "depth" between layers (n.y). A peek panel is appended
    # past one end of the order axis - below/above the DAG for LR, right/left
    # of it for TB - so it never covers a node.
    order_total, depth_total = canvas_w, canvas_h
    order_shift = depth_shift = 0.0
    if peek:
        panel_w, panel_h, peek_digits = peek_panel_size(peek)
        p_order, p_depth = (panel_h, panel_w) if direction == 'LR' else (panel_w, panel_h)
        pn = nodes[peek['uid']]
        # whichever side the node is nearer to, to keep the connector short
        after = pn.x >= canvas_w / 2.0
        order_total = canvas_w + PEEK_GAP + p_order
        order_shift = 0.0 if after else PEEK_GAP + p_order
        depth_total = max(canvas_h, p_depth)
        depth_shift = (depth_total - canvas_h) / 2.0

    def to_screen(u, v):
        if direction == 'TB':
            return u + margin, v + margin + title_h
        return v + margin, u + margin + title_h

    def transform(n):
        return to_screen(n.x + order_shift, n.y + depth_lead + depth_shift)

    if peek:
        side = 1.0 if after else -1.0
        node_u = pn.x + order_shift
        node_v = pn.y + depth_lead + depth_shift
        gap_u = canvas_w + PEEK_GAP / 2.0 if after else p_order + PEEK_GAP / 2.0
        panel_o = canvas_w + PEEK_GAP if after else 0.0
        edge_u = panel_o if after else p_order
        blocked = any(
            not m.is_dummy and m.layer == pn.layer and (m.x - pn.x) * side > 0
            for m in nodes.values()
        )
        if not blocked:
            # Nothing but edges between the node and the panel in its layer:
            # leave straight out of the node's side facing the panel.
            start = (node_u + side * order_size / 2.0, node_v)
            route_v = node_v
        else:
            # Leave the node's downstream face a little off-center toward
            # the panel (so the connector doesn't sit on the node's own
            # outgoing edges), then run along the node-free channel between
            # this layer and the next, so no other node is crossed.
            node_u += side * order_size * 0.3
            route_v = min(node_v + depth_size / 2.0 + layer_gap / 2.0, depth_total + margin * 0.75)
            start = (node_u, node_v + depth_size / 2.0)
        panel_d = min(max(route_v - p_depth / 2.0, 0.0), depth_total - p_depth)
        end_v = min(max(route_v, panel_d + 12.0), panel_d + p_depth - 12.0)
        pts = [start, (node_u, route_v)] if blocked else [start]
        if end_v != route_v:
            # the panel was clamped short of the route; jog in the gap
            pts += [(gap_u, route_v), (gap_u, end_v)]
        pts.append((edge_u, end_v))
        connector = [to_screen(u, v) for u, v in pts]
        sx0, sy0 = to_screen(panel_o, panel_d)
        sx1, sy1 = to_screen(panel_o + p_order, panel_d + p_depth)
        panel_x, panel_y = min(sx0, sx1), min(sy0, sy1)

    def boundary_point(uid, is_source):
        """Point on the node's edge (not its center) that an edge should
        touch, so the arrowhead marker at the path's end lands on the
        node's border and stays visible instead of being drawn under it."""
        n = nodes[uid]
        x, y = transform(n)
        if n.is_dummy:
            return x, y
        if direction == 'TB':
            return (x, y + node_height / 2.0) if is_source else (x, y - node_height / 2.0)
        return (x + node_width / 2.0, y) if is_source else (x - node_width / 2.0, y)

    if direction == 'TB':
        svg_w = order_total + 2 * margin
        svg_h = depth_total + 2 * margin + title_h + legend_h
    else:
        svg_w = depth_total + 2 * margin
        svg_h = order_total + 2 * margin + title_h + legend_h

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{svg_w:.0f}" '
        f'height="{svg_h:.0f}" viewBox="0 0 {svg_w:.0f} {svg_h:.0f}" '
        f'font-family="{escape_xml(font)}">'
    )
    parts.append(f'<rect x="0" y="0" width="{svg_w:.0f}" height="{svg_h:.0f}" fill="white"/>')

    parts.append(
        '<defs>'
        '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="#555a61"/>'
        '</marker>'
        '</defs>'
    )

    if title:
        parts.append(
            f'<text x="{svg_w/2:.0f}" y="{margin/2 + 6:.0f}" '
            f'text-anchor="middle" font-size="18" font-weight="600" '
            f'fill="#1c1c1c">{escape_xml(title)}</text>'
        )

    # edges first, under nodes
    parts.append('<g fill="none" stroke="#8b929b" stroke-width="1.4">')
    for chain in chains:
        pts = [transform(nodes[uid]) for uid in chain]
        pts[0] = boundary_point(chain[0], is_source=True)
        pts[-1] = boundary_point(chain[-1], is_source=False)
        d = edge_path(pts, direction)
        parts.append(f'<path d="{d}" marker-end="url(#arrow)"/>')
    parts.append('</g>')

    # peek connector, under the nodes so its halo doesn't eat the node border
    if peek:
        d = 'M' + ' L'.join(f'{x:.1f},{y:.1f}' for x, y in connector)
        parts.append(
            f'<path d="{d}" fill="none" stroke="white" stroke-width="5" '
            'stroke-linejoin="round"/>'
        )
        parts.append(
            f'<path d="{d}" fill="none" stroke="{PEEK_ACCENT}" stroke-width="1.6" '
            'stroke-dasharray="5 3" stroke-linejoin="round"/>'
        )

    # nodes
    parts.append('<g>')
    max_chars = 14
    for n in nodes.values():
        if n.is_dummy:
            continue
        x, y = transform(n)
        style = style_for(n)
        w, h = node_width, node_height
        rx, ry = x - w / 2.0, y - h / 2.0
        peeked = peek is not None and n.uid == peek['uid']
        parts.append(
            f'<path d="{rounded_rect_path(rx, ry, w, h, 6)}" fill="{style["fill"]}" '
            f'stroke="{PEEK_ACCENT if peeked else style["stroke"]}" '
            f'stroke-width="{2.5 if peeked else 1.5}"/>'
        )
        lines = wrap_label(n.label, max_chars)
        font_size = 11 if len(lines) == 1 else 10
        line_h = font_size + 3
        start_y = y - (len(lines) - 1) * line_h / 2.0 + font_size / 3.0
        for i, line in enumerate(lines):
            parts.append(
                f'<text x="{x:.1f}" y="{start_y + i * line_h:.1f}" '
                f'text-anchor="middle" font-size="{font_size}" '
                f'fill="{style["text"]}">{escape_xml(line)}</text>'
            )
    parts.append('</g>')

    if peek:
        cx, cy = connector[0]
        parts.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="3" fill="{PEEK_ACCENT}"/>')
        render_peek_panel(parts, peek, panel_x, panel_y, panel_w, panel_h, peek_digits)

    # legend
    legend_items = [
        ('Source', PALETTE['source']),
        ('View', PALETTE['model']),
        ('Table', PALETTE['table']),
    ]
    ly = svg_h - legend_h / 2.0 - 4
    # swatch (16) + 6 gap + label text, then a gap between items; the text
    # width is estimated at ~6.2px/char for 11px Helvetica so the whole strip
    # can be centered on the canvas.
    item_gap = 26.0
    item_widths = [22.0 + 6.2 * len(label) for label, _ in legend_items]
    lx = (svg_w - (sum(item_widths) + item_gap * (len(legend_items) - 1))) / 2.0
    parts.append('<g font-size="11" fill="#1c1c1c">')
    for (label, style), item_w in zip(legend_items, item_widths):
        parts.append(
            f'<rect x="{lx:.1f}" y="{ly - 7:.1f}" width="16" height="14" rx="3" '
            f'fill="{style["fill"]}" stroke="{style["stroke"]}" stroke-width="1.5"/>'
        )
        parts.append(f'<text x="{lx + 22:.1f}" y="{ly + 4:.1f}">{escape_xml(label)}</text>')
        lx += item_w + item_gap
    parts.append('</g>')

    parts.append('</svg>')
    return '\n'.join(parts)


# ---------------------------------------------------------------------------
# SVG -> PDF conversion
# ---------------------------------------------------------------------------

# Tried in order by --pdf-converter auto. cairosvg is pure Python (plus
# libcairo) and needs no subprocess, so it wins when it is importable;
# the CLI tools are the fallback for machines without it.
PDF_CONVERTERS = ('cairosvg', 'rsvg-convert', 'inkscape')


def _convert_cairosvg(svg_path, pdf_path):
    import cairosvg  # noqa: PLC0415 - optional dependency, imported on demand
    cairosvg.svg2pdf(url=svg_path, write_to=pdf_path)


def _convert_rsvg(svg_path, pdf_path):
    subprocess.run(
        ['rsvg-convert', '-f', 'pdf', '-o', pdf_path, svg_path],
        check=True, capture_output=True,
    )


def _convert_inkscape(svg_path, pdf_path):
    subprocess.run(
        ['inkscape', svg_path, '--export-type=pdf', f'--export-filename={pdf_path}'],
        check=True, capture_output=True,
    )


def converter_available(name):
    if name == 'cairosvg':
        return importlib.util.find_spec('cairosvg') is not None
    return shutil.which(name) is not None


def svg_to_pdf(svg_path, converter='auto'):
    """Writes <svg_path>.pdf next to the SVG; returns (pdf_path, converter)."""
    if converter == 'auto':
        candidates = [c for c in PDF_CONVERTERS if converter_available(c)]
        if not candidates:
            raise RuntimeError(
                "no SVG->PDF converter found; install cairosvg "
                "(`pip install cairosvg`) or librsvg/inkscape"
            )
        converter = candidates[0]
    elif not converter_available(converter):
        raise RuntimeError(f"requested --pdf-converter {converter} is not available")

    pdf_path = os.path.splitext(svg_path)[0] + '.pdf'
    fn = {
        'cairosvg': _convert_cairosvg,
        'rsvg-convert': _convert_rsvg,
        'inkscape': _convert_inkscape,
    }[converter]
    try:
        fn(svg_path, pdf_path)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or b'').decode(errors='replace').strip()
        raise RuntimeError(f"{converter} failed on {svg_path}: {stderr}") from e
    return pdf_path, converter


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def draw_project(name, label, project_dir, outdir, direction, node_width,
                  node_height, layer_gap, node_gap, dummy_gap, title, font,
                  peek=None, peek_max_lines=40, peek_max_cols=100,
                  peek_font='monospace'):
    manifest = load_manifest(project_dir)
    nodes, edges = build_graph(manifest)
    if not nodes:
        raise ValueError(f"{label}: no model/seed/snapshot/source nodes found")

    peek_info = None
    if peek:
        peek_info = resolve_peek(
            manifest, nodes, peek, label, peek_max_lines, peek_max_cols, peek_font,
        )
        # keep the plain figure; the peek variant gets its own file
        name = f"{name}__peek_{peek_info['label']}"

    assign_layers(nodes, edges)
    layered_edges, chains = insert_dummy_nodes(nodes, edges)
    by_layer = reorder_layers(nodes, layered_edges)
    canvas_w, canvas_h = assign_coordinates(
        nodes, by_layer, layered_edges, node_gap, layer_gap,
        node_width, node_height, dummy_gap, direction,
    )

    svg = render_svg(
        nodes, chains, direction, node_width, node_height, layer_gap,
        node_gap, canvas_w, canvas_h, title, label, font, peek_info,
    )

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, name + '.svg')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(svg)
    return out_path, len([n for n in nodes.values() if not n.is_dummy]), len(edges)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'paths', nargs='*', metavar='PATH',
        help=(
            'path(s) to dbt project(s) to draw - any directory holding '
            'target/manifest.json, e.g. paper/example-dag'
        ),
    )
    parser.add_argument(
        '--project', '-p', dest='projects', action='append', default=[],
        metavar='NAME',
        help=(
            f"benchmark project to draw, named by its directory under "
            f"{os.path.relpath(ROOT_DIR, REPO_ROOT)}/, e.g. p01_iot, tpch, "
            "synth/multi-sink/p01_ecommerce (repeatable). With no PATH and no "
            f"--project, the 10 benchmark pipelines are drawn: {', '.join(DEFAULT_PROJECTS)}"
        ),
    )
    parser.add_argument(
        '--outdir', default=OUT_DIR,
        help=f'output directory (default: {os.path.relpath(OUT_DIR, REPO_ROOT)})',
    )
    parser.add_argument('--direction', choices=['TB', 'LR'], default='LR', help='layout direction (default: LR)')
    parser.add_argument('--node-width', type=float, default=118.0)
    parser.add_argument('--node-height', type=float, default=40.0)
    parser.add_argument('--layer-gap', type=float, default=90.0)
    parser.add_argument('--node-gap', type=float, default=22.0)
    parser.add_argument('--dummy-gap', type=float, default=14.0)
    parser.add_argument('--no-title', action='store_true', help='omit the title text above the figure')
    parser.add_argument(
        '--title', default=None,
        help='title text above the figure (default: the project name); applied to every project drawn',
    )
    parser.add_argument(
        '--font', default='Helvetica, Arial, sans-serif',
        help='SVG font-family for all text (default: "Helvetica, Arial, sans-serif")',
    )
    parser.add_argument(
        '--peek', metavar='NODE', default=None,
        help=(
            'highlight NODE (a model name or unique_id) and show its SQL in a '
            'callout panel beside the DAG; written to <project>__peek_<NODE>.svg'
        ),
    )
    parser.add_argument(
        '--peek-max-lines', type=int, default=40,
        help='longest SQL shown by --peek before it is truncated (default: 40)',
    )
    parser.add_argument(
        '--peek-max-cols', type=int, default=100,
        help='longest SQL line shown by --peek before it is cut off (default: 100)',
    )
    parser.add_argument(
        '--peek-font', default='Menlo, Consolas, DejaVu Sans Mono, Liberation Mono, monospace',
        help='SVG font-family for the --peek SQL; should be monospace',
    )
    parser.add_argument(
        '--pdf', action='store_true',
        help='also write a PDF next to each SVG',
    )
    parser.add_argument(
        '--pdf-converter', choices=('auto',) + PDF_CONVERTERS, default='auto',
        help=(
            'converter used by --pdf (default: auto, which prefers the pure-Python '
            f"cairosvg and falls back to {', '.join(PDF_CONVERTERS[1:])})"
        ),
    )
    args = parser.parse_args()
    if args.peek_max_lines < 2:
        parser.error('--peek-max-lines must be at least 2')
    if args.peek_max_cols < 10:
        parser.error('--peek-max-cols must be at least 10')

    benchmarks = args.projects
    if not benchmarks and not args.paths:
        benchmarks = DEFAULT_PROJECTS

    targets = [resolve_benchmark(name) for name in benchmarks]
    targets += [resolve_path(p) for p in args.paths]

    for name, label, project_dir in targets:
        title = None if args.no_title else (args.title or label)
        out_path, n_nodes, n_edges = draw_project(
            name, label, project_dir, args.outdir, args.direction,
            args.node_width, args.node_height, args.layer_gap, args.node_gap,
            args.dummy_gap, title, args.font, args.peek, args.peek_max_lines,
            args.peek_max_cols, args.peek_font,
        )
        print(f"{label}: {n_nodes} nodes, {n_edges} edges -> {out_path}")
        if args.pdf:
            pdf_path, converter = svg_to_pdf(out_path, args.pdf_converter)
            print(f"{label}: {converter} -> {pdf_path}")


if __name__ == '__main__':
    main()
