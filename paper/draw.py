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

PDF output needs one of: cairosvg (pure Python, `pip install cairosvg`),
rsvg-convert (librsvg), or inkscape; --pdf-converter picks one explicitly.

Output goes to paper/figures/, the "compiled" artifacts dropped straight
into the paper.
"""
import argparse
import importlib.util
import json
import os
import random
import shutil
import subprocess

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
    'table':  {'fill': '#e7eef0', 'stroke': '#3d6b63', 'text': '#1d332f'},
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


def render_svg(nodes, chains, direction, node_width, node_height, layer_gap,
                node_gap, canvas_w, canvas_h, title, project_label, font):
    margin = 48
    legend_h = 40
    title_h = 34 if title else 0

    # n.y is the *center* of a layer measured from the first layer's center,
    # while canvas_h already budgets a full node along the layer axis, so the
    # layer axis needs half a node of lead-in. Without it the first layer's
    # nodes are clipped by the left (LR) / top (TB) edge and all the slack
    # piles up at the opposite end.
    depth_lead = (node_width if direction == 'LR' else node_height) / 2.0

    def transform(n):
        if direction == 'TB':
            return n.x + margin, n.y + margin + title_h + depth_lead
        return n.y + margin + depth_lead, n.x + margin + title_h

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
        svg_w = canvas_w + 2 * margin
        svg_h = canvas_h + 2 * margin + title_h + legend_h
    else:
        svg_w = canvas_h + 2 * margin
        svg_h = canvas_w + 2 * margin + title_h + legend_h

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
        parts.append(
            f'<path d="{rounded_rect_path(rx, ry, w, h, 6)}" '
            f'fill="{style["fill"]}" stroke="{style["stroke"]}" stroke-width="1.5"/>'
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
                  node_height, layer_gap, node_gap, dummy_gap, title, font):
    manifest = load_manifest(project_dir)
    nodes, edges = build_graph(manifest)
    if not nodes:
        raise ValueError(f"{label}: no model/seed/snapshot/source nodes found")

    assign_layers(nodes, edges)
    layered_edges, chains = insert_dummy_nodes(nodes, edges)
    by_layer = reorder_layers(nodes, layered_edges)
    canvas_w, canvas_h = assign_coordinates(
        nodes, by_layer, layered_edges, node_gap, layer_gap,
        node_width, node_height, dummy_gap, direction,
    )

    svg = render_svg(
        nodes, chains, direction, node_width, node_height, layer_gap,
        node_gap, canvas_w, canvas_h, title, label, font,
    )

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, name + '.svg')
    with open(out_path, 'w') as f:
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
            args.dummy_gap, title, args.font,
        )
        print(f"{label}: {n_nodes} nodes, {n_edges} edges -> {out_path}")
        if args.pdf:
            pdf_path, converter = svg_to_pdf(out_path, args.pdf_converter)
            print(f"{label}: {converter} -> {pdf_path}")


if __name__ == '__main__':
    main()
