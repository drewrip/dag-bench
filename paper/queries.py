"""
Emits a LaTeX table of the SQL behind every node of a dbt DAG, meant to sit
next to that project's figure from draw.py.

The figure shows the *shape* of a pipeline - which node feeds which, and what
is materialized as a table vs. a view - but says nothing about what any node
actually computes. This table fills that gap: one row per node, in the same
top-to-bottom layer order the figure uses, showing the node's name, its
materialization, and its SQL body verbatim.

Sources have no SQL of their own, so they are listed with the external
relation they read (schema.identifier) instead.

Node order matches draw.py exactly: nodes are grouped by their longest-path
layer (so every node appears below all of its parents) and sorted by name
within a layer. Layering is imported from draw.py so the two never drift.

Targets are given the same way as draw.py: positional arguments are paths to
a dbt project (any directory holding target/manifest.json), and --project/-p
is a shortcut for a benchmark project under projects/. With neither, the
example DAG in paper/example-dag is used - the one the companion figure is
drawn from.

The SQL is syntax-highlighted by a small built-in lexer (keywords, strings,
numbers, comments, and dbt's {{ ref() }} / {{ source() }} Jinja, which is the
lineage the figure draws). Highlighting is emitted as five macros - \\sqlkw,
\\sqlstr, \\sqlnum, \\sqlcom, \\sqljinja - defined in a \\providecommand block
above the table, so the snippet is self-contained but a paper can override any
of them in its own preamble (or use --no-preamble and define all five).
--highlight mono swaps the colors for bold/italic, and --highlight none drops
the markup entirely.

The generated table needs booktabs, xcolor for the default color highlighting,
and reads best with T1 fonts (straight quotes/underscores in the monospace SQL)
plus a monospace family that has a bold weight:

    \\usepackage{booktabs}
    \\usepackage{xcolor}
    \\usepackage[T1]{fontenc}
    \\usepackage{lmodern}

Usage:
    python3 paper/queries.py                             # example DAG -> stdout
    python3 paper/queries.py -o paper/figures/queries.tex
    python3 paper/queries.py -p p01_iot --outdir paper/figures
    python3 paper/queries.py -p p01_iot --compiled --wrap 72
    python3 paper/queries.py --highlight mono            # no color, bold keywords
    python3 paper/queries.py ~/work/my_dbt_project --no-sources
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# draw.py owns manifest loading, target resolution and layering; reusing it
# keeps this table's row order identical to the figure's layer order.
from draw import (  # noqa: E402 - needs the sys.path tweak above
    build_graph,
    assign_layers,
    load_manifest,
    resolve_benchmark,
    resolve_path,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(REPO_ROOT, 'paper', 'figures')
EXAMPLE_DAG = os.path.join(REPO_ROOT, 'paper', 'example-dag')

# How a node's materialization is spelled in the table's second column.
TYPE_LABELS = {
    'source': 'source',
    'seed': 'seed',
    'snapshot': 'snapshot',
    'view': 'view',
    'table': 'table',
    'incremental': 'incremental',
    'ephemeral': 'ephemeral',
    'materialized_view': 'mat.\\ view',
}

# ---------------------------------------------------------------------------
# Collecting rows
# ---------------------------------------------------------------------------


def node_rows(manifest, include_sources=True, compiled=False):
    """Returns [(name, type_label, sql_lines)] in figure layer order.

    sql_lines is a list of raw (unescaped) source lines; for a source node it
    is a single pseudo-line naming the external relation it reads.
    """
    nodes, edges = build_graph(manifest)
    assign_layers(nodes, edges)

    ordered = sorted(nodes.values(), key=lambda n: (n.layer, n.label))

    rows = []
    for n in ordered:
        if n.kind == 'source':
            if not include_sources:
                continue
            src = manifest['sources'][n.uid]
            relation = '.'.join(
                p for p in (src.get('schema'), src.get('identifier') or src['name']) if p
            )
            rows.append((n.label, TYPE_LABELS['source'], [relation], True))
            continue

        entry = manifest['nodes'][n.uid]
        sql = None
        if compiled:
            sql = entry.get('compiled_code')
        if not sql:
            sql = entry.get('raw_code') or ''
        lines = [l.rstrip() for l in sql.strip('\n').split('\n')] or ['']

        if n.kind == 'model':
            label = TYPE_LABELS.get(n.materialized, n.materialized or 'view')
        else:
            label = TYPE_LABELS.get(n.kind, n.kind)
        rows.append((n.label, label, lines, False))

    return rows


def wrap_sql(lines, width):
    """Soft-wraps over-long SQL lines, indenting continuations under the
    original line's indent so the wrap reads as a wrap and not as new SQL."""
    if not width:
        return lines

    out = []
    for line in lines:
        if len(line) <= width:
            out.append(line)
            continue
        indent = ' ' * (len(line) - len(line.lstrip(' ')) + 4)
        cur = line
        while len(cur) > width:
            # break on the last separator that still fits, else hard-break
            cut = max(cur.rfind(' ', 0, width + 1), cur.rfind(',', 0, width + 1) + 1)
            if cut <= len(indent):
                cut = width
            out.append(cur[:cut].rstrip())
            cur = indent + cur[cut:].lstrip(' ')
        out.append(cur)
    return out


# ---------------------------------------------------------------------------
# SQL lexing and highlighting
# ---------------------------------------------------------------------------

# Deliberately a flat set rather than a per-dialect grammar: the goal is a
# readable figure, not a parser, and every benchmark project is plain
# ANSI-ish SQL plus dbt's Jinja.
SQL_KEYWORDS = {
    'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'begin', 'between', 'by',
    'case', 'cast', 'coalesce', 'create', 'cross', 'cube', 'current', 'delete',
    'desc', 'distinct', 'drop', 'else', 'end', 'except', 'exists', 'false',
    'filter', 'first', 'following', 'from', 'full', 'group', 'grouping',
    'having', 'ilike', 'in', 'inner', 'insert', 'intersect', 'interval',
    'into', 'is', 'join', 'lateral', 'last', 'left', 'like', 'limit', 'not',
    'null', 'nulls', 'offset', 'on', 'or', 'order', 'outer', 'over',
    'partition', 'preceding', 'qualify', 'range', 'right', 'rollup', 'rows',
    'select', 'set', 'similar', 'some', 'table', 'then', 'true', 'union',
    'unbounded', 'unique', 'update', 'using', 'values', 'view', 'when',
    'where', 'window', 'with', 'within',
}

# Token kinds a highlighter is asked to style.
TOKEN_KINDS = ('kw', 'str', 'num', 'com', 'jinja', 'plain', 'space')

_SQL_SCANNER = re.compile(
    r"""
      (?P<space>[ \t]+)
    | (?P<jinja>\{\{.*?\}\}|\{%.*?%\}|\{\#.*?\#\})
    | (?P<com>--[^\n]*)
    | (?P<comstart>/\*)
    | (?P<str>'(?:[^']|'')*'?|"(?:[^"]|"")*"?)
    | (?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
    | (?P<word>[A-Za-z_][A-Za-z_0-9$]*)
    | (?P<other>.)
    """,
    re.VERBOSE,
)


def tokenize_sql(line, state=None):
    """Yields (text, kind) covering `line` exactly, so joining the texts
    reproduces the input. `state` is an optional dict carrying
    'block_comment' across lines of the same query."""
    state = state if state is not None else {}
    pos = 0

    while pos < len(line):
        if state.get('block_comment'):
            end = line.find('*/', pos)
            if end == -1:
                yield line[pos:], 'com'
                return
            yield line[pos:end + 2], 'com'
            state['block_comment'] = False
            pos = end + 2
            continue

        m = _SQL_SCANNER.match(line, pos)
        text = m.group()
        kind = m.lastgroup
        pos = m.end()

        if kind == 'comstart':
            end = line.find('*/', pos)
            if end == -1:
                state['block_comment'] = True
                yield line[m.start():], 'com'
                return
            yield line[m.start():end + 2], 'com'
            pos = end + 2
            continue
        if kind == 'word':
            kind = 'kw' if text.lower() in SQL_KEYWORDS else 'plain'
        elif kind == 'other':
            kind = 'plain'

        yield text, kind


# Colors are picked to match draw.py's print-friendly palette and to stay
# distinguishable once a reviewer prints the paper in grayscale (they differ
# in darkness, and comments/strings also differ in shape via italics).
HIGHLIGHT_COLORS = {
    'kw': '2B4C6F',     # steel blue, as the model nodes in the figure
    'str': '7A5B27',    # brown, as the source nodes
    'num': '3D6B63',    # teal, as the materialized tables
    'com': '6E747D',    # gray
    'jinja': '6B3F72',  # plum: dbt ref()/source() calls, i.e. the DAG edges
}

# Emitted above the table so the snippet drops into a paper unchanged;
# \providecommand/\providecolor mean a preamble that already defines its own
# versions wins, and --no-preamble drops them entirely.
PREAMBLE_COLOR = (
    "% SQL highlighting used by the table below (needs xcolor).\n"
    + ''.join(
        f"\\providecolor{{sql{kind}}}{{HTML}}{{{value}}}\n"
        for kind, value in HIGHLIGHT_COLORS.items()
    )
    + "\\providecommand{\\sqlkw}[1]{\\textcolor{sqlkw}{#1}}\n"
    "\\providecommand{\\sqlstr}[1]{\\textcolor{sqlstr}{#1}}\n"
    "\\providecommand{\\sqlnum}[1]{\\textcolor{sqlnum}{#1}}\n"
    "\\providecommand{\\sqlcom}[1]{\\textcolor{sqlcom}{\\textit{#1}}}\n"
    "\\providecommand{\\sqljinja}[1]{\\textcolor{sqljinja}{#1}}\n"
)

# Monochrome variant: same macro names, no color, so the table body is
# identical either way and only this block changes. Bold typewriter needs a
# font family that has one (lmodern, newtx, ...); cmtt alone silently falls
# back to medium weight, which just reads as no highlighting.
PREAMBLE_MONO = (
    "% SQL highlighting used by the table below (bold tt needs e.g. lmodern).\n"
    "\\providecommand{\\sqlkw}[1]{\\textbf{#1}}\n"
    "\\providecommand{\\sqlstr}[1]{#1}\n"
    "\\providecommand{\\sqlnum}[1]{#1}\n"
    "\\providecommand{\\sqlcom}[1]{\\textit{#1}}\n"
    "\\providecommand{\\sqljinja}[1]{\\textbf{#1}}\n"
)

MACRO_FOR_KIND = {
    'kw': '\\sqlkw',
    'str': '\\sqlstr',
    'num': '\\sqlnum',
    'com': '\\sqlcom',
    'jinja': '\\sqljinja',
}


def make_highlighter(mode):
    """Returns (highlighter, preamble); highlighter is None for --highlight none."""
    if mode == 'none':
        return None, ''

    def highlight(escaped_text, kind):
        macro = MACRO_FOR_KIND.get(kind)
        return f"{macro}{{{escaped_text}}}" if macro else escaped_text

    return highlight, PREAMBLE_COLOR if mode == 'color' else PREAMBLE_MONO


# ---------------------------------------------------------------------------
# LaTeX emission
# ---------------------------------------------------------------------------

ESCAPES = {
    '\\': r'\textbackslash{}',
    '{': r'\{',
    '}': r'\}',
    '$': r'\$',
    '&': r'\&',
    '#': r'\#',
    '%': r'\%',
    '_': r'\_',
    '^': r'\textasciicircum{}',
    '~': r'\textasciitilde{}',
    '<': r'\textless{}',
    '>': r'\textgreater{}',
}


def escape(text):
    return ''.join(ESCAPES.get(c, c) for c in text)


def escape_spaces(text, leading=False):
    r"""Escapes a run of literal spaces. A single interior space stays an
    ordinary space; leading spaces and runs of 2+ become fixed-width \ spaces,
    which TeX will neither collapse nor stretch, so indentation and hand-made
    column alignment inside the SQL survive."""
    if leading or len(text) > 1:
        return '\\ ' * len(text)
    return ' '


def escape_sql_line(line, highlighter=None, state=None):
    r"""Escapes one SQL line for a \ttfamily LaTeX cell, optionally wrapping
    each token in a highlighting macro."""
    stripped = line.lstrip(' ')
    lead = len(line) - len(stripped)
    prefix = '\\ ' * lead

    if highlighter is None:
        out = []
        for text, _ in tokenize_sql(stripped, state=None):
            out.append(escape_spaces(text) if text.strip() == '' else escape(text))
        return prefix + ''.join(out)

    out = []
    for text, kind in tokenize_sql(stripped, state=state):
        if text.strip() == '':
            out.append(escape_spaces(text))
        else:
            out.append(highlighter(escape(text), kind))
    return prefix + ''.join(out)


def sql_cell(lines, is_source, highlighter=None):
    """A nested left-aligned tabular so a multi-line SQL body can live inside
    a single cell of the outer table without disturbing its column widths."""
    if is_source:
        return f"\\emph{{external relation}} \\texttt{{{escape(lines[0])}}}"

    # one mutable lexer state per node, so a /* ... */ comment that spans
    # lines keeps its styling across the rows of the nested tabular
    state = {'block_comment': False}
    body = ' \\\\\n        '.join(
        f"\\texttt{{{escape_sql_line(l, highlighter, state)}}}" if l.strip() else '{}'
        for l in lines
    )
    return (
        "\\begin{tabular}[t]{@{}l@{}}\n        "
        + body
        + "\n      \\end{tabular}"
    )


def build_latex(rows, caption, label, size, placement, highlighter=None, preamble=''):
    header = (
        preamble
        + f"\\begin{{table}}[{placement}]\n"
        "  \\centering\n"
        f"  \\caption{{{caption}}}\n"
        f"  \\label{{{label}}}\n"
        f"  {size}\n"
        "  \\begin{tabular}{@{}lll@{}}\n"
        "    \\toprule\n"
        "    Node & Materialization & SQL \\\\\n"
        "    \\midrule\n"
    )

    blocks = []
    for name, type_label, lines, is_source in rows:
        blocks.append(
            f"    \\texttt{{{escape(name)}}} & {type_label} & "
            f"{sql_cell(lines, is_source, highlighter)} \\\\\n"
        )

    body = "    \\midrule\n".join(blocks)
    footer = (
        "    \\bottomrule\n"
        "  \\end{tabular}\n"
        "\\end{table}\n"
    )
    return header + body + footer


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def table_for(label, project_dir, args, preamble=''):
    manifest = load_manifest(project_dir)
    rows = node_rows(manifest, include_sources=not args.no_sources, compiled=args.compiled)
    if not rows:
        raise ValueError(f"{label}: no model/seed/snapshot/source nodes found")
    rows = [(n, t, wrap_sql(l, args.wrap), s) for n, t, l, s in rows]

    caption = args.caption or (
        f"SQL behind each node of the {escape(label)} pipeline shown in "
        "Figure~\\ref{fig:dag}."
    )
    # underscores are unsafe in a \label, so the auto-generated one uses dashes
    slug = label.replace(' / ', '-').replace('/', '-').replace(' ', '').replace('_', '-')
    tab_label = args.label or f"tab:queries-{slug}"
    highlighter, _ = make_highlighter(args.highlight)
    return build_latex(
        rows, caption, tab_label, args.size, args.placement, highlighter, preamble,
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'paths', nargs='*', metavar='PATH',
        help=(
            'path(s) to dbt project(s) to tabulate - any directory holding '
            'target/manifest.json (default: paper/example-dag)'
        ),
    )
    parser.add_argument(
        '--project', '-p', dest='projects', action='append', default=[],
        metavar='NAME',
        help='benchmark project under projects/, e.g. p01_iot (repeatable)',
    )
    parser.add_argument(
        '-o', '--output', default=None,
        help='write the LaTeX to this file instead of stdout',
    )
    parser.add_argument(
        '--outdir', default=None,
        help=(
            'write one <project>-queries.tex per project into this directory '
            f"(e.g. {os.path.relpath(OUT_DIR, REPO_ROOT)})"
        ),
    )
    parser.add_argument(
        '--compiled', action='store_true',
        help='prefer compiled SQL (needs `dbt compile`) over the raw dbt source',
    )
    parser.add_argument(
        '--no-sources', action='store_true',
        help='omit source nodes, listing only models/seeds/snapshots',
    )
    parser.add_argument(
        '--wrap', type=int, default=64, metavar='N',
        help=(
            'soft-wrap SQL lines longer than N characters (0 disables). 64 fits '
            'the example DAG in a single column at \\footnotesize; the benchmark '
            'pipelines have longer node names and SQL, so they want a smaller N '
            "(~40) or a wider float (table*, landscape)"
        ),
    )
    parser.add_argument(
        '--highlight', choices=('color', 'mono', 'none'), default='color',
        help=(
            'SQL syntax highlighting: color (default, needs xcolor), mono '
            '(bold keywords/Jinja + italic comments, no color), or none'
        ),
    )
    parser.add_argument(
        '--no-preamble', action='store_true',
        help=(
            'omit the \\providecolor/\\providecommand block that defines the '
            'highlighting macros (define \\sqlkw, \\sqlstr, \\sqlnum, \\sqlcom '
            'and \\sqljinja in your own preamble instead)'
        ),
    )
    parser.add_argument('--caption', default=None, help='table caption')
    parser.add_argument('--label', default=None, help=r'table \label (single project only)')
    parser.add_argument(
        '--size', default=r'\footnotesize',
        help=r'font size command applied inside the table (default: \footnotesize)',
    )
    parser.add_argument('--placement', default='t', help='float placement (default: t)')
    args = parser.parse_args()

    targets = [resolve_benchmark(n) for n in args.projects]
    targets += [resolve_path(p) for p in args.paths]
    if not targets:
        targets = [resolve_path(EXAMPLE_DAG)]

    if args.label and len(targets) > 1:
        parser.error('--label only makes sense with a single project')

    _, preamble = make_highlighter(args.highlight)
    if args.no_preamble:
        preamble = ''

    chunks = []
    for name, label, project_dir in targets:
        # each --outdir file is \input separately, so each carries the macro
        # definitions; a concatenated stream only needs them once
        emit_preamble = preamble if (args.outdir or not chunks) else ''
        latex = table_for(label, project_dir, args, emit_preamble)
        if args.outdir:
            os.makedirs(args.outdir, exist_ok=True)
            out_path = os.path.join(args.outdir, f"{name}-queries.tex")
            with open(out_path, 'w') as f:
                f.write(latex)
            print(f"{label} -> {out_path}")
        else:
            chunks.append(latex)

    if chunks:
        out = '\n'.join(chunks)
        if args.output:
            os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
            with open(args.output, 'w') as f:
                f.write(out)
            print(f"wrote {args.output}")
        else:
            print(out, end='')


if __name__ == '__main__':
    main()
