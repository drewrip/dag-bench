#!/usr/bin/env python3
"""Free Snowflake-compatibility checks (rung 2 of utils/snowflake_stage.py).

  python utils/check_snowflake_compat.py [--no-compile] [project ...]

1. Source lint: model SQL (Jinja and comments stripped) must not use the raw
   constructs that need a dagbench_* macro on Snowflake.
2. Offline compile: `dbt compile --target snowflake` with a bogus account and
   token, so it can never log in (no cost, no failed logins on the real user),
   into target/sf_offline/. Then every compiled model and test is
   - pattern-scanned for the same constructs (catches package macros too) and
   - parsed with sqlglot's snowflake dialect.

Projects default to every entry in projects.yaml. Exit 1 on any finding.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DBT = ROOT / ".venv" / "bin" / "dbt"
OFFLINE_DIR = "target/sf_offline"

RULES: list[tuple[str, re.Pattern]] = [
    ("FILTER (WHERE ...) aggregate -> dagbench_agg_filter", re.compile(r"\)\s*filter\s*\(\s*where\b", re.I)),
    ("extract(epoch from a - b) -> dagbench_epoch_diff", re.compile(r"\bextract\s*\(\s*epoch\b", re.I)),
    ("quoted date part extract('x' from ..) -> dagbench_extract", re.compile(r"\bextract\s*\(\s*'", re.I)),
    ("bool_or/bool_and -> dagbench_fn", re.compile(r"\bbool_(or|and)\s*\(", re.I)),
    ("least/greatest are NULL-propagating on Snowflake -> dagbench_fn", re.compile(r"(?<![\w.])(least|greatest)\s*\(", re.I)),
    ("string_agg (not on Snowflake: listagg ... within group) -> dagbench_string_agg", re.compile(r"\bstring_agg\s*\(", re.I)),
    ("similar to (not on Snowflake) -> dagbench_similar_to", re.compile(r"\bsimilar\s+to\b", re.I)),
    ("regexp_replace(..., 'g'): a position on Snowflake -> dagbench_regexp_flag_g", re.compile(r",\s*'g'\s*\)", re.I)),
    ("::interval cast", re.compile(r"::\s*interval\b", re.I)),
    ("~ regex operator", re.compile(r"(?<![!<>=~])\s~\*?\s")),
    ("regexp_matches (DuckDB/Postgres only)", re.compile(r"\bregexp_matches\s*\(", re.I)),
    ("strptime (DuckDB only)", re.compile(r"\bstrptime\s*\(", re.I)),
    ("mixed/lower-case quoted identifier (case-sensitive on Snowflake)", re.compile(r'"[A-Za-z_][A-Za-z0-9_ ]*[a-z][A-Za-z0-9_ ]*"')),
    # A lone backslash; doubled ones are what Snowflake needs for a literal backslash.
    ("backslash in a string literal (an escape on Snowflake)", re.compile(r"'[^'\n]*(?<!\\)\\(?!\\)[^'\n]*'")),
    ("named WINDOW clause / over <name> (not on Snowflake) -> dagbench_over", re.compile(r"\bwindow\s+\w+\s+as\s*\(|\bover\s+(?!\(|__J__)[a-z_]\w*", re.I)),
    ("list/struct literal or [] indexing (DuckDB only)", re.compile(r"\[\s*\d+\s*\]|\blist_\w+\s*\(|\bstruct_pack\s*\(", re.I)),
]

JINJA = re.compile(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}", re.S)
LINE_COMMENT = re.compile(r"--[^\n]*")
BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)


def blank_dead_branches(sql: str) -> str:
    """Blank out code that never renders on Snowflake (e.g. the `else` of
    `{% if target.type == 'snowflake' %}`), keeping line numbers."""
    from utils.snowflake_rewrite_models import skip_spans

    out = list(sql)
    for a, b in skip_spans(sql):
        if sql[a:a + 2] in ("{{", "{%", "{#", "--"):
            continue  # tags and comments are handled by strip()
        for i in range(a, b):
            if out[i] != "\n":
                out[i] = " "
    return "".join(out)


def strip(sql: str) -> str:
    """Drop Jinja and comments, keeping line numbers."""
    sql = blank_dead_branches(sql)
    keep_lines = lambda m: "\n" * m.group(0).count("\n")  # noqa: E731
    # A Jinja expression becomes a placeholder token so `over {{ ... }}` isn't `over as`.
    sql = JINJA.sub(lambda m: "__J__" + keep_lines(m) if m.group(0).startswith("{{") else keep_lines(m), sql)
    sql = BLOCK_COMMENT.sub(keep_lines, sql)
    return LINE_COMMENT.sub("", sql)


def scan(text: str, label: str, findings: list[str]) -> None:
    for name, pat in RULES:
        for m in pat.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            findings.append(f"{label}:{line}: {name}: {m.group(0).strip()[:60]!r}")


def project_list(args: list[str]) -> list[str]:
    if args:
        return [a.strip("/").split("/")[-1] for a in args]
    import yaml

    return [p.strip("/").split("/")[-1] for p in yaml.safe_load((ROOT / "projects.yaml").read_text())]


def offline_compile(pdir: Path) -> tuple[bool, str]:
    import shutil

    shutil.rmtree(pdir / OFFLINE_DIR, ignore_errors=True)  # never lint stale output
    env = os.environ | {
        # Any connection attempt (is_incremental(), introspection) fails locally:
        # *.snowflakecomputing.com is a DNS wildcard, so a bogus account alone
        # would still reach Snowflake's servers.
        "HTTPS_PROXY": "http://127.0.0.1:9",
        "https_proxy": "http://127.0.0.1:9",
        "HTTP_PROXY": "http://127.0.0.1:9",
        "http_proxy": "http://127.0.0.1:9",
        "NO_PROXY": "",
        "no_proxy": "",
        "DAGBENCH_SF_ACCOUNT": "dagbench-offline",
        "DAGBENCH_SF_TOKEN": "offline-compile-no-login",
        "DAGBENCH_SF_PRIVATE_KEY_PATH": "",
        "DBT_TARGET": "snowflake",
    }
    proc = subprocess.run(
        [str(DBT), "compile", "--target", "snowflake", "--no-populate-cache", "--no-partial-parse",
         "--quiet", "--target-path", OFFLINE_DIR,
         # is_incremental() looks the table up, which needs a connection; a failed
         # node also makes dbt drop nodes in flight. Their children compile fine.
         "--exclude", "config.materialized:incremental"],
        cwd=pdir, env=env, capture_output=True, text=True,
    )
    return proc.returncode == 0, (proc.stdout + proc.stderr)[-3000:]


def parse_check(sql: str, label: str, findings: list[str]) -> None:
    import sqlglot
    from sqlglot.errors import ParseError, TokenError

    try:
        sqlglot.parse(sql, read="snowflake")
    except (ParseError, TokenError) as exc:
        msg = str(exc).splitlines()[0][:160]
        findings.append(f"{label}: sqlglot(snowflake) parse error: {msg}")


def check_project(project: str, compile_: bool) -> list[str]:
    pdir = ROOT / "projects" / project
    findings: list[str] = []
    for f in sorted((pdir / "models").rglob("*.sql")):
        scan(strip(f.read_text()), str(f.relative_to(ROOT)), findings)
    if not compile_:
        return findings
    ok, log = offline_compile(pdir)
    if not ok:
        findings.append(f"{project}: offline snowflake compile FAILED:\n{log}")
        return findings
    compiled = pdir / OFFLINE_DIR / "compiled"
    # dbt can exit 0 after a node failed to compile (e.g. is_incremental() needs
    # a connection), skipping its descendants: check completeness.
    import json

    manifest = json.loads((pdir / OFFLINE_DIR / "manifest.json").read_text())
    def compiled_file(n: dict) -> Path:
        ofp = n["original_file_path"]
        if ofp.endswith((".yml", ".yaml")):  # generic test: <yml path>/<test file>
            return compiled / n["package_name"] / ofp / Path(n["path"]).name
        return compiled / n["package_name"] / ofp

    nodes = [n for n in manifest["nodes"].values()
             if n["resource_type"] in ("model", "test") and n["package_name"] == project
             and n["config"].get("enabled", True)]
    incremental = sorted(n["unique_id"] for n in nodes if n["config"].get("materialized") == "incremental")
    # Tests on an excluded incremental model are excluded with it (dbt's indirect selection).
    skipped = set(incremental) | {n["unique_id"] for n in nodes if n["resource_type"] == "test"
                                  and set(n["depends_on"]["nodes"]) & set(incremental)}
    missing = sorted(n["unique_id"] for n in nodes
                     if not compiled_file(n).exists() and n["unique_id"] not in skipped)
    if incremental:
        print(f"  WARN {project}: incremental models are not compiled offline (is_incremental() needs a "
              f"connection); rung 4 compiles them on Snowflake: {', '.join(incremental)}")
    if missing and "250001" in log:
        # Introspection at compile time (e.g. fivetran_utils.fill_staging_columns
        # reads the source's columns): only rung 4 on Snowflake can compile these.
        print(f"  WARN {project}: {len(missing)} node(s) need a connection to compile and were not "
              f"linted offline (rung 4 compiles them on Snowflake), e.g. {missing[:3]}")
    elif missing:
        findings.append(f"{project}: {len(missing)} node(s) not compiled offline, e.g. {missing[:5]}:\n{log}")
    for f in sorted(compiled.rglob("*.sql")):
        rel = f.relative_to(compiled)
        if rel.parts and rel.parts[0] != project:
            continue  # package models we don't build
        sql = f.read_text()
        label = f"{project}:compiled/{rel}"
        scan(LINE_COMMENT.sub("", BLOCK_COMMENT.sub("", sql)), label, findings)
        parse_check(sql, label, findings)
    return findings


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-compile", action="store_true", help="source lint only")
    ap.add_argument("projects", nargs="*")
    args = ap.parse_args(argv)
    total = 0
    for p in project_list(args.projects):
        findings = check_project(p, not args.no_compile)
        total += len(findings)
        print(f"{'OK  ' if not findings else 'FAIL'} {p}" + (f" ({len(findings)} finding(s))" if findings else ""))
        for f in findings:
            print(f"  {f}")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
