#!/usr/bin/env python3
"""Rewrite model and test SQL to call the Snowflake portability macros
(macros/dagbench/snowflake_compat.sql). Idempotent; --check lists what would
change without writing.

  python utils/snowflake_rewrite_models.py [--check] projects/p01_iot ...

Rewrites (outside comments, Jinja tags, and code that never renders on
Snowflake, e.g. the `else` of `{% if target.type == 'snowflake' %}`):
  agg(expr) filter (where cond)   -> {{ dagbench_agg_filter(agg, expr, cond) }}
  least( / greatest( / bool_or( / bool_and(  -> {{ dagbench_fn('least') }}(
  extract('part' from x)          -> {{ dagbench_extract('part', x) }}
  extract(epoch from a - b)       -> {{ dagbench_epoch_diff(a, b) }}
  string_agg(x, sep order by o)   -> {{ dagbench_string_agg(x, sep, o) }}
  regexp_replace(s, p, r, 'g')    -> regexp_replace(s, p, r{{ dagbench_regexp_flag_g() }})
  regexp_replace(s, p, r)         -> regexp_replace(s, p, r{{ dagbench_regexp_first_only() }})
  percentile_*(p) within group (order by x) filter (where c)
                                  -> {{ dagbench_ordered_set_filter(...) }}

Each renders back to the original text on duckdb and postgres; a match whose
spelling the macro can't reproduce is reported and left alone.
utils/check_compiled_unchanged.sh is the final word. Vendor scripts call
rewrite_tree(project, arg_hook=...) to rewrite macro arguments with their own
call rewrites first.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Callable

ArgHook = Callable[[str], str] | None


# ------------------------------------------------------------------ helpers

def jstr(s: str) -> str:
    """A Jinja string literal for plain SQL s."""
    if "'" not in s:
        return f"'{s}'"
    if '"' not in s:
        return f'"{s}"'
    raise ValueError(f"can't quote {s!r} for Jinja")


def jinja_string(sql: str) -> str:
    """`sql`, which may contain `{{ expr }}`, as a Jinja string expression."""
    if "{{" not in sql:
        return jstr(sql)
    if "{%" in sql:
        raise ValueError(f"can't pass a Jinja statement as a macro argument: {sql!r}")
    parts = []
    for i, piece in enumerate(re.split(r"\{\{(.*?)\}\}", sql, flags=re.S)):
        if i % 2:
            parts.append(f"({piece.strip()})")
        elif piece:
            parts.append(jstr(piece))
    return " ~ ".join(parts)


TAG = re.compile(r"\{%-?\s*(if|elif|else|endif)\b(.*?)-?%\}", re.S)
SF_TEST = re.compile(r"""target\.type\s*(==|!=)\s*['"](\w+)['"]""")


def skip_spans(text: str) -> list[tuple[int, int]]:
    """Comments, Jinja tags, and branches that never render on Snowflake."""
    spans = [(m.start(), m.end()) for m in re.finditer(r"\{#.*?#\}|\{\{.*?\}\}|\{%.*?%\}", text, re.S)]
    for m in re.finditer(r"--[^\n]*", text):
        if not any(a <= m.start() < b for a, b in spans):
            spans.append((m.start(), m.end()))
    # if/elif/else/endif stack: is the current branch reachable on snowflake?
    # Per open `if`: is the current branch reachable on snowflake ("live"), and
    # was an earlier branch `target.type == 'snowflake'` ("sure": later ones aren't).
    stack: list[dict] = []
    branch_start = None
    for m in TAG.finditer(text):
        kind, cond = m.group(1), m.group(2)
        if kind == "if":
            t = SF_TEST.search(cond)
            live = True
            if t and SF_TEST.fullmatch(cond.strip()):
                op, val = t.groups()
                live = (val == "snowflake") == (op == "==")
            stack.append({"live": live, "sure": bool(t and SF_TEST.fullmatch(cond.strip()) and live and t.group(1) == "==")})
        elif kind in ("elif", "else") and stack:
            top = stack[-1]
            if top["sure"]:
                live = False  # the snowflake branch was an earlier one
            elif kind == "elif":
                t = SF_TEST.search(cond)
                live = True
                if t and SF_TEST.fullmatch(cond.strip()):
                    op, val = t.groups()
                    live = (val == "snowflake") == (op == "==")
                    top["sure"] = live and op == "=="
            else:
                live = True
            top["live"] = live
        elif kind == "endif" and stack:
            stack.pop()
        dead = any(not f["live"] for f in stack)
        if dead and branch_start is None:
            branch_start = m.end()
        elif not dead and branch_start is not None:
            spans.append((branch_start, m.start()))
            branch_start = None
    return spans


def in_spans(i: int, spans: list[tuple[int, int]]) -> bool:
    return any(a <= i < b for a, b in spans)


def match_back(text: str, close: int) -> int:
    """Index of the '(' matching the ')' at `close` (quotes respected)."""
    depth, quote = 0, None
    for i in range(close, -1, -1):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c == ")":
            depth += 1
        elif c == "(":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("unbalanced ')'")


def match_fwd(text: str, open_: int) -> int:
    """Index of the ')' matching the '(' at `open_` (quotes respected)."""
    depth, quote = 0, None
    for i in range(open_, len(text)):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("unbalanced '('")


# ----------------------------------------------------------------- rewrites

FILTER_HEAD = re.compile(r"\)(?P<gap>\s*)(?P<head>filter\s*\(\s*where\s+)", re.I)
AGG_NAME = re.compile(r"\b(count|sum|avg|min|max)$", re.I)
WITHIN_GROUP = re.compile(r"\b(?P<fn>percentile_cont|percentile_disc|mode)\((?P<p>[^()]*)\)(?P<ws1>\s*)within(?P<ws2>\s+)group(?P<ws3>\s*)$", re.I)
ORDER_BY_HEAD = re.compile(r"^(?P<ob>\s*order\s+by\s+)", re.I)
FN = re.compile(r"(?<![\w.'])(?P<name>least|greatest|bool_or|bool_and)\(", re.I)
EXTRACT_Q = re.compile(r"\bextract\('(?P<part>\w+)' from (?P<expr>[^()]+?)\)", re.I)
EPOCH_HEAD = re.compile(r"\bextract\(epoch from ", re.I)
STRING_AGG_HEAD = re.compile(r"\bstring_agg\(", re.I)
REGEXP_REPLACE_HEAD = re.compile(r"\bregexp_replace\(", re.I)


def split_top(text: str, sep_re: str) -> list[str]:
    """Split on a regex at paren depth 0, outside quotes."""
    parts, depth, quote, start, i = [], 0, None, 0, 0
    pat = re.compile(sep_re, re.I)
    while i < len(text):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0:
            m = pat.match(text, i)
            if m:
                parts.append(text[start:i])
                parts.append(m.group(0))
                i = start = m.end()
                continue
        i += 1
    parts.append(text[start:])
    return parts  # [chunk, sep, chunk, sep, ...]


def rewrite_scanned(text: str, head: re.Pattern, build, path: str, problems: list[str]) -> str:
    """Replace `head ... )` calls (scanned, so arguments may nest) with build(inner, original) or None."""
    out, pos = [], 0
    spans = skip_spans(text)
    for m in head.finditer(text):
        if m.start() < pos or in_spans(m.start(), spans):
            continue
        close = match_fwd(text, text.index("(", m.start()))
        inner = text[m.end():close]  # after the head, e.g. after "extract(epoch from "
        new = build(inner, text[m.start():close + 1])
        if new is None:
            problems.append(f"{path}: left alone: {text[m.start():close + 1][:80]!r}")
            continue
        out.append(text[pos:m.start()])
        out.append(new)
        pos = close + 1
    out.append(text[pos:])
    return "".join(out)


def rewrite_filters(text: str, path: str, problems: list[str], arg_hook: ArgHook) -> str:
    hook = arg_hook or (lambda s: s)
    reported: set[str] = set()

    def report(msg: str) -> None:
        if msg not in reported:
            reported.add(msg)
            problems.append(msg)

    while True:
        spans = skip_spans(text)
        hit = None
        for m in FILTER_HEAD.finditer(text):
            if in_spans(m.start(), spans):
                continue
            open_ = match_back(text, m.start())
            name = AGG_NAME.search(text[:open_])
            wg = None if name else WITHIN_GROUP.search(text[:open_])
            if wg and ORDER_BY_HEAD.match(text[open_ + 1:m.start()]):
                close = match_fwd(text, m.end() - len(m["head"]) + m["head"].index("("))
                cond = text[m.end():close]
                ob = ORDER_BY_HEAD.match(text[open_ + 1:m.start()])["ob"]
                order_expr = text[open_ + 1 + len(ob):m.start()]
                if "filter" in cond.lower() or re.search(r"\bselect\b", cond, re.I):
                    report(f"{path}: ordered-set FILTER needs a hand rewrite")
                    continue
                head_txt = text[wg.start():open_ + 1] + ob  # e.g. "percentile_disc(0.5) within group (order by "
                hit = ("os", wg.start(), close + 1, head_txt, order_expr, cond, m["gap"], m["head"])
                break
            if not name:
                report(f"{path}: FILTER after a non-aggregate call, left alone: {text[max(0, open_ - 30):m.end()]!r}")
                continue
            close = match_fwd(text, m.end() - len(m["head"]) + m["head"].index("("))
            cond = text[m.end():close]
            expr = text[open_ + 1:m.start()]
            if re.search(r"\bselect\b", cond, re.I):
                report(f"{path}: subquery inside FILTER needs a hand rewrite: {text[name.start():close + 1][:80]!r}")
                continue
            if "filter" in cond.lower() or "filter" in expr.lower():
                continue  # nested FILTER: take the inner one first
            hit = ("agg", name.start(), close + 1, name.group(1), expr, cond, m["gap"], m["head"])
            break
        if hit is None:
            return text
        kind, start, end, agg, expr, cond, gap, head = hit
        if kind == "os":
            # agg = "fn(p) within group (order by ", expr = the order expression
            extra = ("" if gap == " " else f", gap={jstr(gap)}") + ("" if head == "filter (where " else f", fhead={jstr(head)}")
            call = (f"{{{{ dagbench_ordered_set_filter({jinja_string(hook(agg))}, {jinja_string(hook(expr))}, "
                    f"{jinja_string(hook(cond))}{extra}) }}}}")
            text = text[:start] + call + text[end:]
            continue
        extra = ("" if gap == " " else f", gap={jstr(gap)}") + ("" if head == "filter (where " else f", head={jstr(head)}")
        call = (f"{{{{ dagbench_agg_filter({jstr(agg)}, {jinja_string(hook(expr))}, "
                f"{jinja_string(hook(cond))}{extra}) }}}}")
        text = text[:start] + call + text[end:]


def sub_outside(pattern: re.Pattern, fn, text: str) -> str:
    spans = skip_spans(text)
    return pattern.sub(lambda m: m.group(0) if in_spans(m.start(), spans) else fn(m), text)


def rewrite(text: str, path: str, problems: list[str], arg_hook: ArgHook = None) -> str:
    def f_fn(m: re.Match) -> str:
        return f"{{{{ dagbench_fn({jstr(m['name'])}) }}}}("

    def f_extract(m: re.Match) -> str:
        return f"{{{{ dagbench_extract({jstr(m['part'])}, {jstr(m['expr'])}) }}}}"

    hook = arg_hook or (lambda s: s)

    def b_epoch(inner: str, orig: str):
        parts = split_top(inner, r" - ")
        if len(parts) != 3:
            return None
        a, b = parts[0], parts[2]
        if orig != f"extract(epoch from {a} - {b})":
            return None
        return f"{{{{ dagbench_epoch_diff({jinja_string(hook(a))}, {jinja_string(hook(b))}) }}}}"

    def b_regexp_replace(inner: str, orig: str):
        """Only the tail changes, so the arguments keep their spelling."""
        if "dagbench_regexp_" in inner:
            return orig  # already rewritten
        args = split_top(inner, r",")
        n = (len(args) + 1) // 2
        last = args[-1]
        trail = last[len(last.rstrip()):]  # whitespace before ')', kept as written
        if n == 3:
            return orig[:len(orig) - 1 - len(trail)] + "{{ dagbench_regexp_first_only() }}" + trail + ")"
        if n == 4 and last.strip().lower() == "'g'":
            tail = args[-2] + last.rstrip()  # ", 'g'" as written
            body = orig[:len(orig) - 1 - len(trail)]
            assert body.endswith(tail), orig
            return body[:-len(tail)] + "{{ dagbench_regexp_flag_g() }}" + trail + ")"
        return None

    def b_string_agg(inner: str, orig: str):
        parts = split_top(inner, r"\s+order\s+by\s+")
        if len(parts) != 3:
            return None
        head_args, kw, order = parts
        args = split_top(head_args, r",\s*")
        if len(args) != 3:
            return None
        expr, comma, sep = args
        if comma != ", " or orig != f"string_agg({expr}, {sep}{kw}{order})":
            return None
        extra = "" if kw == " order by " else f", kw={jstr(kw)}"
        return (f"{{{{ dagbench_string_agg({jinja_string(hook(expr))}, {jinja_string(sep)}, "
                f"{jinja_string(hook(order))}{extra}) }}}}")

    text = sub_outside(FN, f_fn, text)
    text = rewrite_filters(text, path, problems, arg_hook)
    text = sub_outside(EXTRACT_Q, f_extract, text)
    text = rewrite_scanned(text, EPOCH_HEAD, b_epoch, path, problems)
    text = rewrite_scanned(text, STRING_AGG_HEAD, b_string_agg, path, problems)
    text = rewrite_scanned(text, REGEXP_REPLACE_HEAD, b_regexp_replace, path, problems)
    return text


def sql_files(root: Path):
    """Model and singular-test SQL of a project."""
    for sub in ("models", "tests"):
        yield from sorted(Path(root, sub).rglob("*.sql"))


def rewrite_tree(root: Path, arg_hook: ArgHook = None, write: bool = True) -> tuple[list[Path], list[str]]:
    """Rewrite a project in place: (changed files, problems)."""
    problems: list[str] = []
    changed = []
    for f in sql_files(root):
        old = f.read_text()
        new = rewrite(old, str(f), problems, arg_hook)
        if new != old:
            changed.append(f)
            if write:
                f.write_text(new)
    return changed, problems


def main(argv: list[str]) -> int:
    check = "--check" in argv
    roots = [a for a in argv if a != "--check"]
    total, problems = 0, []
    for root in roots:
        changed, probs = rewrite_tree(Path(root), write=not check)
        problems += probs
        total += len(changed)
        for f in changed:
            print(f"{'would rewrite' if check else 'rewrote'} {f}")
    for p in problems:
        print("WARN", p, file=sys.stderr)
    print(f"{total} file(s) {'to change' if check else 'changed'}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
