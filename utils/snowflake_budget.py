#!/usr/bin/env python3
"""Budget ledger for every command that touches Snowflake.

  python -m utils.snowflake_budget run [--estimate-min M] [--label L] [--free] -- <cmd...>
  python -m utils.snowflake_budget status
  python -m utils.snowflake_budget config [--size XS] [--auto-suspend 60] [--type STANDARD]

`run` goes through `utils.snowflake_secret run`, so this is the one entry point
for both the budget and the token. It:

- refuses to start when spent + estimate > cap;
- kills the command (whole process group) once its running cost would cross the cap;
- records an upper bound of the warehouse cost to .snowflake_ledger.jsonl:
  ceil(wall time to whole minutes, at least 60 s) + AUTO_SUSPEND tail, times the
  size's credits/hr. This assumes nobody else kept the shared warehouse warm;
- prints the running total afterwards.

`--free` is only for commands that cannot resume the warehouse (SHOW/DESC/DDL,
e.g. the preflight); they're logged with 0 credits.

Cap: DAGBENCH_SF_CREDIT_CAP (default 2.5 credits). Warehouse size and
AUTO_SUSPEND come from the latest `config` record (written by the preflight);
until it exists, `run` refuses anything that is not --free.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = Path(os.environ.get("DAGBENCH_SF_LEDGER", ROOT / ".snowflake_ledger.jsonl"))
DEFAULT_CAP = 2.5

CREDITS_PER_HOUR = {
    "X-SMALL": 1, "XSMALL": 1, "XS": 1,
    "SMALL": 2, "S": 2,
    "MEDIUM": 4, "M": 4,
    "LARGE": 8, "L": 8,
    "X-LARGE": 16, "XLARGE": 16, "XL": 16,
    "2X-LARGE": 32, "XXLARGE": 32, "2XL": 32,
    "3X-LARGE": 64, "XXXLARGE": 64, "3XL": 64,
    "4X-LARGE": 128, "X4LARGE": 128, "4XL": 128,
    "5X-LARGE": 256, "5XL": 256,
    "6X-LARGE": 512, "6XL": 512,
}
# Snowpark-optimized warehouses bill 1.5x a standard one of the same size.
TYPE_MULTIPLIER = {"STANDARD": 1.0, "SNOWPARK-OPTIMIZED": 1.5}


def cap() -> float:
    return float(os.environ.get("DAGBENCH_SF_CREDIT_CAP", DEFAULT_CAP))


def read_ledger() -> list[dict]:
    if not LEDGER.exists():
        return []
    return [json.loads(l) for l in LEDGER.read_text().splitlines() if l.strip()]


def append(record: dict) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), **record}
    with LEDGER.open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def current_config(records: list[dict] | None = None) -> dict | None:
    cfgs = [r for r in (records if records is not None else read_ledger()) if r.get("kind") == "config"]
    return cfgs[-1] if cfgs else None


def spent(records: list[dict] | None = None) -> float:
    return sum(r.get("credits", 0.0) for r in (records if records is not None else read_ledger()) if r.get("kind") == "run")


def rate_per_sec(cfg: dict) -> float:
    size = str(cfg["size"]).upper().replace(" ", "")
    if size not in CREDITS_PER_HOUR:
        raise SystemExit(f"snowflake_budget: unknown warehouse size {cfg['size']!r}")
    mult = TYPE_MULTIPLIER.get(str(cfg.get("type", "STANDARD")).upper(), 1.5)
    return CREDITS_PER_HOUR[size] * mult / 3600.0


def charge(wall_s: float, cfg: dict) -> float:
    billed = max(60, math.ceil(wall_s / 60) * 60) + int(cfg.get("auto_suspend", 600))
    return billed * rate_per_sec(cfg)


def status_line(records: list[dict] | None = None) -> str:
    s = spent(records)
    return f"snowflake_budget: spent {s:.3f} / cap {cap():.3f} credits (upper bound), {cap() - s:.3f} left"


def cmd_status(_args) -> int:
    records = read_ledger()
    cfg = current_config(records)
    print(f"ledger: {LEDGER}")
    print(f"warehouse config: {cfg if cfg else 'none (run utils/snowflake_preflight.py)'}")
    for r in records:
        if r.get("kind") == "run":
            print(f"  {r['ts']}  {r['credits']:.3f} cr  {r['wall_s']:7.1f}s  rc={r['rc']}  {r.get('label') or ''}  {r['cmd']}")
    print(status_line(records))
    return 0


def cmd_config(args) -> int:
    cfg = dict(current_config() or {})
    cfg.pop("ts", None)
    cfg["kind"] = "config"
    if args.size:
        cfg["size"] = args.size
    if args.auto_suspend is not None:
        cfg["auto_suspend"] = args.auto_suspend
    if args.type:
        cfg["type"] = args.type
    if "size" not in cfg or "auto_suspend" not in cfg:
        print("snowflake_budget: config needs --size and --auto-suspend", file=sys.stderr)
        return 2
    rate_per_sec(cfg)  # validate
    append(cfg)
    print(f"snowflake_budget: config {cfg}")
    return 0


def cmd_run(args) -> int:
    cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
    if not cmd:
        print("snowflake_budget: nothing to run", file=sys.stderr)
        return 2
    records = read_ledger()
    cfg = current_config(records)
    already = spent(records)
    limit = cap()
    if args.free:
        estimate, max_wall = 0.0, None
    else:
        if cfg is None:
            print("snowflake_budget: no warehouse config yet; run utils/snowflake_preflight.py first", file=sys.stderr)
            return 2
        estimate = charge(args.estimate_min * 60, cfg)
        if already + estimate > limit:
            print(f"snowflake_budget: REFUSED: spent {already:.3f} + estimate {estimate:.3f} > cap {limit:.3f}", file=sys.stderr)
            return 4
        # Longest wall time whose charge still fits under the cap.
        room = limit - already
        max_wall = room / rate_per_sec(cfg) - int(cfg.get("auto_suspend", 600))
        max_wall = math.floor(max_wall / 60) * 60
        if max_wall < 60:
            print("snowflake_budget: REFUSED: less than one billable minute left under the cap", file=sys.stderr)
            return 4
    print(f"snowflake_budget: starting (estimate {estimate:.3f} cr; {status_line(records)[18:]})", flush=True)
    full = [sys.executable, "-m", "utils.snowflake_secret", "run", "--", *cmd]
    t0 = time.monotonic()
    proc = subprocess.Popen(full, cwd=os.getcwd(), start_new_session=True,
                            env=os.environ | {"PYTHONPATH": str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")})
    killed = False
    try:
        while True:
            try:
                rc = proc.wait(timeout=1.0)
                break
            except subprocess.TimeoutExpired:
                # Stop 5 s early: the charge rounds wall time up to whole minutes.
                if max_wall is not None and time.monotonic() - t0 > max_wall - 5:
                    print(f"snowflake_budget: KILLING: wall time {max_wall}s would cross the cap", file=sys.stderr, flush=True)
                    os.killpg(proc.pid, signal.SIGTERM)
                    killed = True
                    try:
                        rc = proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL)
                        rc = proc.wait()
                    break
    except KeyboardInterrupt:
        os.killpg(proc.pid, signal.SIGINT)
        rc = proc.wait()
    wall = time.monotonic() - t0
    credits = 0.0 if args.free else charge(wall, cfg)
    append({"kind": "run", "label": args.label, "cmd": " ".join(cmd)[:300], "cwd": os.getcwd(),
            "wall_s": round(wall, 1), "credits": round(credits, 5), "rc": rc, "killed": killed,
            "free": bool(args.free), "warehouse": None if cfg is None else cfg.get("size")})
    print(f"snowflake_budget: this command {credits:.3f} cr ({wall:.0f}s wall)")
    print(status_line())
    return rc if rc else (5 if killed else 0)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="snowflake_budget", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="action", required=True)
    r = sub.add_parser("run")
    r.add_argument("--estimate-min", type=float, default=1.0, help="expected warehouse minutes (default 1)")
    r.add_argument("--label", default=None)
    r.add_argument("--free", action="store_true", help="command cannot resume the warehouse (SHOW/DDL only)")
    r.add_argument("cmd", nargs=argparse.REMAINDER)
    r.set_defaults(fn=cmd_run)
    s = sub.add_parser("status")
    s.set_defaults(fn=cmd_status)
    c = sub.add_parser("config")
    c.add_argument("--size")
    c.add_argument("--auto-suspend", type=int)
    c.add_argument("--type")
    c.set_defaults(fn=cmd_config)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
