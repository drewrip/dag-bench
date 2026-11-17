#!/usr/bin/env python3
"""Snowflake preflight: check the account without warehouse compute.

  python -m utils.snowflake_budget run --free --label preflight -- python -m utils.snowflake_preflight
  ... -- python -m utils.snowflake_preflight --set-user-timeouts   # also ALTER USER (see below)

Only SHOW / DDL statements, which run on cloud services and never resume the
warehouse:

- SHOW WAREHOUSES: size, type, auto_suspend, state, cluster counts. Writes a
  ledger `config` record for utils.snowflake_budget;
- SHOW PARAMETERS for statement timeouts (user and warehouse);
- SHOW GRANTS TO ROLE;
- CREATE / DROP SCHEMA CMU.DAGBENCH_PREFLIGHT (DDL rights);
- with --set-user-timeouts: ALTER USER <me> SET STATEMENT_TIMEOUT_IN_SECONDS=120,
  STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=300. This is user-wide and outlives this
  work (it affects every session of the user), so it is opt-in. The dbt
  profiles and our scripts set the same limits per session anyway.

Gate: exits 3 when the warehouse is larger than SMALL or AUTO_SUSPEND > 300 s.
Whether WAREHOUSE_METERING_HISTORY is readable is not checked here: querying
it needs a running warehouse.
"""
from __future__ import annotations

import argparse
import os
import sys

from utils import snowflake_budget
from utils.snowflake_bench_utils import DATABASE, connect

WAREHOUSE = os.environ.get("DAGBENCH_SF_WAREHOUSE", "CMU_ROBOTS")
ALLOWED_SIZES = {"X-SMALL", "XSMALL", "SMALL"}
MAX_SUSPEND = 300


def rows(cur, sql: str) -> list[dict]:
    cur.execute(sql)
    names = [d[0].lower() for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set-user-timeouts", action="store_true")
    args = ap.parse_args(argv)

    con = connect()
    cur = con.cursor()
    ok = True
    try:
        who = rows(cur, "show parameters like 'QUERY_TAG' in session")  # proves the login works
        print(f"login: ok (session query_tag={who[0]['value'] if who else '?'})")

        wh = rows(cur, f"show warehouses like '{WAREHOUSE}'")
        if not wh:
            print(f"warehouse {WAREHOUSE}: NOT VISIBLE to this role")
            return 3
        w = wh[0]
        size = str(w.get("size", "")).upper()
        suspend = int(w.get("auto_suspend") or 0)
        wtype = str(w.get("type", "STANDARD")).upper()
        print(f"warehouse {WAREHOUSE}: size={w.get('size')} type={wtype} state={w.get('state')} "
              f"auto_suspend={w.get('auto_suspend')} auto_resume={w.get('auto_resume')} "
              f"clusters={w.get('min_cluster_count')}-{w.get('max_cluster_count')} "
              f"max_concurrency_level={w.get('max_concurrency_level', '?')} "
              f"resource_monitor={w.get('resource_monitor')} owner={w.get('owner')}")
        if not suspend:
            print("  WARNING: auto_suspend is NULL/0 (never suspends); the ledger assumes 3600 s")
        snowflake_budget.append({"kind": "config", "size": size, "auto_suspend": suspend or 3600,
                                 "type": wtype, "warehouse": WAREHOUSE, "source": "preflight"})
        gate_ok = size.replace(" ", "") in ALLOWED_SIZES and 0 < suspend <= MAX_SUSPEND
        print(f"  gate (<= SMALL and auto_suspend <= {MAX_SUSPEND}s): {'PASS' if gate_ok else 'FAIL'}")
        ok &= gate_ok

        for scope in ("user", f"warehouse {WAREHOUSE}"):
            try:
                for p in rows(cur, f"show parameters like 'STATEMENT%TIMEOUT%' in {scope}"):
                    print(f"param {scope}: {p['key']}={p['value']} (level={p.get('level') or 'default'})")
            except Exception as exc:  # noqa: BLE001
                print(f"param {scope}: not readable ({type(exc).__name__})")

        grants: list[dict] = []
        role_name = con.role or "CMUADMIN"
        try:
            grants = rows(cur, f"show grants to role {role_name}")
        except Exception as exc:  # noqa: BLE001
            print(f"grants: not readable ({type(exc).__name__})")
        relevant = [g for g in grants if str(g.get("name", "")).upper().split(".")[0].strip('"') in {WAREHOUSE, DATABASE}
                    or str(g.get("granted_on", "")).upper() == "ACCOUNT"]
        for g in relevant:
            print(f"grant: {g.get('privilege')} on {g.get('granted_on')} {g.get('name')}")
        if grants and not relevant:
            print(f"grants: none directly on {WAREHOUSE}/{DATABASE} (may come via inherited roles)")

        try:
            cur.execute(f"create schema if not exists {DATABASE}.DAGBENCH_PREFLIGHT")
            cur.execute(f"drop schema {DATABASE}.DAGBENCH_PREFLIGHT")
            print(f"ddl: CREATE/DROP SCHEMA in {DATABASE}: ok")
        except Exception as exc:  # noqa: BLE001
            print(f"ddl: CREATE SCHEMA in {DATABASE} FAILED ({type(exc).__name__}: {str(exc)[:200]})")
            ok = False

        try:
            cur.execute("list @~/dagbench/")
            print("user stage: ok")
        except Exception as exc:  # noqa: BLE001
            print(f"user stage: FAILED ({type(exc).__name__})")
            ok = False

        if args.set_user_timeouts:
            try:
                cur.execute(f'alter user "{con.user.upper()}" set statement_timeout_in_seconds = 120, '
                            "statement_queued_timeout_in_seconds = 300")
                print("alter user timeouts: ok (user-wide)")
            except Exception as exc:  # noqa: BLE001
                print(f"alter user timeouts: not allowed ({type(exc).__name__}); per-session limits only")
    finally:
        cur.close()
        con.close()
    print("preflight:", "PASS" if ok else "FAIL (see above)")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
