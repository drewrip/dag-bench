import duckdb, sys, os
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_accounts_chunk(start, end, industries, base):
    rng = np.random.default_rng(start)
    size = end - start
    ind_idx = rng.integers(0, len(industries), size)
    country_idx = rng.integers(0, 5, size)
    arr_rand = rng.uniform(5000, 500000, size)
    days_rand = rng.integers(0, 701, size)
    csm_rand = rng.integers(1, 21, size)
    health_rand = rng.integers(1, 101, size)
    countries = ["US", "UK", "DE", "FR", "CA"]
    return [
        (
            i,
            f"Account {i}",
            industries[ind_idx[i - start]],
            countries[country_idx[i - start]],
            round(float(arr_rand[i - start]), 2),
            base + timedelta(days=int(days_rand[i - start])),
            int(csm_rand[i - start]),
            int(health_rand[i - start]),
        )
        for i in range(start, end)
    ]


def generate_subscriptions_chunk(start, end, NAC, plans, base):
    rng = np.random.default_rng(start)
    size = end - start
    acc_rand = rng.integers(1, NAC + 1, size)
    plan_idx = rng.integers(0, len(plans), size)
    seat_rand = rng.integers(1, 201, size)
    mrr_rand = rng.uniform(99, 9999, size)
    sd_days = rng.integers(0, 601, size)
    active_rand = rng.random(size)
    return [
        (
            i,
            int(acc_rand[i - start]),
            plans[plan_idx[i - start]],
            int(seat_rand[i - start]),
            round(float(mrr_rand[i - start]), 2),
            (sd := base + timedelta(days=int(sd_days[i - start]))),
            sd + timedelta(days=365),
            bool(active_rand[i - start] > 0.1),
        )
        for i in range(start, end)
    ]


def generate_events_chunk(start, end, NAC, etypes, bts):
    rng = np.random.default_rng(start)
    size = end - start
    acc_rand = rng.integers(1, NAC + 1, size)
    user_rand = rng.integers(1, NAC * 5 + 1, size)
    etype_idx = rng.integers(0, len(etypes), size)
    sec_rand = rng.integers(0, 700 * 86400 + 1, size)
    sess_rand = rng.integers(1, NAC * 20 + 1, size)
    plat_idx = rng.integers(0, 3, size)
    platforms = ["web", "mobile", "api"]
    return [
        (
            i,
            int(acc_rand[i - start]),
            int(user_rand[i - start]),
            etypes[etype_idx[i - start]],
            bts + timedelta(seconds=int(sec_rand[i - start])),
            f"sess_{sess_rand[i - start]}",
            platforms[plat_idx[i - start]],
        )
        for i in range(start, end)
    ]


def generate_feature_usage_chunk(start, end, NAC, features, base):
    rng = np.random.default_rng(start)
    size = end - start
    acc_rand = rng.integers(1, NAC + 1, size)
    feat_idx = rng.integers(0, len(features), size)
    days_rand = rng.integers(0, 701, size)
    usage_rand = rng.integers(1, 1001, size)
    return [
        (
            i,
            int(acc_rand[i - start]),
            features[feat_idx[i - start]],
            base + timedelta(days=int(days_rand[i - start])),
            int(usage_rand[i - start]),
        )
        for i in range(start, end)
    ]


def generate_support_tickets_chunk(start, end, NAC, bts, priorities, tcats):
    rng = np.random.default_rng(start)
    size = end - start
    acc_rand = rng.integers(1, NAC + 1, size)
    created_sec = rng.integers(0, 700 * 86400 + 1, size)
    resolved_sec = rng.integers(0, 700 * 86400 + 1, size)
    res_prob = rng.random(size)
    prio_idx = rng.integers(0, len(priorities), size)
    cat_idx = rng.integers(0, len(tcats), size)
    csat_prob = rng.random(size)
    csat_rand = rng.integers(1, 6, size)
    resolved_rand = rng.random(size)
    return [
        (
            i,
            int(acc_rand[i - start]),
            bts + timedelta(seconds=int(created_sec[i - start])),
            bts + timedelta(seconds=int(resolved_sec[i - start]))
            if res_prob[i - start] > 0.2
            else None,
            priorities[prio_idx[i - start]],
            tcats[cat_idx[i - start]],
            int(csat_rand[i - start]) if csat_prob[i - start] > 0.3 else None,
            bool(resolved_rand[i - start] > 0.2),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    NAC, NSB, NEV, NFU, NST = (
        max(a, int(b * sf_adj))
        for a, b in [(10, 500), (10, 700), (100, 50000), (20, 5000), (10, 2000)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS support_tickets; DROP TABLE IF EXISTS feature_usage;
    DROP TABLE IF EXISTS events; DROP TABLE IF EXISTS subscriptions; DROP TABLE IF EXISTS accounts;
    CREATE TABLE accounts(account_id INTEGER PRIMARY KEY,name VARCHAR,industry VARCHAR,
      country VARCHAR,arr DECIMAL(12,2),created_date DATE,csm_id INTEGER,health_score TINYINT);
    CREATE TABLE subscriptions(sub_id INTEGER PRIMARY KEY,account_id INTEGER,plan VARCHAR,
      seats INTEGER,mrr DECIMAL(10,2),start_date DATE,end_date DATE,is_active BOOLEAN);
    CREATE TABLE events(event_id BIGINT PRIMARY KEY,account_id INTEGER,user_id INTEGER,
      event_type VARCHAR,event_ts TIMESTAMP,session_id VARCHAR,platform VARCHAR);
    CREATE TABLE feature_usage(fu_id INTEGER PRIMARY KEY,account_id INTEGER,feature_name VARCHAR,
      usage_date DATE,usage_count INTEGER);
    CREATE TABLE support_tickets(ticket_id INTEGER PRIMARY KEY,account_id INTEGER,
      created_ts TIMESTAMP,resolved_ts TIMESTAMP,priority VARCHAR,category VARCHAR,
      csat_score TINYINT,is_resolved BOOLEAN);
    """)
    base = date(2022, 1, 1)
    bts = datetime(2022, 1, 1)
    industries = ["fintech", "healthtech", "edtech", "ecommerce", "manufacturing"]
    plans = ["starter", "growth", "enterprise", "enterprise_plus"]
    etypes = ["login", "page_view", "feature_click", "export", "api_call"]
    features = [
        "dashboard",
        "reports",
        "api",
        "integrations",
        "automations",
        "analytics",
    ]
    priorities = ["low", "medium", "high", "critical"]
    tcats = ["billing", "technical", "feature_request", "onboarding", "other"]

    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "accounts",
            [
                "account_id",
                "name",
                "industry",
                "country",
                "arr",
                "created_date",
                "csm_id",
                "health_score",
            ],
            run_parallel(executor, generate_accounts_chunk, NAC, industries, base),
        )

        batched_insert(
            con,
            "subscriptions",
            [
                "sub_id",
                "account_id",
                "plan",
                "seats",
                "mrr",
                "start_date",
                "end_date",
                "is_active",
            ],
            run_parallel(executor, generate_subscriptions_chunk, NSB, NAC, plans, base),
        )

        batched_insert(
            con,
            "events",
            [
                "event_id",
                "account_id",
                "user_id",
                "event_type",
                "event_ts",
                "session_id",
                "platform",
            ],
            run_parallel(executor, generate_events_chunk, NEV, NAC, etypes, bts),
        )

        batched_insert(
            con,
            "feature_usage",
            ["fu_id", "account_id", "feature_name", "usage_date", "usage_count"],
            run_parallel(
                executor, generate_feature_usage_chunk, NFU, NAC, features, base
            ),
        )

        batched_insert(
            con,
            "support_tickets",
            [
                "ticket_id",
                "account_id",
                "created_ts",
                "resolved_ts",
                "priority",
                "category",
                "csat_score",
                "is_resolved",
            ],
            run_parallel(
                executor,
                generate_support_tickets_chunk,
                NST,
                NAC,
                bts,
                priorities,
                tcats,
            ),
        )

    con.close()
    print(f"p06 done accounts={NAC} events={NEV}")


if __name__ == "__main__":
    main()
