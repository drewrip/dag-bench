import duckdb, random, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_accounts_chunk(start, end, industries, base):
    return [
        (
            i,
            f"Account {i}",
            random.choice(industries),
            random.choice(["US", "UK", "DE", "FR", "CA"]),
            round(random.uniform(5000, 500000), 2),
            base + timedelta(days=random.randint(0, 700)),
            random.randint(1, 20),
            random.randint(1, 100),
        )
        for i in range(start, end)
    ]


def generate_subscriptions_chunk(start, end, NAC, plans, base):
    return [
        (
            i,
            random.randint(1, NAC),
            random.choice(plans),
            random.randint(1, 200),
            round(random.uniform(99, 9999), 2),
            sd := base + timedelta(days=random.randint(0, 600)),
            sd + timedelta(days=365),
            random.random() > 0.1,
        )
        for i in range(start, end)
    ]


def generate_events_chunk(start, end, NAC, etypes, bts):
    return [
        (
            i,
            random.randint(1, NAC),
            random.randint(1, NAC * 5),
            random.choice(etypes),
            bts + timedelta(seconds=random.randint(0, 700 * 86400)),
            f"sess_{random.randint(1, NAC * 20)}",
            random.choice(["web", "mobile", "api"]),
        )
        for i in range(start, end)
    ]


def generate_feature_usage_chunk(start, end, NAC, features, base):
    return [
        (
            i,
            random.randint(1, NAC),
            random.choice(features),
            base + timedelta(days=random.randint(0, 700)),
            random.randint(1, 1000),
        )
        for i in range(start, end)
    ]


def generate_support_tickets_chunk(start, end, NAC, bts, priorities, tcats):
    return [
        (
            i,
            random.randint(1, NAC),
            bts + timedelta(seconds=random.randint(0, 700 * 86400)),
            bts + timedelta(seconds=random.randint(0, 700 * 86400))
            if random.random() > 0.2
            else None,
            random.choice(priorities),
            random.choice(tcats),
            random.randint(1, 5) if random.random() > 0.3 else None,
            random.random() > 0.2,
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

    cpu_count = min(4, os.cpu_count() or 1)

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
