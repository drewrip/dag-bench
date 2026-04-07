import csv, duckdb, random, sys, os, tempfile
from datetime import datetime, timedelta, date

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NAC = max(10, int(500 * sf))
NSB = max(10, int(700 * sf))
NEV = max(100, int(50000 * sf))
NFU = max(20, int(5000 * sf))
NST = max(10, int(2000 * sf))

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")

def batched_insert(sql, rows):
    rows = list(rows)
    if not rows:
        return
    table_name = sql.split()[2]
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False) as tmp:
        csv.writer(tmp).writerows(rows)
        temp_path = tmp.name
    try:
        con.execute(f"COPY {table_name} FROM '{temp_path}' (FORMAT CSV)")
    finally:
        os.unlink(temp_path)

con.execute("""
DROP TABLE IF EXISTS support_tickets; DROP TABLE IF EXISTS feature_usage;
DROP TABLE IF EXISTS events; DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts(account_id INTEGER PRIMARY KEY, name VARCHAR,
    industry VARCHAR, country VARCHAR, arr DECIMAL(12,2),
    created_date DATE, csm_id INTEGER, health_score TINYINT);
CREATE TABLE subscriptions(sub_id INTEGER PRIMARY KEY, account_id INTEGER,
    plan VARCHAR, seats INTEGER, mrr DECIMAL(10,2),
    start_date DATE, end_date DATE, is_active BOOLEAN, renewal_date DATE);
CREATE TABLE events(event_id BIGINT PRIMARY KEY, account_id INTEGER,
    user_id INTEGER, event_type VARCHAR, event_ts TIMESTAMP,
    session_id VARCHAR, platform VARCHAR);
CREATE TABLE feature_usage(fu_id INTEGER PRIMARY KEY, account_id INTEGER,
    feature_name VARCHAR, usage_date DATE, usage_count INTEGER);
CREATE TABLE support_tickets(ticket_id INTEGER PRIMARY KEY, account_id INTEGER,
    created_ts TIMESTAMP, resolved_ts TIMESTAMP, priority VARCHAR,
    category VARCHAR, csat_score TINYINT, is_resolved BOOLEAN);
""")
con.execute("BEGIN")

base = date(2022, 1, 1)
bts = datetime(2022, 1, 1)
industries = ["fintech", "healthtech", "edtech", "ecommerce", "manufacturing", "media"]
plans = ["starter", "growth", "enterprise", "enterprise_plus"]
etypes = ["login", "page_view", "feature_click", "export", "api_call", "report_view"]
features = [
    "dashboard",
    "reports",
    "api",
    "integrations",
    "automations",
    "analytics",
    "exports",
]
priorities = ["low", "medium", "high", "critical"]
ticket_cats = ["billing", "technical", "feature_request", "onboarding", "other"]

batched_insert(
    "INSERT INTO accounts VALUES(?,?,?,?,?,?,?,?)",
    [
        (
            i,
            f"Account {i}",
            random.choice(industries),
            random.choice(["US", "UK", "DE", "FR", "CA", "AU"]),
            round(random.uniform(5000, 500000), 2),
            base + timedelta(days=random.randint(0, 700)),
            random.randint(1, 20),
            random.randint(1, 100),
        )
        for i in range(1, NAC + 1)
    ],
)
batched_insert(
    "INSERT INTO subscriptions VALUES(?,?,?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NAC),
            random.choice(plans),
            random.randint(1, 200),
            round(random.uniform(99, 9999), 2),
            sd := base + timedelta(days=random.randint(0, 600)),
            sd + timedelta(days=365),
            random.random() > 0.1,
            sd + timedelta(days=365),
        )
        for i in range(1, NSB + 1)
    ],
)
batched_insert(
    "INSERT INTO events VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NAC),
            random.randint(1, NAC * 5),
            random.choice(etypes),
            bts + timedelta(seconds=random.randint(0, 700 * 86400)),
            f"sess_{random.randint(1, NAC * 20)}",
            random.choice(["web", "mobile", "api"]),
        )
        for i in range(1, NEV + 1)
    ],
)
batched_insert(
    "INSERT INTO feature_usage VALUES(?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NAC),
            random.choice(features),
            base + timedelta(days=random.randint(0, 700)),
            random.randint(1, 1000),
        )
        for i in range(1, NFU + 1)
    ],
)
batched_insert(
    "INSERT INTO support_tickets VALUES(?,?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NAC),
            bts + timedelta(seconds=random.randint(0, 700 * 86400)),
            bts + timedelta(seconds=random.randint(0, 700 * 86400))
            if random.random() > 0.2
            else None,
            random.choice(priorities),
            random.choice(ticket_cats),
            random.randint(1, 5) if random.random() > 0.3 else None,
            random.random() > 0.2,
        )
        for i in range(1, NST + 1)
    ],
)
con.commit()
con.close()
print(f"p06 done: accounts={NAC} events={NEV} tickets={NST}")
