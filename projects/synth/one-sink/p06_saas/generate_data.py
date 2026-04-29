import pyarrow as pa
import duckdb, random, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor

def generate_accounts_chunk(start, end, industries, base):
    return [
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
        for i in range(start, end)
    ]

def generate_subscriptions_chunk(start, end, NAC, plans, base):
    rows = []
    for i in range(start, end):
        sd = base + timedelta(days=random.randint(0, 600))
        rows.append((
            i,
            random.randint(1, NAC),
            random.choice(plans),
            random.randint(1, 200),
            round(random.uniform(99, 9999), 2),
            sd,
            sd + timedelta(days=365),
            random.random() > 0.1,
            sd + timedelta(days=365),
        ))
    return rows

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

def generate_support_tickets_chunk(start, end, NAC, bts, priorities, ticket_cats):
    return [
        (
            i,
            random.randint(1, NAC),
            bts + timedelta(seconds=random.randint(0, 700 * 86400)),
            bts + timedelta(seconds=random.randint(0, 700 * 86400)) if random.random() > 0.2 else None,
            random.choice(priorities),
            random.choice(ticket_cats),
            random.randint(1, 5) if random.random() > 0.3 else None,
            random.random() > 0.2,
        )
        for i in range(start, end)
    ]

def batched_insert(con, table_name, columns, rows):
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")

def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf *= 100
    NAC = max(10, int(500 * sf))
    NSB = max(10, int(700 * sf))
    NEV = max(100, int(50000 * sf))
    NFU = max(20, int(5000 * sf))
    NST = max(10, int(2000 * sf))

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

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

    base = date(2022, 1, 1)
    bts = datetime(2022, 1, 1)
    industries = ["fintech", "healthtech", "edtech", "ecommerce", "manufacturing", "media"]
    plans = ["starter", "growth", "enterprise", "enterprise_plus"]
    etypes = ["login", "page_view", "feature_click", "export", "api_call", "report_view"]
    features = ["dashboard", "reports", "api", "integrations", "automations", "analytics", "exports"]
    priorities = ["low", "medium", "high", "critical"]
    ticket_cats = ["billing", "technical", "feature_request", "onboarding", "other"]

    cpu_count = min(4, os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        def run_parallel(gen_func, total, *args):
            chunk_size = max(1, total // cpu_count)
            futures = []
            for i in range(0, total, chunk_size):
                futures.append(executor.submit(gen_func, i + 1, min(i + chunk_size + 1, total + 1), *args))
            rows = []
            for f in futures:
                rows.extend(f.result())
            return rows

        batched_insert(con, "accounts", ['account_id', 'name', 'industry', 'country', 'arr', 'created_date', 'csm_id', 'health_score'],
                       run_parallel(generate_accounts_chunk, NAC, industries, base))
        batched_insert(con, "subscriptions", ['sub_id', 'account_id', 'plan', 'seats', 'mrr', 'start_date', 'end_date', 'is_active', 'renewal_date'],
                       run_parallel(generate_subscriptions_chunk, NSB, NAC, plans, base))
        batched_insert(con, "events", ['event_id', 'account_id', 'user_id', 'event_type', 'event_ts', 'session_id', 'platform'],
                       run_parallel(generate_events_chunk, NEV, NAC, etypes, bts))
        batched_insert(con, "feature_usage", ['fu_id', 'account_id', 'feature_name', 'usage_date', 'usage_count'],
                       run_parallel(generate_feature_usage_chunk, NFU, NAC, features, base))
        batched_insert(con, "support_tickets", ['ticket_id', 'account_id', 'created_ts', 'resolved_ts', 'priority', 'category', 'csat_score', 'is_resolved'],
                       run_parallel(generate_support_tickets_chunk, NST, NAC, bts, priorities, ticket_cats))

    con.close()
    print(f"p06 done: accounts={NAC} events={NEV} tickets={NST}")

if __name__ == "__main__":
    main()
