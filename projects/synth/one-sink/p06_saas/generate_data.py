import duckdb, numpy as np, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_accounts_chunk(start, end, industries, base):
    size = end - start
    rng = np.random.default_rng(start)
    industry_indices = rng.integers(0, len(industries), size)
    countries = ["US", "UK", "DE", "FR", "CA", "AU"]
    country_indices = rng.integers(0, len(countries), size)
    arrs = rng.uniform(5000, 500000, size)
    days_offset = rng.integers(0, 701, size)
    csm_ids = rng.integers(1, 21, size)
    health_scores = rng.integers(1, 101, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Account {i}",
            industries[industry_indices[idx]],
            countries[country_indices[idx]],
            round(float(arrs[idx]), 2),
            base + timedelta(days=int(days_offset[idx])),
            int(csm_ids[idx]),
            int(health_scores[idx]),
        ))
    return rows

def generate_subscriptions_chunk(start, end, NAC, plans, base):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    plan_indices = rng.integers(0, len(plans), size)
    seats = rng.integers(1, 201, size)
    mrrs = rng.uniform(99, 9999, size)
    days_offset = rng.integers(0, 601, size)
    active_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        sd = base + timedelta(days=int(days_offset[idx]))
        rows.append((
            i,
            int(account_ids[idx]),
            plans[plan_indices[idx]],
            int(seats[idx]),
            round(float(mrrs[idx]), 2),
            sd,
            sd + timedelta(days=365),
            bool(active_probs[idx] > 0.1),
            sd + timedelta(days=365),
        ))
    return rows

def generate_events_chunk(start, end, NAC, etypes, bts):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    user_ids = rng.integers(1, NAC * 5 + 1, size)
    etype_indices = rng.integers(0, len(etypes), size)
    seconds_offset = rng.integers(0, 700 * 86400 + 1, size)
    session_ids = rng.integers(1, NAC * 20 + 1, size)
    platforms = ["web", "mobile", "api"]
    platform_indices = rng.integers(0, len(platforms), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(account_ids[idx]),
            int(user_ids[idx]),
            etypes[etype_indices[idx]],
            bts + timedelta(seconds=int(seconds_offset[idx])),
            f"sess_{session_ids[idx]}",
            platforms[platform_indices[idx]],
        ))
    return rows

def generate_feature_usage_chunk(start, end, NAC, features, base):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    feature_indices = rng.integers(0, len(features), size)
    days_offset = rng.integers(0, 701, size)
    usage_counts = rng.integers(1, 1001, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(account_ids[idx]),
            features[feature_indices[idx]],
            base + timedelta(days=int(days_offset[idx])),
            int(usage_counts[idx]),
        ))
    return rows

def generate_support_tickets_chunk(start, end, NAC, bts, priorities, ticket_cats):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    created_offsets = rng.integers(0, 700 * 86400 + 1, size)
    resolved_offsets = rng.integers(0, 700 * 86400 + 1, size)
    resolved_probs = rng.random(size)
    priority_indices = rng.integers(0, len(priorities), size)
    cat_indices = rng.integers(0, len(ticket_cats), size)
    csat_scores = rng.integers(1, 6, size)
    csat_probs = rng.random(size)
    is_resolved_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        created_ts = bts + timedelta(seconds=int(created_offsets[idx]))
        resolved_ts = bts + timedelta(seconds=int(resolved_offsets[idx])) if resolved_probs[idx] > 0.2 else None
        rows.append((
            i,
            int(account_ids[idx]),
            created_ts,
            resolved_ts,
            priorities[priority_indices[idx]],
            ticket_cats[cat_indices[idx]],
            int(csat_scores[idx]) if csat_probs[idx] > 0.3 else None,
            bool(is_resolved_probs[idx] > 0.2),
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 100.0
    NAC = max(10, int(500 * sf_adj))
    NSB = max(10, int(700 * sf_adj))
    NEV = max(100, int(50000 * sf_adj))
    NFU = max(20, int(5000 * sf_adj))
    NST = max(10, int(2000 * sf_adj))


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

    cpu_count = os.cpu_count()
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "accounts", ['account_id', 'name', 'industry', 'country', 'arr', 'created_date', 'csm_id', 'health_score'],
                       run_parallel(executor, generate_accounts_chunk, NAC, industries, base))
        batched_insert(con, "subscriptions", ['sub_id', 'account_id', 'plan', 'seats', 'mrr', 'start_date', 'end_date', 'is_active', 'renewal_date'],
                       run_parallel(executor, generate_subscriptions_chunk, NSB, NAC, plans, base))
        batched_insert(con, "events", ['event_id', 'account_id', 'user_id', 'event_type', 'event_ts', 'session_id', 'platform'],
                       run_parallel(executor, generate_events_chunk, NEV, NAC, etypes, bts))
        batched_insert(con, "feature_usage", ['fu_id', 'account_id', 'feature_name', 'usage_date', 'usage_count'],
                       run_parallel(executor, generate_feature_usage_chunk, NFU, NAC, features, base))
        batched_insert(con, "support_tickets", ['ticket_id', 'account_id', 'created_ts', 'resolved_ts', 'priority', 'category', 'csat_score', 'is_resolved'],
                       run_parallel(executor, generate_support_tickets_chunk, NST, NAC, bts, priorities, ticket_cats))


    con.close()
    print(f"p06 done: accounts={NAC} events={NEV} tickets={NST}")

if __name__ == "__main__":
    main()
