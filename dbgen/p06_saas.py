import duckdb, numpy as np, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


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
    
    account_ids = range(start, end)
    account_names = [f"Account {i}" for i in account_ids]
    selected_industries = np.take(industries, industry_indices).tolist()
    selected_countries = np.take(countries, country_indices).tolist()
    arrs_rounded = np.round(arrs, 2).tolist()
    created_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]")).tolist()
    selected_csm_ids = csm_ids.tolist()
    selected_health_scores = health_scores.tolist()
    
    return list(zip(account_ids, account_names, selected_industries, selected_countries, arrs_rounded, created_dates, selected_csm_ids, selected_health_scores))


def generate_subscriptions_chunk(start, end, NAC, plans, base):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    plan_indices = rng.integers(0, len(plans), size)
    seats = rng.integers(1, 201, size)
    mrrs = rng.uniform(99, 9999, size)
    days_offset = rng.integers(0, 601, size)
    active_probs = rng.random(size)

    sub_ids = range(start, end)
    selected_account_ids = account_ids.tolist()
    selected_plans = np.take(plans, plan_indices).tolist()
    selected_seats = seats.tolist()
    mrrs_rounded = np.round(mrrs, 2).tolist()
    
    start_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]"))
    end_dates = start_dates + np.timedelta64(365, 'D')
    
    is_active = (active_probs > 0.1).tolist()
    
    return list(zip(
        sub_ids,
        selected_account_ids,
        selected_plans,
        selected_seats,
        mrrs_rounded,
        start_dates.tolist(),
        end_dates.tolist(),
        is_active,
        end_dates.tolist()
    ))


def generate_events_chunk(start, end, NAC, etypes, bts):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    user_ids = rng.integers(1, NAC * 5 + 1, size)
    etype_indices = rng.integers(0, len(etypes), size)
    seconds_offset = rng.integers(0, 700 * 86400 + 1, size)
    session_ids_raw = rng.integers(1, NAC * 20 + 1, size)
    platforms = ["web", "mobile", "api"]
    platform_indices = rng.integers(0, len(platforms), size)

    event_ids = range(start, end)
    selected_account_ids = account_ids.tolist()
    selected_user_ids = user_ids.tolist()
    selected_etypes = np.take(etypes, etype_indices).tolist()
    event_tss = (np.datetime64(bts) + seconds_offset.astype("timedelta64[s]")).tolist()
    session_ids = [f"sess_{sid}" for sid in session_ids_raw]
    selected_platforms = np.take(platforms, platform_indices).tolist()
    
    return list(zip(event_ids, selected_account_ids, selected_user_ids, selected_etypes, event_tss, session_ids, selected_platforms))


def generate_feature_usage_chunk(start, end, NAC, features, base):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NAC + 1, size)
    feature_indices = rng.integers(0, len(features), size)
    days_offset = rng.integers(0, 701, size)
    usage_counts = rng.integers(1, 1001, size)

    fu_ids = range(start, end)
    selected_account_ids = account_ids.tolist()
    selected_features = np.take(features, feature_indices).tolist()
    usage_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]")).tolist()
    selected_usage_counts = usage_counts.tolist()
    
    return list(zip(fu_ids, selected_account_ids, selected_features, usage_dates, selected_usage_counts))


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

    ticket_ids = range(start, end)
    selected_account_ids = account_ids.tolist()
    created_tss = (np.datetime64(bts) + created_offsets.astype("timedelta64[s]")).tolist()
    
    resolved_tss = [
        (np.datetime64(bts) + np.timedelta64(off, 's')).tolist() if prob > 0.2 else None 
        for off, prob in zip(resolved_offsets, resolved_probs)
    ]
    # Correction on resolved_tss:’tolist()’ on numpy.datetime64 is fine, but here we are in a list comprehension.
    # Just use the numpy datetime64 and it will be converted to python datetime by duckdb
    resolved_tss = [
        (np.datetime64(bts) + np.timedelta64(int(off), 's')) if prob > 0.2 else None 
        for off, prob in zip(resolved_offsets, resolved_probs)
    ]
    
    selected_priorities = np.take(priorities, priority_indices).tolist()
    selected_cats = np.take(ticket_cats, cat_indices).tolist()
    
    csat_values = [
        int(score) if prob > 0.3 else None 
        for score, prob in zip(csat_scores, csat_probs)
    ]
    is_resolved = (is_resolved_probs > 0.2).tolist()
    
    return list(zip(ticket_ids, selected_account_ids, created_tss, resolved_tss, selected_priorities, selected_cats, csat_values, is_resolved))


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 800.0
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

    cpu_count = get_worker_count()
    progress = GenerationProgress("p06_saas", 5)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("accounts")
        batched_insert(con, "accounts", ['account_id', 'name', 'industry', 'country', 'arr', 'created_date', 'csm_id', 'health_score'],
                       run_parallel(executor, generate_accounts_chunk, NAC, industries, base))
        progress.advance("subscriptions")
        batched_insert(con, "subscriptions", ['sub_id', 'account_id', 'plan', 'seats', 'mrr', 'start_date', 'end_date', 'is_active', 'renewal_date'],
                       run_parallel(executor, generate_subscriptions_chunk, NSB, NAC, plans, base))
        progress.advance("events")
        batched_insert(con, "events", ['event_id', 'account_id', 'user_id', 'event_type', 'event_ts', 'session_id', 'platform'],
                       run_parallel(executor, generate_events_chunk, NEV, NAC, etypes, bts))
        progress.advance("feature_usage")
        batched_insert(con, "feature_usage", ['fu_id', 'account_id', 'feature_name', 'usage_date', 'usage_count'],
                       run_parallel(executor, generate_feature_usage_chunk, NFU, NAC, features, base))
        progress.advance("support_tickets")
        batched_insert(con, "support_tickets", ['ticket_id', 'account_id', 'created_ts', 'resolved_ts', 'priority', 'category', 'csat_score', 'is_resolved'],
                       run_parallel(executor, generate_support_tickets_chunk, NST, NAC, bts, priorities, ticket_cats))


    con.close()
    print_generation_summary(
        "p06_saas",
        sf,
        {
            "accounts": NAC,
            "subscriptions": NSB,
            "events": NEV,
            "feature_usage": NFU,
            "support_tickets": NST,
        },
    )

if __name__ == "__main__":
    main()
