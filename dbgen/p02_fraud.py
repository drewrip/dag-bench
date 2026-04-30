import duckdb, numpy as np, sys, os
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_accounts_chunk(start, end, atypes, base):
    size = end - start
    rng = np.random.default_rng(start)
    atype_indices = rng.integers(0, len(atypes), size)
    country_indices = rng.integers(0, 5, size)
    countries = ["US", "GB", "DE", "FR", "CA"]
    limits = rng.uniform(500, 50000, size)
    days_back = rng.integers(30, 3651, size)
    frozen_probs = rng.random(size)
    
    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Holder {i}",
            atypes[atype_indices[idx]],
            countries[country_indices[idx]],
            round(float(limits[idx]), 2),
            (base - timedelta(days=int(days_back[idx]))).date(),
            bool(frozen_probs[idx] < 0.03),
        ))
    return rows

def generate_merchants_chunk(start, end, cats, risks):
    size = end - start
    rng = np.random.default_rng(start)
    cat_indices = rng.integers(0, len(cats), size)
    country_indices = rng.integers(0, 5, size)
    countries = ["US", "GB", "NG", "CN", "RU"]
    risk_indices = rng.integers(0, len(risks), size)
    avg_amounts = rng.uniform(5, 500, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Merchant {i}",
            cats[cat_indices[idx]],
            countries[country_indices[idx]],
            risks[risk_indices[idx]],
            round(float(avg_amounts[idx]), 2),
        ))
    return rows

def generate_transactions_chunk(start, end, NA, NM, base, chans, currs, rcodes):
    size = end - start
    rng = np.random.default_rng(start)
    account_ids = rng.integers(1, NA + 1, size)
    merchant_ids = rng.integers(1, NM + 1, size)
    amounts = rng.uniform(1, 5000, size)
    seconds_offset = rng.integers(0, 365 * 86400 + 1, size)
    chan_indices = rng.integers(0, len(chans), size)
    curr_indices = rng.integers(0, len(currs), size)
    declined_probs = rng.random(size)
    flagged_probs = rng.random(size)
    rcode_indices = rng.integers(0, len(rcodes), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(account_ids[idx]),
            int(merchant_ids[idx]),
            round(float(amounts[idx]), 2),
            base + timedelta(seconds=int(seconds_offset[idx])),
            chans[chan_indices[idx]],
            currs[curr_indices[idx]],
            bool(declined_probs[idx] < 0.05),
            bool(flagged_probs[idx] < 0.04),
            rcodes[rcode_indices[idx]],
        ))
    return rows

def generate_alerts_chunk(start, end, flagged_ids, NT, atypes2, sevs, base, ress):
    size = end - start
    rng = np.random.default_rng(start)
    
    if flagged_ids:
        flagged_indices = rng.integers(0, len(flagged_ids), size)
    else:
        random_txn_ids = rng.integers(1, NT + 1, size)
        
    atype2_indices = rng.integers(0, len(atypes2), size)
    sev_indices = rng.integers(0, len(sevs), size)
    seconds_offset = rng.integers(0, 365 * 86400 + 1, size)
    resolved_probs = rng.random(size)
    res_probs = rng.random(size)
    res_indices = rng.integers(0, len(ress), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        txn_id = int(flagged_ids[flagged_indices[idx]]) if flagged_ids else int(random_txn_ids[idx])
        rows.append((
            i,
            txn_id,
            atypes2[atype2_indices[idx]],
            sevs[sev_indices[idx]],
            base + timedelta(seconds=int(seconds_offset[idx])),
            bool(resolved_probs[idx] > 0.4),
            ress[res_indices[idx]] if res_probs[idx] > 0.4 else None,
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 2200.0
    NA = max(10, int(1000 * sf_adj))
    NM = max(10, int(300 * sf_adj))
    NT = max(50, int(20000 * sf_adj))
    NAL = max(5, int(500 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS alerts; DROP TABLE IF EXISTS transactions;
    DROP TABLE IF EXISTS merchants; DROP TABLE IF EXISTS accounts;
    CREATE TABLE accounts(account_id INTEGER PRIMARY KEY, holder_name VARCHAR,
        account_type VARCHAR, country VARCHAR, credit_limit DECIMAL(12,2),
        opened_date DATE, is_frozen BOOLEAN);
    CREATE TABLE merchants(merchant_id INTEGER PRIMARY KEY, name VARCHAR,
        category VARCHAR, country VARCHAR, risk_tier VARCHAR,
        avg_txn_amount DECIMAL(10,2));
    CREATE TABLE transactions(txn_id INTEGER PRIMARY KEY, account_id INTEGER,
        merchant_id INTEGER, amount DECIMAL(12,2), txn_ts TIMESTAMP,
        channel VARCHAR, currency VARCHAR, is_declined BOOLEAN,
        is_flagged BOOLEAN, response_code VARCHAR);
    CREATE TABLE alerts(alert_id INTEGER PRIMARY KEY, txn_id INTEGER,
        alert_type VARCHAR, severity VARCHAR, created_ts TIMESTAMP,
        resolved BOOLEAN, resolution VARCHAR);
    """)

    base = datetime(2022, 1, 1)
    atypes = ["checking", "savings", "credit", "business"]
    cats = ["retail", "travel", "grocery", "online", "gaming", "crypto", "atm"]
    risks = ["low", "medium", "high", "critical"]
    chans = ["pos", "web", "mobile", "atm", "wire"]
    currs = ["USD", "EUR", "GBP", "JPY", "BTC"]
    rcodes = ["00", "01", "05", "14", "51", "57", "96"]
    atypes2 = ["velocity", "geo_anomaly", "amount_spike", "card_not_present", "identity"]
    sevs = ["info", "warning", "critical"]
    ress = ["confirmed_fraud", "false_positive", "under_review"]

    cpu_count = get_worker_count()
    progress = GenerationProgress("p02_fraud", 4)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("accounts")
        batched_insert(con, "accounts", ['account_id', 'holder_name', 'account_type', 'country', 'credit_limit', 'opened_date', 'is_frozen'],
                       run_parallel(executor, generate_accounts_chunk, NA, atypes, base))
        progress.advance("merchants")
        batched_insert(con, "merchants", ['merchant_id', 'name', 'category', 'country', 'risk_tier', 'avg_txn_amount'],
                       run_parallel(executor, generate_merchants_chunk, NM, cats, risks))
        
        progress.advance("transactions")
        txns = run_parallel(executor, generate_transactions_chunk, NT, NA, NM, base, chans, currs, rcodes)
        batched_insert(con, "transactions", ['txn_id', 'account_id', 'merchant_id', 'amount', 'txn_ts', 'channel', 'currency', 'is_declined', 'is_flagged', 'response_code'], txns)

        flagged_ids = [
            row[0]
            for row in con.execute(
                "SELECT txn_id FROM transactions WHERE is_flagged ORDER BY txn_id LIMIT ?",
                [NAL],
            ).fetchall()
        ]
        progress.advance("alerts")
        batched_insert(con, "alerts", ['alert_id', 'txn_id', 'alert_type', 'severity', 'created_ts', 'resolved', 'resolution'],
                       run_parallel(executor, generate_alerts_chunk, NAL, flagged_ids, NT, atypes2, sevs, base, ress))


    con.close()
    print_generation_summary(
        "p02_fraud",
        sf,
        {
            "accounts": NA,
            "merchants": NM,
            "transactions": NT,
            "alerts": NAL,
        },
    )

if __name__ == "__main__":
    main()
