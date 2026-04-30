import duckdb, sys, os
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_accounts_chunk(start, end, atypes, base):
    rng = np.random.default_rng(start)
    size = end - start
    atypes_idx = rng.integers(0, len(atypes), size)
    country_idx = rng.integers(0, 5, size)
    limit_rand = rng.uniform(500, 50000, size)
    days_rand = rng.integers(30, 3651, size)
    frozen_rand = rng.random(size)
    countries = ["US", "GB", "DE", "FR", "CA"]
    return [
        (
            i,
            f"Holder {i}",
            atypes[atypes_idx[i - start]],
            countries[country_idx[i - start]],
            round(float(limit_rand[i - start]), 2),
            (base - timedelta(days=int(days_rand[i - start]))).date(),
            bool(frozen_rand[i - start] < 0.03),
        )
        for i in range(start, end)
    ]

def generate_merchants_chunk(start, end, cats, risks):
    rng = np.random.default_rng(start)
    size = end - start
    cats_idx = rng.integers(0, len(cats), size)
    country_idx = rng.integers(0, 5, size)
    risks_idx = rng.integers(0, len(risks), size)
    amount_rand = rng.uniform(5, 500, size)
    countries = ["US", "GB", "NG", "CN", "RU"]
    return [
        (
            i,
            f"Merchant {i}",
            cats[cats_idx[i - start]],
            countries[country_idx[i - start]],
            risks[risks_idx[i - start]],
            round(float(amount_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]

def generate_transactions_chunk(start, end, NA, NM, base, chans, currs, rcodes):
    rng = np.random.default_rng(start)
    size = end - start
    acc_rand = rng.integers(1, NA + 1, size)
    merch_rand = rng.integers(1, NM + 1, size)
    amount_rand = rng.uniform(1, 5000, size)
    sec_rand = rng.integers(0, 365 * 86400 + 1, size)
    chan_idx = rng.integers(0, len(chans), size)
    curr_idx = rng.integers(0, len(currs), size)
    dec_rand = rng.random(size)
    flag_rand = rng.random(size)
    rcode_idx = rng.integers(0, len(rcodes), size)
    return [
        (
            i,
            int(acc_rand[i - start]),
            int(merch_rand[i - start]),
            round(float(amount_rand[i - start]), 2),
            base + timedelta(seconds=int(sec_rand[i - start])),
            chans[chan_idx[i - start]],
            currs[curr_idx[i - start]],
            bool(dec_rand[i - start] < 0.05),
            bool(flag_rand[i - start] < 0.04), # is_flagged
            rcodes[rcode_idx[i - start]],
        )
        for i in range(start, end)
    ]

def generate_alerts_chunk(start, end, flagged_ids, atypes2, sevs, base, ress):
    rng = np.random.default_rng(start)
    size = end - start
    if flagged_ids:
        flag_idx = rng.integers(0, len(flagged_ids), size)
    type2_idx = rng.integers(0, len(atypes2), size)
    sev_idx = rng.integers(0, len(sevs), size)
    sec_rand = rng.integers(0, 365 * 86400 + 1, size)
    res_prob = rng.random(size)
    res_idx = rng.integers(0, len(ress), size)
    res_prob2 = rng.random(size)
    return [
        (
            i,
            int(flagged_ids[flag_idx[i - start]]) if flagged_ids else i,
            atypes2[type2_idx[i - start]],
            sevs[sev_idx[i - start]],
            base + timedelta(seconds=int(sec_rand[i - start])),
            bool(res_prob[i - start] > 0.4),
            ress[res_idx[i - start]] if res_prob2[i - start] > 0.4 else None,
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 100.0
    NA, NM, NT, NAL = (
        max(a, int(b * sf_adj)) for a, b in [(10, 1000), (10, 300), (50, 20000), (5, 500)]
    )

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS alerts; DROP TABLE IF EXISTS transactions;
    DROP TABLE IF EXISTS merchants; DROP TABLE IF EXISTS accounts;
    CREATE TABLE accounts(account_id INTEGER PRIMARY KEY,holder_name VARCHAR,
      account_type VARCHAR,country VARCHAR,credit_limit DECIMAL(12,2),opened_date DATE,is_frozen BOOLEAN);
    CREATE TABLE merchants(merchant_id INTEGER PRIMARY KEY,name VARCHAR,category VARCHAR,
      country VARCHAR,risk_tier VARCHAR,avg_txn_amount DECIMAL(10,2));
    CREATE TABLE transactions(txn_id INTEGER PRIMARY KEY,account_id INTEGER,merchant_id INTEGER,
      amount DECIMAL(12,2),txn_ts TIMESTAMP,channel VARCHAR,currency VARCHAR,
      is_declined BOOLEAN,is_flagged BOOLEAN,response_code VARCHAR);
    CREATE TABLE alerts(alert_id INTEGER PRIMARY KEY,txn_id INTEGER,alert_type VARCHAR,
      severity VARCHAR,created_ts TIMESTAMP,resolved BOOLEAN,resolution VARCHAR);
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

    cpu_count = os.cpu_count()
    
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "accounts", ['account_id', 'holder_name', 'account_type', 'country', 'credit_limit', 'opened_date', 'is_frozen'], 
                       run_parallel(executor, generate_accounts_chunk, NA, atypes, base))
        
        batched_insert(con, "merchants", ['merchant_id', 'name', 'category', 'country', 'risk_tier', 'avg_txn_amount'],
                       run_parallel(executor, generate_merchants_chunk, NM, cats, risks))
        
        txns = run_parallel(executor, generate_transactions_chunk, NT, NA, NM, base, chans, currs, rcodes)
        batched_insert(con, "transactions", ['txn_id', 'account_id', 'merchant_id', 'amount', 'txn_ts', 'channel', 'currency', 'is_declined', 'is_flagged', 'response_code'], txns)
        
        flagged_ids = [t[0] for t in txns if t[8]]
        batched_insert(con, "alerts", ['alert_id', 'txn_id', 'alert_type', 'severity', 'created_ts', 'resolved', 'resolution'],
                       run_parallel(executor, generate_alerts_chunk, NAL, flagged_ids, atypes2, sevs, base, ress))


    con.close()
    print(f"p02 done accounts={NA} txns={NT}")

if __name__ == "__main__":
    main()
