import duckdb, random, sys, os
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_accounts_chunk(start, end, atypes, base):
    return [
        (
            i,
            f"Holder {i}",
            random.choice(atypes),
            random.choice(["US", "GB", "DE", "FR", "CA"]),
            round(random.uniform(500, 50000), 2),
            (base - timedelta(days=random.randint(30, 3650))).date(),
            random.random() < 0.03,
        )
        for i in range(start, end)
    ]

def generate_merchants_chunk(start, end, cats, risks):
    return [
        (
            i,
            f"Merchant {i}",
            random.choice(cats),
            random.choice(["US", "GB", "NG", "CN", "RU"]),
            random.choice(risks),
            round(random.uniform(5, 500), 2),
        )
        for i in range(start, end)
    ]

def generate_transactions_chunk(start, end, NA, NM, base, chans, currs, rcodes):
    return [
        (
            i,
            random.randint(1, NA),
            random.randint(1, NM),
            round(random.uniform(1, 5000), 2),
            base + timedelta(seconds=random.randint(0, 365 * 86400)),
            random.choice(chans),
            random.choice(currs),
            random.random() < 0.05,
            random.random() < 0.04,
            random.choice(rcodes),
        )
        for i in range(start, end)
    ]

def generate_alerts_chunk(start, end, flagged_ids, NT, atypes2, sevs, base, ress):
    return [
        (
            i,
            random.choice(flagged_ids) if flagged_ids else random.randint(1, NT),
            random.choice(atypes2),
            random.choice(sevs),
            base + timedelta(seconds=random.randint(0, 365 * 86400)),
            random.random() > 0.4,
            random.choice(ress) if random.random() > 0.4 else None,
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 200.0
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

    cpu_count = min(4, os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "accounts", ['account_id', 'holder_name', 'account_type', 'country', 'credit_limit', 'opened_date', 'is_frozen'],
                       run_parallel(executor, generate_accounts_chunk, NA, atypes, base))
        batched_insert(con, "merchants", ['merchant_id', 'name', 'category', 'country', 'risk_tier', 'avg_txn_amount'],
                       run_parallel(executor, generate_merchants_chunk, NM, cats, risks))
        
        txns = run_parallel(executor, generate_transactions_chunk, NT, NA, NM, base, chans, currs, rcodes)
        batched_insert(con, "transactions", ['txn_id', 'account_id', 'merchant_id', 'amount', 'txn_ts', 'channel', 'currency', 'is_declined', 'is_flagged', 'response_code'], txns)

        flagged_ids = [t[0] for t in txns if t[8]][:NAL]
        batched_insert(con, "alerts", ['alert_id', 'txn_id', 'alert_type', 'severity', 'created_ts', 'resolved', 'resolution'],
                       run_parallel(executor, generate_alerts_chunk, NAL, flagged_ids, NT, atypes2, sevs, base, ress))


    con.close()
    print(f"p02 done: accounts={NA} txns={NT} alerts={NAL}")

if __name__ == "__main__":
    main()
