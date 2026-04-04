import duckdb, random, sys, os
from datetime import datetime, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NA = max(10, int(1000 * sf))
NM = max(10, int(300 * sf))
NT = max(50, int(20000 * sf))
NAL = max(5, int(500 * sf))

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

from datetime import date

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

con.executemany(
    "INSERT INTO accounts VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            f"Holder {i}",
            random.choice(atypes),
            random.choice(["US", "GB", "DE", "FR", "CA"]),
            round(random.uniform(500, 50000), 2),
            (base - timedelta(days=random.randint(30, 3650))).date(),
            random.random() < 0.03,
        )
        for i in range(1, NA + 1)
    ],
)
con.executemany(
    "INSERT INTO merchants VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            f"Merchant {i}",
            random.choice(cats),
            random.choice(["US", "GB", "NG", "CN", "RU"]),
            random.choice(risks),
            round(random.uniform(5, 500), 2),
        )
        for i in range(1, NM + 1)
    ],
)

txns = []
for i in range(1, NT + 1):
    ts = base + timedelta(seconds=random.randint(0, 365 * 86400))
    flagged = random.random() < 0.04
    txns.append(
        (
            i,
            random.randint(1, NA),
            random.randint(1, NM),
            round(random.uniform(1, 5000), 2),
            ts,
            random.choice(chans),
            random.choice(currs),
            random.random() < 0.05,
            flagged,
            random.choice(rcodes),
        )
    )
con.executemany("INSERT INTO transactions VALUES(?,?,?,?,?,?,?,?,?,?)", txns)

flagged_ids = [t[0] for t in txns if t[8]][:NAL]
con.executemany(
    "INSERT INTO alerts VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.choice(flagged_ids) if flagged_ids else random.randint(1, NT),
            random.choice(atypes2),
            random.choice(sevs),
            base + timedelta(seconds=random.randint(0, 365 * 86400)),
            random.random() > 0.4,
            random.choice(ress) if random.random() > 0.4 else None,
        )
        for i in range(1, NAL + 1)
    ],
)
con.close()
print(f"p02 done: accounts={NA} txns={NT} alerts={NAL}")
