import csv, duckdb, random, sys, os, tempfile
from datetime import datetime, timedelta, date

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NCA, NIMP, NCL, NCV = (
    max(a, int(b * sf)) for a, b in [(10, 200), (100, 500000), (20, 15000), (5, 3000)]
)
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
DROP TABLE IF EXISTS conversions; DROP TABLE IF EXISTS clicks;
DROP TABLE IF EXISTS impressions; DROP TABLE IF EXISTS campaigns;
CREATE TABLE campaigns(campaign_id INTEGER PRIMARY KEY,name VARCHAR,advertiser VARCHAR,
  channel VARCHAR,objective VARCHAR,start_date DATE,end_date DATE,
  budget DECIMAL(12,2),cpm_target DECIMAL(6,2));
CREATE TABLE impressions(imp_id BIGINT PRIMARY KEY,campaign_id INTEGER,user_id BIGINT,
  imp_ts TIMESTAMP,device VARCHAR,geo VARCHAR,placement VARCHAR,cost_usd DECIMAL(8,6));
CREATE TABLE clicks(click_id BIGINT PRIMARY KEY,imp_id BIGINT,campaign_id INTEGER,
  user_id BIGINT,click_ts TIMESTAMP,device VARCHAR);
CREATE TABLE conversions(conv_id INTEGER PRIMARY KEY,click_id BIGINT,campaign_id INTEGER,
  user_id BIGINT,conv_ts TIMESTAMP,conv_type VARCHAR,revenue DECIMAL(10,2));
""")
con.execute("BEGIN")
bts = datetime(2023, 1, 1)
base = date(2023, 1, 1)
chans = ["search", "social", "display", "video", "email", "affiliate"]
objs = ["awareness", "traffic", "leads", "sales", "retention"]
devs = ["desktop", "mobile", "tablet", "ctv"]
geos = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "BR"]
places = ["header", "sidebar", "feed", "pre-roll", "interstitial", "sponsored"]
ctypes = ["purchase", "lead", "signup", "download", "call"]
batched_insert(
    "INSERT INTO campaigns VALUES(?,?,?,?,?,?,?,?,?)",
    [
        (
            i,
            f"Campaign {i}",
            f"Brand {random.randint(1, 20)}",
            random.choice(chans),
            random.choice(objs),
            base + timedelta(days=random.randint(0, 200)),
            base + timedelta(days=random.randint(200, 365)),
            round(random.uniform(5000, 500000), 2),
            round(random.uniform(0.5, 15), 2),
        )
        for i in range(1, NCA + 1)
    ],
)
imp_rows = []
for i in range(1, NIMP + 1):
    imp_rows.append(
        (
            i,
            random.randint(1, NCA),
            random.randint(1, NIMP * 10),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(devs),
            random.choice(geos),
            random.choice(places),
            round(random.uniform(0.0001, 0.05), 6),
        )
    )
batched_insert("INSERT INTO impressions VALUES(?,?,?,?,?,?,?,?)", imp_rows)
click_rows = []
for i in range(1, NCL + 1):
    ir = imp_rows[random.randint(0, len(imp_rows) - 1)]
    click_rows.append(
        (
            i,
            ir[0],
            ir[1],
            ir[2],
            ir[3] + timedelta(seconds=random.randint(1, 3600)),
            ir[4],
        )
    )
batched_insert("INSERT INTO clicks VALUES(?,?,?,?,?,?)", click_rows)
batched_insert(
    "INSERT INTO conversions VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            click_rows[random.randint(0, len(click_rows) - 1)][0],
            random.randint(1, NCA),
            random.randint(1, NIMP * 10),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(ctypes),
            round(random.uniform(0, 500), 2),
        )
        for i in range(1, NCV + 1)
    ],
)
con.commit()
con.close()
print(f"p08 done campaigns={NCA} impressions={NIMP} clicks={NCL}")
