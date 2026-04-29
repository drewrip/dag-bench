import pyarrow as pa
import duckdb, random, sys, os
from datetime import datetime, timedelta, date

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NCA = max(10, int(200 * sf))
NIMP = max(100, int(500000 * sf))
NCL = max(20, int(15000 * sf))
NCV = max(5, int(3000 * sf))

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")

def batched_insert(table_name, columns, rows):
    rows = list(rows)
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")

con.execute("""
DROP TABLE IF EXISTS conversions; DROP TABLE IF EXISTS clicks;
DROP TABLE IF EXISTS impressions; DROP TABLE IF EXISTS campaigns;
CREATE TABLE campaigns(campaign_id INTEGER PRIMARY KEY, name VARCHAR,
    advertiser VARCHAR, channel VARCHAR, objective VARCHAR,
    start_date DATE, end_date DATE, budget DECIMAL(12,2), cpm_target DECIMAL(6,2)); CREATE TABLE impressions(imp_id BIGINT PRIMARY KEY, campaign_id INTEGER,
    user_id BIGINT, imp_ts TIMESTAMP, device VARCHAR, geo VARCHAR,
    placement VARCHAR, cost_usd DECIMAL(8,6));
CREATE TABLE clicks(click_id BIGINT PRIMARY KEY, imp_id BIGINT,
    campaign_id INTEGER, user_id BIGINT, click_ts TIMESTAMP,
    landing_url VARCHAR, device VARCHAR);
CREATE TABLE conversions(conv_id INTEGER PRIMARY KEY, click_id BIGINT,
    campaign_id INTEGER, user_id BIGINT, conv_ts TIMESTAMP,
    conv_type VARCHAR, revenue DECIMAL(10,2));
""")

bts = datetime(2023, 1, 1)
base = date(2023, 1, 1)
channels = ["search", "social", "display", "video", "email", "affiliate"]
objectives = ["awareness", "traffic", "leads", "sales", "retention"]
devices = ["desktop", "mobile", "tablet", "ctv"]
geos = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "BR"]
placements = ["header", "sidebar", "feed", "pre-roll", "interstitial", "sponsored"]
ctypes = ["purchase", "lead", "signup", "download", "call"]

batched_insert("campaigns", ['campaign_id', 'name', 'advertiser', 'channel', 'objective', 'start_date', 'end_date', 'budget', 'cpm_target'], [
        (
            i,
            f"Campaign {i}",
            f"Brand {random.randint(1, 20)}",
            random.choice(channels),
            random.choice(objectives),
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
            random.choice(devices),
            random.choice(geos),
            random.choice(placements),
            round(random.uniform(0.0001, 0.05), 6),
        )
    )
batched_insert("impressions", ['imp_id', 'campaign_id', 'user_id', 'imp_ts', 'device', 'geo', 'placement', 'cost_usd'], imp_rows)

# clicks reference random impressions
imp_ids = [r[0] for r in imp_rows]
click_rows = []
for i in range(1, NCL + 1):
    imp = random.choice(imp_ids)
    imp_row = imp_rows[imp - 1]
    ct = imp_row[3] + timedelta(seconds=random.randint(1, 3600))
    click_rows.append(
        (
            i,
            imp,
            imp_row[1],
            imp_row[2],
            ct,
            f"https://brand.com/lp/{random.randint(1, 20)}",
            imp_row[4],
        )
    )
batched_insert("clicks", ['click_id', 'imp_id', 'campaign_id', 'user_id', 'click_ts', 'landing_url', 'device'], click_rows)

click_ids = [r[0] for r in click_rows]
batched_insert("conversions", ['conv_id', 'click_id', 'campaign_id', 'user_id', 'conv_ts', 'conv_type', 'revenue'], [
        (
            i,
            random.choice(click_ids),
            random.randint(1, NCA),
            random.randint(1, NIMP * 10),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(ctypes),
            round(random.uniform(0, 500), 2),
        )
        for i in range(1, NCV + 1)
    ],
)
con.close()
print(f"p08 done: campaigns={NCA} impressions={NIMP} clicks={NCL} conversions={NCV}")
