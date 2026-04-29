import duckdb, random, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_campaigns_chunk(start, end, channels, objectives, base):
    return [
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
        for i in range(start, end)
    ]

def generate_impressions_chunk(start, end, NCA, bts, devices, geos, placements):
    return [
        (
            i,
            random.randint(1, NCA),
            random.randint(1, (end - start) * 100), # Approximating user_id range
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(devices),
            random.choice(geos),
            random.choice(placements),
            round(random.uniform(0.0001, 0.05), 6),
        )
        for i in range(start, end)
    ]

def generate_clicks_chunk(start, end, imp_rows, imp_ids):
    rows = []
    for i in range(start, end):
        imp_id = random.choice(imp_ids)
        imp_row = imp_rows[imp_id - 1]
        ct = imp_row[3] + timedelta(seconds=random.randint(1, 3600))
        rows.append((
            i,
            imp_id,
            imp_row[1],
            imp_row[2],
            ct,
            f"https://brand.com/lp/{random.randint(1, 20)}",
            imp_row[4],
        ))
    return rows

def generate_conversions_chunk(start, end, click_ids, NCA, NIMP, bts, ctypes):
    return [
        (
            i,
            random.choice(click_ids),
            random.randint(1, NCA),
            random.randint(1, NIMP * 10),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(ctypes),
            round(random.uniform(0, 500), 2),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1.0
    NCA = max(10, int(200 * sf_adj))
    NIMP = max(100, int(500000 * sf_adj))
    NCL = max(20, int(15000 * sf_adj))
    NCV = max(5, int(3000 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS conversions; DROP TABLE IF EXISTS clicks;
    DROP TABLE IF EXISTS impressions; DROP TABLE IF EXISTS campaigns;
    CREATE TABLE campaigns(campaign_id INTEGER PRIMARY KEY, name VARCHAR,
        advertiser VARCHAR, channel VARCHAR, objective VARCHAR,
        start_date DATE, end_date DATE, budget DECIMAL(12,2), cpm_target DECIMAL(6,2));
    CREATE TABLE impressions(imp_id BIGINT PRIMARY KEY, campaign_id INTEGER,
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

    cpu_count = min(4, os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "campaigns", ['campaign_id', 'name', 'advertiser', 'channel', 'objective', 'start_date', 'end_date', 'budget', 'cpm_target'],
                       run_parallel(executor, generate_campaigns_chunk, NCA, channels, objectives, base))
        
        imp_rows = run_parallel(executor, generate_impressions_chunk, NIMP, NCA, bts, devices, geos, placements)
        batched_insert(con, "impressions", ['imp_id', 'campaign_id', 'user_id', 'imp_ts', 'device', 'geo', 'placement', 'cost_usd'], imp_rows)

        imp_ids = [r[0] for r in imp_rows]
        click_rows = run_parallel(executor, generate_clicks_chunk, NCL, imp_rows, imp_ids)
        batched_insert(con, "clicks", ['click_id', 'imp_id', 'campaign_id', 'user_id', 'click_ts', 'landing_url', 'device'], click_rows)

        click_ids = [r[0] for r in click_rows]
        batched_insert(con, "conversions", ['conv_id', 'click_id', 'campaign_id', 'user_id', 'conv_ts', 'conv_type', 'revenue'],
                       run_parallel(executor, generate_conversions_chunk, NCV, click_ids, NCA, NIMP, bts, ctypes))


    con.close()
    print(f"p08 done: campaigns={NCA} impressions={NIMP} clicks={NCL} conversions={NCV}")

if __name__ == "__main__":
    main()
