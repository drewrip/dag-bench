import duckdb, numpy as np, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_campaigns_chunk(start, end, channels, objectives, base):
    size = end - start
    rng = np.random.default_rng(start)
    advertiser_ids = rng.integers(1, 21, size)
    channel_indices = rng.integers(0, len(channels), size)
    objective_indices = rng.integers(0, len(objectives), size)
    start_days_offset = rng.integers(0, 201, size)
    end_days_offset = rng.integers(200, 366, size)
    budgets = rng.uniform(5000, 500000, size)
    cpm_targets = rng.uniform(0.5, 15, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Campaign {i}",
            f"Brand {advertiser_ids[idx]}",
            channels[channel_indices[idx]],
            objectives[objective_indices[idx]],
            base + timedelta(days=int(start_days_offset[idx])),
            base + timedelta(days=int(end_days_offset[idx])),
            round(float(budgets[idx]), 2),
            round(float(cpm_targets[idx]), 2),
        ))
    return rows

def generate_impressions_chunk(start, end, NCA, bts, devices, geos, placements):
    size = end - start
    rng = np.random.default_rng(start)
    campaign_ids = rng.integers(1, NCA + 1, size)
    user_ids = rng.integers(1, size * 100 + 1, size)
    seconds_offset = rng.integers(0, 300 * 86400 + 1, size)
    device_indices = rng.integers(0, len(devices), size)
    geo_indices = rng.integers(0, len(geos), size)
    placement_indices = rng.integers(0, len(placements), size)
    costs = rng.uniform(0.0001, 0.05, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(campaign_ids[idx]),
            int(user_ids[idx]),
            bts + timedelta(seconds=int(seconds_offset[idx])),
            devices[device_indices[idx]],
            geos[geo_indices[idx]],
            placements[placement_indices[idx]],
            round(float(costs[idx]), 6),
        ))
    return rows

def generate_clicks_chunk(start, end, imp_rows, imp_ids):
    size = end - start
    rng = np.random.default_rng(start)
    imp_indices = rng.integers(0, len(imp_ids), size)
    seconds_offset = rng.integers(1, 3601, size)
    landing_url_ids = rng.integers(1, 21, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        imp_id = imp_ids[imp_indices[idx]]
        imp_row = imp_rows[imp_id - 1]
        ct = imp_row[3] + timedelta(seconds=int(seconds_offset[idx]))
        rows.append((
            i,
            int(imp_id),
            int(imp_row[1]),
            int(imp_row[2]),
            ct,
            f"https://brand.com/lp/{landing_url_ids[idx]}",
            imp_row[4],
        ))
    return rows

def generate_conversions_chunk(start, end, click_ids, NCA, NIMP, bts, ctypes):
    size = end - start
    rng = np.random.default_rng(start)
    click_id_indices = rng.integers(0, len(click_ids), size)
    campaign_ids = rng.integers(1, NCA + 1, size)
    user_ids = rng.integers(1, NIMP * 10 + 1, size)
    seconds_offset = rng.integers(0, 300 * 86400 + 1, size)
    ctype_indices = rng.integers(0, len(ctypes), size)
    revenues = rng.uniform(0, 500, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(click_ids[click_id_indices[idx]]),
            int(campaign_ids[idx]),
            int(user_ids[idx]),
            bts + timedelta(seconds=int(seconds_offset[idx])),
            ctypes[ctype_indices[idx]],
            round(float(revenues[idx]), 2),
        ))
    return rows


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

    cpu_count = os.cpu_count()
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
