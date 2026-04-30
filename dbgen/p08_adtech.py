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

    campaign_ids = range(start, end)
    campaign_names = [f"Campaign {i}" for i in campaign_ids]
    advertisers = [f"Brand {aid}" for aid in advertiser_ids]
    selected_channels = np.take(channels, channel_indices).tolist()
    selected_objectives = np.take(objectives, objective_indices).tolist()
    start_dates = (np.datetime64(base) + start_days_offset.astype("timedelta64[D]")).tolist()
    end_dates = (np.datetime64(base) + end_days_offset.astype("timedelta64[D]")).tolist()
    budgets_rounded = np.round(budgets, 2).tolist()
    cpm_targets_rounded = np.round(cpm_targets, 2).tolist()
    
    return list(zip(campaign_ids, campaign_names, advertisers, selected_channels, selected_objectives, start_dates, end_dates, budgets_rounded, cpm_targets_rounded))


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

    imp_ids = range(start, end)
    selected_campaign_ids = campaign_ids.tolist()
    selected_user_ids = user_ids.tolist()
    imp_tss = (np.datetime64(bts) + seconds_offset.astype("timedelta64[s]")).tolist()
    selected_devices = np.take(devices, device_indices).tolist()
    selected_geos = np.take(geos, geo_indices).tolist()
    selected_placements = np.take(placements, placement_indices).tolist()
    costs_rounded = np.round(costs, 6).tolist()
    
    return list(zip(imp_ids, selected_campaign_ids, selected_user_ids, imp_tss, selected_devices, selected_geos, selected_placements, costs_rounded))


def generate_clicks_chunk(start, end, imp_refs):
    size = end - start
    rng = np.random.default_rng(start)
    imp_indices = rng.integers(0, len(imp_refs), size)
    seconds_offset = rng.integers(1, 3601, size)
    landing_url_ids = rng.integers(1, 21, size)

    click_ids = range(start, end)
    
    # imp_refs is list of tuples: (imp_id, campaign_id, user_id, imp_ts, device)
    selected_refs = [imp_refs[idx] for idx in imp_indices]
    
    imp_ids = [ref[0] for ref in selected_refs]
    campaign_ids = [ref[1] for ref in selected_refs]
    user_ids = [ref[2] for ref in selected_refs]
    imp_tss = [ref[3] for ref in selected_refs]
    devices = [ref[4] for ref in selected_refs]
    
    click_tss = [
        imp_ts + np.timedelta64(int(off), 's') 
        for imp_ts, off in zip(imp_tss, seconds_offset)
    ]
    
    landing_urls = [f"https://brand.com/lp/{uid}" for uid in landing_url_ids]
    
    return list(zip(click_ids, imp_ids, campaign_ids, user_ids, click_tss, landing_urls, devices))


def generate_conversions_chunk(start, end, click_refs, bts, ctypes):
    size = end - start
    rng = np.random.default_rng(start)
    click_indices = rng.integers(0, len(click_refs), size)
    seconds_offset = rng.integers(0, 300 * 86400 + 1, size)
    ctype_indices = rng.integers(0, len(ctypes), size)
    revenues = rng.uniform(0, 500, size)

    conv_ids = range(start, end)
    
    # click_refs is list of tuples: (click_id, campaign_id, user_id)
    selected_refs = [click_refs[idx] for idx in click_indices]
    
    click_ids = [ref[0] for ref in selected_refs]
    campaign_ids = [ref[1] for ref in selected_refs]
    user_ids = [ref[2] for ref in selected_refs]
    
    conv_tss = (np.datetime64(bts) + seconds_offset.astype("timedelta64[s]")).tolist()
    selected_ctypes = np.take(ctypes, ctype_indices).tolist()
    revenues_rounded = np.round(revenues, 2).tolist()
    
    return list(zip(conv_ids, click_ids, campaign_ids, user_ids, conv_tss, selected_ctypes, revenues_rounded))


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 84
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

    cpu_count = get_worker_count()
    progress = GenerationProgress("p08_adtech", 4)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("campaigns")
        batched_insert(con, "campaigns", ['campaign_id', 'name', 'advertiser', 'channel', 'objective', 'start_date', 'end_date', 'budget', 'cpm_target'],
                       run_parallel(executor, generate_campaigns_chunk, NCA, channels, objectives, base))
        
        progress.advance("impressions")
        imp_rows = run_parallel(executor, generate_impressions_chunk, NIMP, NCA, bts, devices, geos, placements)
        batched_insert(con, "impressions", ['imp_id', 'campaign_id', 'user_id', 'imp_ts', 'device', 'geo', 'placement', 'cost_usd'], imp_rows)
        imp_refs = con.execute(
            f"""
            SELECT imp_id, campaign_id, user_id, imp_ts, device
            FROM impressions
            USING SAMPLE {max(NCL, 1)} ROWS
            """
        ).fetchall()
        progress.advance("clicks")
        click_rows = run_parallel(executor, generate_clicks_chunk, NCL, imp_refs)
        batched_insert(con, "clicks", ['click_id', 'imp_id', 'campaign_id', 'user_id', 'click_ts', 'landing_url', 'device'], click_rows)
        click_refs = con.execute(
            f"""
            SELECT click_id, campaign_id, user_id
            FROM clicks
            USING SAMPLE {max(NCV, 1)} ROWS
            """
        ).fetchall()
        progress.advance("conversions")
        batched_insert(con, "conversions", ['conv_id', 'click_id', 'campaign_id', 'user_id', 'conv_ts', 'conv_type', 'revenue'],
                       run_parallel(executor, generate_conversions_chunk, NCV, click_refs, bts, ctypes))


    con.close()
    print_generation_summary(
        "p08_adtech",
        sf,
        {
            "campaigns": NCA,
            "impressions": NIMP,
            "clicks": NCL,
            "conversions": NCV,
        },
    )

if __name__ == "__main__":
    main()
