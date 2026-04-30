import duckdb, sys, os
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_campaigns_chunk(start, end, chans, objs, base):
    rng = np.random.default_rng(start)
    size = end - start
    brand_rand = rng.integers(1, 21, size)
    chan_idx = rng.integers(0, len(chans), size)
    obj_idx = rng.integers(0, len(objs), size)
    start_days = rng.integers(0, 201, size)
    end_days = rng.integers(200, 366, size)
    budget_rand = rng.uniform(5000, 500000, size)
    cpm_rand = rng.uniform(0.5, 15, size)
    return [
        (
            i,
            f"Campaign {i}",
            f"Brand {brand_rand[i - start]}",
            chans[chan_idx[i - start]],
            objs[obj_idx[i - start]],
            base + timedelta(days=int(start_days[i - start])),
            base + timedelta(days=int(end_days[i - start])),
            round(float(budget_rand[i - start]), 2),
            round(float(cpm_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]


def generate_impressions_chunk(start, end, NCA, NIMP, bts, devs, geos, places):
    rng = np.random.default_rng(start)
    size = end - start
    camp_rand = rng.integers(1, NCA + 1, size)
    user_rand = rng.integers(1, NIMP * 10 + 1, size)
    sec_rand = rng.integers(0, 300 * 86400 + 1, size)
    dev_idx = rng.integers(0, len(devs), size)
    geo_idx = rng.integers(0, len(geos), size)
    place_idx = rng.integers(0, len(places), size)
    cost_rand = rng.uniform(0.0001, 0.05, size)
    return [
        (
            i,
            int(camp_rand[i - start]),
            int(user_rand[i - start]),
            bts + timedelta(seconds=int(sec_rand[i - start])),
            devs[dev_idx[i - start]],
            geos[geo_idx[i - start]],
            places[place_idx[i - start]],
            round(float(cost_rand[i - start]), 6),
        )
        for i in range(start, end)
    ]


def generate_clicks_chunk(start, end, imp_rows):
    rng = np.random.default_rng(start)
    size = end - start
    imp_idx = rng.integers(0, len(imp_rows), size)
    sec_rand = rng.integers(1, 3601, size)
    
    rows = []
    for i in range(start, end):
        ir = imp_rows[imp_idx[i - start]]
        rows.append(
            (
                i,
                ir[0],
                ir[1],
                ir[2],
                ir[3] + timedelta(seconds=int(sec_rand[i - start])),
                ir[4],
            )
        )
    return rows


def generate_conversions_chunk(start, end, click_rows, NCA, NIMP, bts, ctypes):
    rng = np.random.default_rng(start)
    size = end - start
    click_idx = rng.integers(0, len(click_rows), size)
    camp_rand = rng.integers(1, NCA + 1, size)
    user_rand = rng.integers(1, NIMP * 10 + 1, size)
    sec_rand = rng.integers(0, 300 * 86400 + 1, size)
    type_idx = rng.integers(0, len(ctypes), size)
    rev_rand = rng.uniform(0, 500, size)
    return [
        (
            i,
            click_rows[click_idx[i - start]][0],
            int(camp_rand[i - start]),
            int(user_rand[i - start]),
            bts + timedelta(seconds=int(sec_rand[i - start])),
            ctypes[type_idx[i - start]],
            round(float(rev_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    NCA, NIMP, NCL, NCV = (
        max(a, int(b * sf_adj))
        for a, b in [(10, 200), (100, 500000), (20, 15000), (5, 3000)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

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
    bts = datetime(2023, 1, 1)
    base = date(2023, 1, 1)
    chans = ["search", "social", "display", "video", "email", "affiliate"]
    objs = ["awareness", "traffic", "leads", "sales", "retention"]
    devs = ["desktop", "mobile", "tablet", "ctv"]
    geos = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "BR"]
    places = ["header", "sidebar", "feed", "pre-roll", "interstitial", "sponsored"]
    ctypes = ["purchase", "lead", "signup", "download", "call"]

    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "campaigns",
            [
                "campaign_id",
                "name",
                "advertiser",
                "channel",
                "objective",
                "start_date",
                "end_date",
                "budget",
                "cpm_target",
            ],
            run_parallel(executor, generate_campaigns_chunk, NCA, chans, objs, base),
        )

        imp_rows = run_parallel(
            executor,
            generate_impressions_chunk,
            NIMP,
            NCA,
            NIMP,
            bts,
            devs,
            geos,
            places,
        )
        batched_insert(
            con,
            "impressions",
            [
                "imp_id",
                "campaign_id",
                "user_id",
                "imp_ts",
                "device",
                "geo",
                "placement",
                "cost_usd",
            ],
            imp_rows,
        )

        click_rows = run_parallel(executor, generate_clicks_chunk, NCL, imp_rows)
        batched_insert(
            con,
            "clicks",
            ["click_id", "imp_id", "campaign_id", "user_id", "click_ts", "device"],
            click_rows,
        )

        batched_insert(
            con,
            "conversions",
            [
                "conv_id",
                "click_id",
                "campaign_id",
                "user_id",
                "conv_ts",
                "conv_type",
                "revenue",
            ],
            run_parallel(
                executor,
                generate_conversions_chunk,
                NCV,
                click_rows,
                NCA,
                NIMP,
                bts,
                ctypes,
            ),
        )

    con.close()
    print(f"p08 done campaigns={NCA} impressions={NIMP} clicks={NCL}")


if __name__ == "__main__":
    main()
