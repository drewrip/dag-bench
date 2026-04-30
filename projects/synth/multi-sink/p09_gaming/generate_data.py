import duckdb, sys, os
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_players_chunk(start, end, countries, platforms, bts, age_groups):
    rng = np.random.default_rng(start)
    size = end - start
    country_idx = rng.integers(0, len(countries), size)
    plat_idx = rng.integers(0, len(platforms), size)
    sec_rand = rng.integers(0, 200 * 86400 + 1, size)
    age_idx = rng.integers(0, len(age_groups), size)
    paid_rand = rng.random(size)
    return [
        (
            i,
            f"Player_{i}",
            countries[country_idx[i - start]],
            platforms[plat_idx[i - start]],
            bts + timedelta(seconds=int(sec_rand[i - start])),
            age_groups[age_idx[i - start]],
            bool(paid_rand[i - start] > 0.6),
        )
        for i in range(start, end)
    ]


def generate_levels_chunk(start, end, worlds, diffs):
    rng = np.random.default_rng(start)
    size = end - start
    world_idx = rng.integers(0, len(worlds), size)
    diff_idx = rng.integers(0, len(diffs), size)
    time_rand = rng.integers(60, 601, size)
    coins_rand = rng.integers(10, 501, size)
    return [
        (
            i,
            f"Level_{i}",
            worlds[world_idx[i - start]],
            diffs[diff_idx[i - start]],
            int(time_rand[i - start]),
            int(coins_rand[i - start]),
        )
        for i in range(start, end)
    ]


def generate_sessions_chunk(start, end, NPL, bts, platforms):
    rng = np.random.default_rng(start)
    size = end - start
    player_rand = rng.integers(1, NPL + 1, size)
    start_sec = rng.integers(0, 300 * 86400 + 1, size)
    end_sec = rng.integers(60, 7201, size)
    plat_idx = rng.integers(0, len(platforms), size)
    vmaj_rand = rng.integers(1, 4, size)
    vmin_rand = rng.integers(0, 10, size)
    coins_rand = rng.integers(0, 1001, size)
    return [
        (
            i,
            int(player_rand[i - start]),
            (ss := bts + timedelta(seconds=int(start_sec[i - start]))),
            ss + timedelta(seconds=int(end_sec[i - start])),
            platforms[plat_idx[i - start]],
            f"v{vmaj_rand[i - start]}.{vmin_rand[i - start]}",
            int(coins_rand[i - start]),
        )
        for i in range(start, end)
    ]


def generate_events_chunk(start, end, sess, etypes, NLV):
    rng = np.random.default_rng(start)
    size = end - start
    sess_idx = rng.integers(0, len(sess), size)
    etype_idx = rng.integers(0, len(etypes), size)
    sec_rand = rng.integers(0, 7201, size)
    level_rand = rng.integers(1, max(2, NLV + 1), size)
    val_rand = rng.uniform(0, 1000, size)
    
    rows = []
    for i in range(start, end):
        idx = i - start
        s = sess[sess_idx[idx]]
        rows.append(
            (
                i,
                s[0],
                s[1],
                etypes[etype_idx[idx]],
                s[2] + timedelta(seconds=int(sec_rand[idx])),
                int(level_rand[idx]) if NLV > 0 else 1,
                round(float(val_rand[idx]), 2),
            )
        )
    return rows


def generate_purchases_chunk(start, end, NPL, bts, itypes):
    rng = np.random.default_rng(start)
    size = end - start
    player_rand = rng.integers(1, NPL + 1, size)
    sec_rand = rng.integers(0, 300 * 86400 + 1, size)
    itype_idx = rng.integers(0, len(itypes), size)
    item_rand = rng.integers(1, 51, size)
    price_rand = rng.uniform(0.99, 99.99, size)
    refund_rand = rng.random(size)
    return [
        (
            i,
            int(player_rand[i - start]),
            bts + timedelta(seconds=int(sec_rand[i - start])),
            itypes[itype_idx[i - start]],
            f"Item_{item_rand[i - start]}",
            round(float(price_rand[i - start]), 2),
            bool(refund_rand[i - start] < 0.03),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    NPL, NSS, NEV, NPU, NLV = (
        max(a, int(b * sf_adj))
        for a, b in [(20, 2000), (50, 10000), (200, 80000), (10, 3000), (10, 50)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS purchases; DROP TABLE IF EXISTS events;
    DROP TABLE IF EXISTS sessions; DROP TABLE IF EXISTS levels; DROP TABLE IF EXISTS players;
    CREATE TABLE players(player_id INTEGER PRIMARY KEY,username VARCHAR,country VARCHAR,
      platform VARCHAR,created_ts TIMESTAMP,age_group VARCHAR,is_paid_user BOOLEAN);
    CREATE TABLE levels(level_id INTEGER PRIMARY KEY,level_name VARCHAR,world VARCHAR,
      difficulty VARCHAR,par_time_sec INTEGER,reward_coins INTEGER);
    CREATE TABLE sessions(session_id BIGINT PRIMARY KEY,player_id INTEGER,session_start TIMESTAMP,
      session_end TIMESTAMP,platform VARCHAR,version VARCHAR,coins_earned INTEGER);
    CREATE TABLE events(event_id BIGINT PRIMARY KEY,session_id BIGINT,player_id INTEGER,
      event_type VARCHAR,event_ts TIMESTAMP,level_id INTEGER,value DOUBLE);
    CREATE TABLE purchases(purchase_id INTEGER PRIMARY KEY,player_id INTEGER,purchase_ts TIMESTAMP,
      item_type VARCHAR,item_name VARCHAR,price_usd DECIMAL(8,2),is_refunded BOOLEAN);
    """)
    bts = datetime(2023, 1, 1)
    countries = ["US", "CN", "DE", "JP", "BR", "KR", "RU", "GB"]
    platforms = ["PC", "Mobile_iOS", "Mobile_Android", "Console"]
    age_groups = ["<13", "13-17", "18-24", "25-34", "35-44", "45+"]
    worlds = ["Forest", "Desert", "Ocean", "Space", "Underground", "Sky"]
    diffs = ["easy", "normal", "hard", "nightmare"]
    etypes = ["level_start", "level_complete", "level_fail", "achievement", "death"]
    itypes = ["coin_pack", "skin", "level_skip", "power_up", "subscription"]

    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "players",
            [
                "player_id",
                "username",
                "country",
                "platform",
                "created_ts",
                "age_group",
                "is_paid_user",
            ],
            run_parallel(
                executor,
                generate_players_chunk,
                NPL,
                countries,
                platforms,
                bts,
                age_groups,
            ),
        )

        batched_insert(
            con,
            "levels",
            [
                "level_id",
                "level_name",
                "world",
                "difficulty",
                "par_time_sec",
                "reward_coins",
            ],
            run_parallel(executor, generate_levels_chunk, NLV, worlds, diffs),
        )

        sess = run_parallel(executor, generate_sessions_chunk, NSS, NPL, bts, platforms)
        batched_insert(
            con,
            "sessions",
            [
                "session_id",
                "player_id",
                "session_start",
                "session_end",
                "platform",
                "version",
                "coins_earned",
            ],
            sess,
        )

        batched_insert(
            con,
            "events",
            [
                "event_id",
                "session_id",
                "player_id",
                "event_type",
                "event_ts",
                "level_id",
                "value",
            ],
            run_parallel(executor, generate_events_chunk, NEV, sess, etypes, NLV),
        )

        batched_insert(
            con,
            "purchases",
            [
                "purchase_id",
                "player_id",
                "purchase_ts",
                "item_type",
                "item_name",
                "price_usd",
                "is_refunded",
            ],
            run_parallel(executor, generate_purchases_chunk, NPU, NPL, bts, itypes),
        )

    con.close()
    print(f"p09 done players={NPL} sessions={NSS} events={NEV}")


if __name__ == "__main__":
    main()
