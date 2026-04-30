import duckdb, numpy as np, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_players_chunk(start, end, countries, platforms, bts, age_groups):
    size = end - start
    rng = np.random.default_rng(start)
    country_indices = rng.integers(0, len(countries), size)
    platform_indices = rng.integers(0, len(platforms), size)
    seconds_offset = rng.integers(0, 200 * 86400 + 1, size)
    age_indices = rng.integers(0, len(age_groups), size)
    paid_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Player_{i}",
            countries[country_indices[idx]],
            platforms[platform_indices[idx]],
            bts + timedelta(seconds=int(seconds_offset[idx])),
            age_groups[age_indices[idx]],
            bool(paid_probs[idx] > 0.6),
        ))
    return rows

def generate_levels_chunk(start, end, worlds, difficulties):
    size = end - start
    rng = np.random.default_rng(start)
    world_indices = rng.integers(0, len(worlds), size)
    diff_indices = rng.integers(0, len(difficulties), size)
    par_times = rng.integers(60, 601, size)
    rewards = rng.integers(10, 501, size)
    unlock_offsets = rng.integers(0, 4, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Level_{i}",
            worlds[world_indices[idx]],
            difficulties[diff_indices[idx]],
            int(par_times[idx]),
            int(rewards[idx]),
            max(1, i - int(unlock_offsets[idx])),
        ))
    return rows

def generate_sessions_chunk(start, end, NPL, bts, platforms):
    size = end - start
    rng = np.random.default_rng(start)
    player_ids = rng.integers(1, NPL + 1, size)
    seconds_offset = rng.integers(0, 300 * 86400 + 1, size)
    durations = rng.integers(60, 7201, size)
    platform_indices = rng.integers(0, len(platforms), size)
    v_majors = rng.integers(1, 4, size)
    v_minors = rng.integers(0, 10, size)
    attempts = rng.integers(0, 11, size)
    coins = rng.integers(0, 1001, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        ss = bts + timedelta(seconds=int(seconds_offset[idx]))
        dur = int(durations[idx])
        rows.append((
            i,
            int(player_ids[idx]),
            ss,
            ss + timedelta(seconds=dur),
            platforms[platform_indices[idx]],
            f"v{v_majors[idx]}.{v_minors[idx]}",
            int(attempts[idx]),
            int(coins[idx]),
        ))
    return rows

def generate_events_chunk(start, end, sess_rows, etypes, NLV):
    size = end - start
    rng = np.random.default_rng(start)
    sess_indices = rng.integers(0, len(sess_rows), size)
    etype_indices = rng.integers(0, len(etypes), size)
    seconds_offset = rng.integers(0, 7201, size)
    level_ids = rng.integers(1, NLV + 1, size) if NLV > 0 else np.ones(size, dtype=int)
    values = rng.uniform(0, 1000, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        sess = sess_rows[sess_indices[idx]]
        et = sess[2] + timedelta(seconds=int(seconds_offset[idx]))
        rows.append((
            i,
            int(sess[0]),
            int(sess[1]),
            etypes[etype_indices[idx]],
            et,
            int(level_ids[idx]),
            round(float(values[idx]), 2),
            f"meta_{i}",
        ))
    return rows

def generate_purchases_chunk(start, end, NPL, bts, itypes, currencies):
    size = end - start
    rng = np.random.default_rng(start)
    player_ids = rng.integers(1, NPL + 1, size)
    seconds_offset = rng.integers(0, 300 * 86400 + 1, size)
    itype_indices = rng.integers(0, len(itypes), size)
    item_ids = rng.integers(1, 51, size)
    prices = rng.uniform(0.99, 99.99, size)
    currency_indices = rng.integers(0, len(currencies), size)
    refund_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(player_ids[idx]),
            bts + timedelta(seconds=int(seconds_offset[idx])),
            itypes[itype_indices[idx]],
            f"Item_{item_ids[idx]}",
            round(float(prices[idx]), 2),
            currencies[currency_indices[idx]],
            bool(refund_probs[idx] < 0.03),
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 10.0
    NPL = max(20, int(2000 * sf_adj))
    NSS = max(50, int(10000 * sf_adj))
    NEV = max(200, int(80000 * sf_adj))
    NPU = max(10, int(3000 * sf_adj))
    NLV = max(10, int(50 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS purchases; DROP TABLE IF EXISTS events;
    DROP TABLE IF EXISTS sessions; DROP TABLE IF EXISTS levels;
    DROP TABLE IF EXISTS players;
    CREATE TABLE players(player_id INTEGER PRIMARY KEY, username VARCHAR,
        country VARCHAR, platform VARCHAR, created_ts TIMESTAMP,
        age_group VARCHAR, is_paid_user BOOLEAN);
    CREATE TABLE levels(level_id INTEGER PRIMARY KEY, level_name VARCHAR,
        world VARCHAR, difficulty VARCHAR, par_time_sec INTEGER,
        reward_coins INTEGER, unlock_level INTEGER);
    CREATE TABLE sessions(session_id BIGINT PRIMARY KEY, player_id INTEGER,
        session_start TIMESTAMP, session_end TIMESTAMP, platform VARCHAR,
        version VARCHAR, levels_attempted INTEGER, coins_earned INTEGER);
    CREATE TABLE events(event_id BIGINT PRIMARY KEY, session_id BIGINT,
        player_id INTEGER, event_type VARCHAR, event_ts TIMESTAMP,
        level_id INTEGER, value DOUBLE, metadata VARCHAR);
    CREATE TABLE purchases(purchase_id INTEGER PRIMARY KEY, player_id INTEGER,
        purchase_ts TIMESTAMP, item_type VARCHAR, item_name VARCHAR,
        price_usd DECIMAL(8,2), currency VARCHAR, is_refunded BOOLEAN);
    """)

    bts = datetime(2023, 1, 1)
    countries = ["US", "CN", "DE", "JP", "BR", "KR", "RU", "GB", "CA", "FR"]
    platforms = ["PC", "Mobile_iOS", "Mobile_Android", "Console_PS", "Console_Xbox"]
    age_groups = ["<13", "13-17", "18-24", "25-34", "35-44", "45+"]
    worlds = ["Forest", "Desert", "Ocean", "Space", "Underground", "Sky"]
    difficulties = ["easy", "normal", "hard", "nightmare"]
    etypes = ["level_start", "level_complete", "level_fail", "achievement", "item_pickup", "death", "checkpoint", "boss_kill"]
    itypes = ["coin_pack", "skin", "level_skip", "power_up", "subscription", "loot_box"]
    currencies = ["USD", "EUR", "GBP", "JPY", "BRL"]

    cpu_count = os.cpu_count()
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "players", ['player_id', 'username', 'country', 'platform', 'created_ts', 'age_group', 'is_paid_user'],
                       run_parallel(executor, generate_players_chunk, NPL, countries, platforms, bts, age_groups))
        batched_insert(con, "levels", ['level_id', 'level_name', 'world', 'difficulty', 'par_time_sec', 'reward_coins', 'unlock_level'],
                       run_parallel(executor, generate_levels_chunk, NLV, worlds, difficulties))
        
        sess_rows = run_parallel(executor, generate_sessions_chunk, NSS, NPL, bts, platforms)
        batched_insert(con, "sessions", ['session_id', 'player_id', 'session_start', 'session_end', 'platform', 'version', 'levels_attempted', 'coins_earned'], sess_rows)

        batched_insert(con, "events", ['event_id', 'session_id', 'player_id', 'event_type', 'event_ts', 'level_id', 'value', 'metadata'],
                       run_parallel(executor, generate_events_chunk, NEV, sess_rows, etypes, NLV))
        
        batched_insert(con, "purchases", ['purchase_id', 'player_id', 'purchase_ts', 'item_type', 'item_name', 'price_usd', 'currency', 'is_refunded'],
                       run_parallel(executor, generate_purchases_chunk, NPU, NPL, bts, itypes, currencies))


    con.close()
    print(f"p09 done: players={NPL} sessions={NSS} events={NEV} purchases={NPU}")

if __name__ == "__main__":
    main()
