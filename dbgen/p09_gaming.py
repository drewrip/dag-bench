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

def generate_events_chunk(start, end, session_ids, player_ids, session_starts, etypes, NLV):
    size = end - start
    rng = np.random.default_rng(start)
    sess_indices = rng.integers(0, len(session_ids), size)
    etype_indices = rng.integers(0, len(etypes), size)
    seconds_offset = rng.integers(0, 7201, size)
    level_ids = rng.integers(1, NLV + 1, size) if NLV > 0 else np.ones(size, dtype=int)
    values = rng.uniform(0, 1000, size)
    event_ids = range(start, end)

    selected_session_ids = np.take(session_ids, sess_indices)
    selected_player_ids = np.take(player_ids, sess_indices)
    selected_starts = np.take(session_starts, sess_indices)
    event_ts = (selected_starts + seconds_offset.astype("timedelta64[s]")).tolist()
    event_types = np.take(etypes, etype_indices)
    rounded_values = np.round(values, 2)

    return list(
        zip(
            event_ids,
            selected_session_ids.tolist(),
            selected_player_ids.tolist(),
            event_types.tolist(),
            event_ts,
            level_ids.tolist(),
            rounded_values.tolist(),
            [f"meta_{i}" for i in event_ids],
        )
    )

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
    sf_adj = sf * 400
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

    cpu_count = get_worker_count()
    progress = GenerationProgress("p09_gaming", 5)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("players")
        batched_insert(con, "players", ['player_id', 'username', 'country', 'platform', 'created_ts', 'age_group', 'is_paid_user'],
                       run_parallel(executor, generate_players_chunk, NPL, countries, platforms, bts, age_groups))
        progress.advance("levels")
        batched_insert(con, "levels", ['level_id', 'level_name', 'world', 'difficulty', 'par_time_sec', 'reward_coins', 'unlock_level'],
                       run_parallel(executor, generate_levels_chunk, NLV, worlds, difficulties))
        
        progress.advance("sessions")
        sess_rows = run_parallel(executor, generate_sessions_chunk, NSS, NPL, bts, platforms)
        batched_insert(con, "sessions", ['session_id', 'player_id', 'session_start', 'session_end', 'platform', 'version', 'levels_attempted', 'coins_earned'], sess_rows)
        session_refs = con.execute(
            f"""
            SELECT session_id, player_id, session_start
            FROM sessions
            USING SAMPLE {max(NEV, 1)} ROWS
            """
        ).fetchall()
        session_ids = np.array([row[0] for row in session_refs], dtype=np.int64)
        player_ids = np.array([row[1] for row in session_refs], dtype=np.int64)
        session_starts = np.array([row[2] for row in session_refs], dtype="datetime64[us]")
        progress.advance("events")
        batched_insert(con, "events", ['event_id', 'session_id', 'player_id', 'event_type', 'event_ts', 'level_id', 'value', 'metadata'],
                       run_parallel(executor, generate_events_chunk, NEV, session_ids, player_ids, session_starts, np.array(etypes, dtype=object), NLV))
        
        progress.advance("purchases")
        batched_insert(con, "purchases", ['purchase_id', 'player_id', 'purchase_ts', 'item_type', 'item_name', 'price_usd', 'currency', 'is_refunded'],
                       run_parallel(executor, generate_purchases_chunk, NPU, NPL, bts, itypes, currencies))


    con.close()
    print_generation_summary(
        "p09_gaming",
        sf,
        {
            "players": NPL,
            "levels": NLV,
            "sessions": NSS,
            "events": NEV,
            "purchases": NPU,
        },
    )

if __name__ == "__main__":
    main()
