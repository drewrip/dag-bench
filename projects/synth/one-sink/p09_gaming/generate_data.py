import duckdb, random, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_players_chunk(start, end, countries, platforms, bts, age_groups):
    return [
        (
            i,
            f"Player_{i}",
            random.choice(countries),
            random.choice(platforms),
            bts + timedelta(seconds=random.randint(0, 200 * 86400)),
            random.choice(age_groups),
            random.random() > 0.6,
        )
        for i in range(start, end)
    ]

def generate_levels_chunk(start, end, worlds, difficulties):
    return [
        (
            i,
            f"Level_{i}",
            random.choice(worlds),
            random.choice(difficulties),
            random.randint(60, 600),
            random.randint(10, 500),
            max(1, i - random.randint(0, 3)),
        )
        for i in range(start, end)
    ]

def generate_sessions_chunk(start, end, NPL, bts, platforms):
    rows = []
    for i in range(start, end):
        ss = bts + timedelta(seconds=random.randint(0, 300 * 86400))
        dur = random.randint(60, 7200)
        rows.append((
            i,
            random.randint(1, NPL),
            ss,
            ss + timedelta(seconds=dur),
            random.choice(platforms),
            f"v{random.randint(1, 3)}.{random.randint(0, 9)}",
            random.randint(0, 10),
            random.randint(0, 1000),
        ))
    return rows

def generate_events_chunk(start, end, sess_rows, etypes, NLV):
    rows = []
    for i in range(start, end):
        sess = random.choice(sess_rows)
        et = sess[2] + timedelta(seconds=random.randint(0, 7200))
        rows.append((
            i,
            sess[0],
            sess[1],
            random.choice(etypes),
            et,
            random.randint(1, NLV) if NLV > 0 else 1,
            round(random.uniform(0, 1000), 2),
            f"meta_{i}",
        ))
    return rows

def generate_purchases_chunk(start, end, NPL, bts, itypes, currencies):
    return [
        (
            i,
            random.randint(1, NPL),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(itypes),
            f"Item_{random.randint(1, 50)}",
            round(random.uniform(0.99, 99.99), 2),
            random.choice(currencies),
            random.random() < 0.03,
        )
        for i in range(start, end)
    ]


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

    cpu_count = min(4, os.cpu_count() or 1)
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
