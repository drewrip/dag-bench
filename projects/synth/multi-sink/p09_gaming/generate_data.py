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

def generate_levels_chunk(start, end, worlds, diffs):
    return [
        (
            i,
            f"Level_{i}",
            random.choice(worlds),
            random.choice(diffs),
            random.randint(60, 600),
            random.randint(10, 500),
        )
        for i in range(start, end)
    ]

def generate_sessions_chunk(start, end, NPL, bts, platforms):
    return [
        (
            i,
            random.randint(1, NPL),
            ss := bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            ss + timedelta(seconds=random.randint(60, 7200)),
            random.choice(platforms),
            f"v{random.randint(1, 3)}.{random.randint(0, 9)}",
            random.randint(0, 1000),
        )
        for i in range(start, end)
    ]

def generate_events_chunk(start, end, sess, etypes, NLV):
    rows = []
    for i in range(start, end):
        s = random.choice(sess)
        rows.append(
            (
                i,
                s[0],
                s[1],
                random.choice(etypes),
                s[2] + timedelta(seconds=random.randint(0, 7200)),
                random.randint(1, NLV) if NLV > 0 else 1,
                round(random.uniform(0, 1000), 2),
            )
        )
    return rows

def generate_purchases_chunk(start, end, NPL, bts, itypes):
    return [
        (
            i,
            random.randint(1, NPL),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(itypes),
            f"Item_{random.randint(1, 50)}",
            round(random.uniform(0.99, 99.99), 2),
            random.random() < 0.03,
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    NPL, NSS, NEV, NPU, NLV = (
        max(a, int(b * sf))
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

    cpu_count = min(4, os.cpu_count() or 1)
    
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "players", ['player_id', 'username', 'country', 'platform', 'created_ts', 'age_group', 'is_paid_user'], 
                       run_parallel(executor, generate_players_chunk, NPL, countries, platforms, bts, age_groups))
        
        batched_insert(con, "levels", ['level_id', 'level_name', 'world', 'difficulty', 'par_time_sec', 'reward_coins'],
                       run_parallel(executor, generate_levels_chunk, NLV, worlds, diffs))
        
        sess = run_parallel(executor, generate_sessions_chunk, NSS, NPL, bts, platforms)
        batched_insert(con, "sessions", ['session_id', 'player_id', 'session_start', 'session_end', 'platform', 'version', 'coins_earned'], sess)
        
        batched_insert(con, "events", ['event_id', 'session_id', 'player_id', 'event_type', 'event_ts', 'level_id', 'value'],
                       run_parallel(executor, generate_events_chunk, NEV, sess, etypes, NLV))
        
        batched_insert(con, "purchases", ['purchase_id', 'player_id', 'purchase_ts', 'item_type', 'item_name', 'price_usd', 'is_refunded'],
                       run_parallel(executor, generate_purchases_chunk, NPU, NPL, bts, itypes))


    con.close()
    print(f"p09 done players={NPL} sessions={NSS} events={NEV}")

if __name__ == "__main__":
    main()
