import csv, duckdb, random, sys, os, tempfile
from datetime import datetime, timedelta, date

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
sf *= 10
NPL = max(20, int(2000 * sf))
NSS = max(50, int(10000 * sf))
NEV = max(200, int(80000 * sf))
NPU = max(10, int(3000 * sf))
NLV = max(10, int(50 * sf))

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")


def batched_insert(sql, rows):
    rows = list(rows)
    if not rows:
        return
    table_name = sql.split()[2]
    with tempfile.NamedTemporaryFile(
        "w", newline="", suffix=".csv", delete=False
    ) as tmp:
        csv.writer(tmp).writerows(rows)
        temp_path = tmp.name
    try:
        con.execute(f"COPY {table_name} FROM '{temp_path}' (FORMAT CSV)")
    finally:
        os.unlink(temp_path)


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
con.execute("BEGIN")

bts = datetime(2023, 1, 1)
countries = ["US", "CN", "DE", "JP", "BR", "KR", "RU", "GB", "CA", "FR"]
platforms = ["PC", "Mobile_iOS", "Mobile_Android", "Console_PS", "Console_Xbox"]
age_groups = ["<13", "13-17", "18-24", "25-34", "35-44", "45+"]
worlds = ["Forest", "Desert", "Ocean", "Space", "Underground", "Sky"]
difficulties = ["easy", "normal", "hard", "nightmare"]
etypes = [
    "level_start",
    "level_complete",
    "level_fail",
    "achievement",
    "item_pickup",
    "death",
    "checkpoint",
    "boss_kill",
]
itypes = ["coin_pack", "skin", "level_skip", "power_up", "subscription", "loot_box"]
currencies = ["USD", "EUR", "GBP", "JPY", "BRL"]

batched_insert(
    "INSERT INTO players VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            f"Player_{i}",
            random.choice(countries),
            random.choice(platforms),
            bts + timedelta(seconds=random.randint(0, 200 * 86400)),
            random.choice(age_groups),
            random.random() > 0.6,
        )
        for i in range(1, NPL + 1)
    ],
)
batched_insert(
    "INSERT INTO levels VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            f"Level_{i}",
            random.choice(worlds),
            random.choice(difficulties),
            random.randint(60, 600),
            random.randint(10, 500),
            max(1, i - random.randint(0, 3)),
        )
        for i in range(1, NLV + 1)
    ],
)

sess_rows = []
for i in range(1, NSS + 1):
    ss = bts + timedelta(seconds=random.randint(0, 300 * 86400))
    dur = random.randint(60, 7200)
    sess_rows.append(
        (
            i,
            random.randint(1, NPL),
            ss,
            ss + timedelta(seconds=dur),
            random.choice(platforms),
            f"v{random.randint(1, 3)}.{random.randint(0, 9)}",
            random.randint(0, 10),
            random.randint(0, 1000),
        )
    )
batched_insert("INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?)", sess_rows)

ev_rows = []
for i in range(1, NEV + 1):
    sess = random.choice(sess_rows)
    et = sess[2] + timedelta(seconds=random.randint(0, 7200))
    ev_rows.append(
        (
            i,
            sess[0],
            sess[1],
            random.choice(etypes),
            et,
            random.randint(1, NLV) if NLV > 0 else 1,
            round(random.uniform(0, 1000), 2),
            f"meta_{i}",
        )
    )
batched_insert("INSERT INTO events VALUES(?,?,?,?,?,?,?,?)", ev_rows)

batched_insert(
    "INSERT INTO purchases VALUES(?,?,?,?,?,?,?,?)",
    [
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
        for i in range(1, NPU + 1)
    ],
)
con.commit()
con.close()
print(f"p09 done: players={NPL} sessions={NSS} events={NEV} purchases={NPU}")
