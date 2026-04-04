import duckdb, random, sys, os
from datetime import datetime, timedelta, date

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
con.executemany(
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
con.executemany(
    "INSERT INTO levels VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            f"Level_{i}",
            random.choice(worlds),
            random.choice(diffs),
            random.randint(60, 600),
            random.randint(10, 500),
        )
        for i in range(1, NLV + 1)
    ],
)
sess = []
for i in range(1, NSS + 1):
    ss = bts + timedelta(seconds=random.randint(0, 300 * 86400))
    sess.append(
        (
            i,
            random.randint(1, NPL),
            ss,
            ss + timedelta(seconds=random.randint(60, 7200)),
            random.choice(platforms),
            f"v{random.randint(1, 3)}.{random.randint(0, 9)}",
            random.randint(0, 1000),
        )
    )
con.executemany("INSERT INTO sessions VALUES(?,?,?,?,?,?,?)", sess)
ev = []
for i in range(1, NEV + 1):
    s = random.choice(sess)
    ev.append(
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
con.executemany("INSERT INTO events VALUES(?,?,?,?,?,?,?)", ev)
con.executemany(
    "INSERT INTO purchases VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NPL),
            bts + timedelta(seconds=random.randint(0, 300 * 86400)),
            random.choice(itypes),
            f"Item_{random.randint(1, 50)}",
            round(random.uniform(0.99, 99.99), 2),
            random.random() < 0.03,
        )
        for i in range(1, NPU + 1)
    ],
)
con.close()
print(f"p09 done players={NPL} sessions={NSS} events={NEV}")
