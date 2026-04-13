import csv, duckdb, random, sys, os, tempfile
from datetime import datetime, timedelta, date

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
sf *= 10
NS, ND, NR, NML = (
    max(a, int(b * sf)) for a, b in [(3, 30), (10, 150), (100, 200000), (5, 500)]
)
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
DROP TABLE IF EXISTS maintenance_logs; DROP TABLE IF EXISTS readings;
DROP TABLE IF EXISTS devices; DROP TABLE IF EXISTS sites;
CREATE TABLE sites(site_id INTEGER PRIMARY KEY,name VARCHAR,region VARCHAR,
  latitude DOUBLE,longitude DOUBLE,timezone VARCHAR);
CREATE TABLE devices(device_id INTEGER PRIMARY KEY,site_id INTEGER,device_type VARCHAR,
  model VARCHAR,firmware VARCHAR,installed_date DATE,is_active BOOLEAN);
CREATE TABLE readings(reading_id BIGINT PRIMARY KEY,device_id INTEGER,ts TIMESTAMP,
  temperature_c DOUBLE,humidity_pct DOUBLE,pressure_hpa DOUBLE,
  battery_pct TINYINT,rssi_dbm SMALLINT,error_flag BOOLEAN);
CREATE TABLE maintenance_logs(log_id INTEGER PRIMARY KEY,device_id INTEGER,
  log_ts TIMESTAMP,action VARCHAR,technician VARCHAR);
""")
con.execute("BEGIN")
base = datetime(2023, 1, 1)
regions = ["NA", "EU", "APAC", "LATAM"]
dtypes = ["temperature", "humidity", "pressure", "multi", "air_quality"]
actions = ["calibrate", "replace_battery", "firmware_update", "repair", "inspect"]
batched_insert(
    "INSERT INTO sites VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            f"Site-{i}",
            random.choice(regions),
            round(random.uniform(-60, 60), 4),
            round(random.uniform(-180, 180), 4),
            random.choice(["UTC", "US/Eastern", "Europe/Berlin", "Asia/Tokyo"]),
        )
        for i in range(1, NS + 1)
    ],
)
batched_insert(
    "INSERT INTO devices VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NS),
            random.choice(dtypes),
            f"Model-{random.choice('ABC')}{random.randint(1, 5)}",
            f"v{random.randint(1, 4)}.{random.randint(0, 9)}",
            (base - timedelta(days=random.randint(0, 730))).date(),
            random.random() > 0.05,
        )
        for i in range(1, ND + 1)
    ],
)
rows = []
for i in range(1, NR + 1):
    rows.append(
        (
            i,
            random.randint(1, ND),
            base + timedelta(seconds=random.randint(0, 180 * 86400)),
            round(random.gauss(20, 8), 2),
            round(random.uniform(20, 95), 2),
            round(random.gauss(1013, 15), 2),
            random.randint(5, 100),
            random.randint(-90, -30),
            random.random() < 0.02,
        )
    )
batched_insert("INSERT INTO readings VALUES(?,?,?,?,?,?,?,?,?)", rows)
batched_insert(
    "INSERT INTO maintenance_logs VALUES(?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, ND),
            base + timedelta(hours=random.randint(0, 4320)),
            random.choice(actions),
            f"Tech-{random.randint(1, 20)}",
        )
        for i in range(1, NML + 1)
    ],
)
con.commit()
con.close()
print(f"p03 done sites={NS} devices={ND} readings={NR}")
