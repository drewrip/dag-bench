import pyarrow as pa
import duckdb, random, sys, os, math
from datetime import datetime, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
sf *= 20
NS = max(3, int(30 * sf))
ND = max(10, int(150 * sf))
NR = max(100, int(200000 * sf))
NML = max(5, int(500 * sf))

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")


def batched_insert(table_name, columns, rows):
    rows = list(rows)
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")


con.execute("""
DROP TABLE IF EXISTS maintenance_logs; DROP TABLE IF EXISTS readings;
DROP TABLE IF EXISTS devices; DROP TABLE IF EXISTS sites;
CREATE TABLE sites(site_id INTEGER PRIMARY KEY, name VARCHAR,
    region VARCHAR, latitude DOUBLE, longitude DOUBLE, timezone VARCHAR);
CREATE TABLE devices(device_id INTEGER PRIMARY KEY, site_id INTEGER,
    device_type VARCHAR, model VARCHAR, firmware VARCHAR,
    installed_date DATE, is_active BOOLEAN);
CREATE TABLE readings(reading_id BIGINT PRIMARY KEY, device_id INTEGER,
    ts TIMESTAMP, temperature_c DOUBLE, humidity_pct DOUBLE,
    pressure_hpa DOUBLE, battery_pct TINYINT, rssi_dbm SMALLINT,
    error_flag BOOLEAN);
CREATE TABLE maintenance_logs(log_id INTEGER PRIMARY KEY, device_id INTEGER,
    log_ts TIMESTAMP, action VARCHAR, technician VARCHAR, notes VARCHAR);
""")

base = datetime(2023, 1, 1)
regions = ["NA", "EU", "APAC", "LATAM"]
dtypes = ["temperature", "humidity", "pressure", "multi", "air_quality"]
actions = ["calibrate", "replace_battery", "firmware_update", "repair", "inspect"]

batched_insert("sites", ['site_id', 'name', 'region', 'latitude', 'longitude', 'timezone'], [
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
batched_insert("devices", ['device_id', 'site_id', 'device_type', 'model', 'firmware', 'installed_date', 'is_active'], [
        (
            i,
            random.randint(1, NS),
            random.choice(dtypes),
            f"Model-{random.choice(['A', 'B', 'C'])}{random.randint(1, 5)}",
            f"v{random.randint(1, 4)}.{random.randint(0, 9)}.{random.randint(0, 99)}",
            (base - timedelta(days=random.randint(0, 730))).date(),
            random.random() > 0.05,
        )
        for i in range(1, ND + 1)
    ],
)

rows = []
for i in range(1, NR + 1):
    t = base + timedelta(seconds=random.randint(0, 180 * 86400))
    rows.append(
        (
            i,
            random.randint(1, ND),
            t,
            round(random.gauss(20, 8), 2),
            round(random.uniform(20, 95), 2),
            round(random.gauss(1013, 15), 2),
            random.randint(5, 100),
            random.randint(-90, -30),
            random.random() < 0.02,
        )
    )
batched_insert("readings", ['reading_id', 'device_id', 'ts', 'temperature_c', 'humidity_pct', 'pressure_hpa', 'battery_pct', 'rssi_dbm', 'error_flag'], rows)
batched_insert("maintenance_logs", ['log_id', 'device_id', 'log_ts', 'action', 'technician', 'notes'], [
        (
            i,
            random.randint(1, ND),
            base + timedelta(hours=random.randint(0, 4320)),
            random.choice(actions),
            f"Tech-{random.randint(1, 20)}",
            f"Performed {random.choice(actions)} on device",
        )
        for i in range(1, NML + 1)
    ],
)
con.close()
print(f"p03 done: sites={NS} devices={ND} readings={NR}")
