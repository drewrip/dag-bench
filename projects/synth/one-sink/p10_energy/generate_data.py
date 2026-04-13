import csv, duckdb, random, sys, os, tempfile
from datetime import datetime, timedelta, date

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
sf *= 10
NSB = max(5, int(50 * sf))
NMT = max(20, int(1000 * sf))
NCR = max(200, int(500000 * sf))
NOE = max(5, int(200 * sf))

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
DROP TABLE IF EXISTS outage_events; DROP TABLE IF EXISTS consumption_readings;
DROP TABLE IF EXISTS meters; DROP TABLE IF EXISTS substations;
CREATE TABLE substations(sub_id INTEGER PRIMARY KEY, name VARCHAR,
    region VARCHAR, capacity_mw DECIMAL(10,2), voltage_kv INTEGER,
    lat DOUBLE, lon DOUBLE);
CREATE TABLE meters(meter_id INTEGER PRIMARY KEY, sub_id INTEGER,
    customer_id INTEGER, meter_type VARCHAR, tariff_class VARCHAR,
    install_date DATE, is_smart BOOLEAN, rated_capacity_kw DECIMAL(8,2));
CREATE TABLE consumption_readings(reading_id BIGINT PRIMARY KEY,
    meter_id INTEGER, read_ts TIMESTAMP, kwh DECIMAL(12,4),
    voltage_v DOUBLE, power_factor DOUBLE, is_estimated BOOLEAN);
CREATE TABLE outage_events(outage_id INTEGER PRIMARY KEY, sub_id INTEGER,
    start_ts TIMESTAMP, end_ts TIMESTAMP, cause VARCHAR,
    affected_meters INTEGER, severity VARCHAR);
""")
con.execute("BEGIN")

bts = datetime(2023, 1, 1)
base = date(2023, 1, 1)
regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
mtypes = ["residential", "commercial", "industrial", "municipal"]
tariffs = ["standard", "time_of_use", "demand", "green", "low_income"]
causes = ["equipment_failure", "weather", "third_party", "maintenance", "unknown"]
severities = ["minor", "moderate", "major", "critical"]

batched_insert(
    "INSERT INTO substations VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            f"SUB-{i:03d}",
            random.choice(regions),
            round(random.uniform(10, 500), 2),
            random.choice([11, 33, 66, 110, 132, 220]),
            round(random.uniform(25, 50), 4),
            round(random.uniform(-120, -70), 4),
        )
        for i in range(1, NSB + 1)
    ],
)
batched_insert(
    "INSERT INTO meters VALUES(?,?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NSB),
            random.randint(1, NMT * 2),
            random.choice(mtypes),
            random.choice(tariffs),
            base - timedelta(days=random.randint(0, 3650)),
            random.random() > 0.3,
            round(random.uniform(1, 1000), 2),
        )
        for i in range(1, NMT + 1)
    ],
)

read_rows = []
for i in range(1, NCR + 1):
    read_rows.append(
        (
            i,
            random.randint(1, NMT),
            bts + timedelta(seconds=random.randint(0, 364 * 86400)),
            round(abs(random.gauss(5, 3)), 4),
            round(random.gauss(230, 5), 2),
            round(random.uniform(0.7, 1.0), 3),
            random.random() < 0.02,
        )
    )
batched_insert("INSERT INTO consumption_readings VALUES(?,?,?,?,?,?,?)", read_rows)

batched_insert(
    "INSERT INTO outage_events VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NSB),
            st := bts + timedelta(seconds=random.randint(0, 364 * 86400)),
            st + timedelta(minutes=random.randint(5, 1440)),
            random.choice(causes),
            random.randint(1, 500),
            random.choice(severities),
        )
        for i in range(1, NOE + 1)
    ],
)
con.commit()
con.close()
print(f"p10 done: substations={NSB} meters={NMT} readings={NCR} outages={NOE}")
