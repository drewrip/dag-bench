import duckdb, random, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_substations_chunk(start, end, regions):
    return [
        (
            i,
            f"SUB-{i:03d}",
            random.choice(regions),
            round(random.uniform(10, 500), 2),
            random.choice([11, 33, 66, 110, 132, 220]),
            round(random.uniform(25, 50), 4),
            round(random.uniform(-120, -70), 4),
        )
        for i in range(start, end)
    ]

def generate_meters_chunk(start, end, NSB, mtypes, tariffs, base):
    return [
        (
            i,
            random.randint(1, NSB),
            random.randint(1, 1000 * 2), # Using a dummy max for customer_id if NMT not passed
            random.choice(mtypes),
            random.choice(tariffs),
            base - timedelta(days=random.randint(0, 3650)),
            random.random() > 0.3,
            round(random.uniform(1, 1000), 2),
        )
        for i in range(start, end)
    ]

def generate_readings_chunk(start, end, NMT, bts):
    return [
        (
            i,
            random.randint(1, NMT),
            bts + timedelta(seconds=random.randint(0, 364 * 86400)),
            round(abs(random.gauss(5, 3)), 4),
            round(random.gauss(230, 5), 2),
            round(random.uniform(0.7, 1.0), 3),
            random.random() < 0.02,
        )
        for i in range(start, end)
    ]

def generate_outages_chunk(start, end, NSB, causes, severities, bts):
    rows = []
    for i in range(start, end):
        st = bts + timedelta(seconds=random.randint(0, 364 * 86400))
        rows.append((
            i,
            random.randint(1, NSB),
            st,
            st + timedelta(minutes=random.randint(5, 1440)),
            random.choice(causes),
            random.randint(1, 500),
            random.choice(severities),
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 10.0
    NSB = max(5, int(50 * sf_adj))
    NMT = max(20, int(1000 * sf_adj))
    NCR = max(200, int(500000 * sf_adj))
    NOE = max(5, int(200 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

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

    bts = datetime(2023, 1, 1)
    base = date(2023, 1, 1)
    regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
    mtypes = ["residential", "commercial", "industrial", "municipal"]
    tariffs = ["standard", "time_of_use", "demand", "green", "low_income"]
    causes = ["equipment_failure", "weather", "third_party", "maintenance", "unknown"]
    severities = ["minor", "moderate", "major", "critical"]

    cpu_count = os.cpu_count()
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "substations", ['sub_id', 'name', 'region', 'capacity_mw', 'voltage_kv', 'lat', 'lon'],
                       run_parallel(executor, generate_substations_chunk, NSB, regions))
        batched_insert(con, "meters", ['meter_id', 'sub_id', 'customer_id', 'meter_type', 'tariff_class', 'install_date', 'is_smart', 'rated_capacity_kw'],
                       run_parallel(executor, generate_meters_chunk, NMT, NSB, mtypes, tariffs, base))
        batched_insert(con, "consumption_readings", ['reading_id', 'meter_id', 'read_ts', 'kwh', 'voltage_v', 'power_factor', 'is_estimated'],
                       run_parallel(executor, generate_readings_chunk, NCR, NMT, bts))
        batched_insert(con, "outage_events", ['outage_id', 'sub_id', 'start_ts', 'end_ts', 'cause', 'affected_meters', 'severity'],
                       run_parallel(executor, generate_outages_chunk, NOE, NSB, causes, severities, bts))


    con.close()
    print(f"p10 done: substations={NSB} meters={NMT} readings={NCR} outages={NOE}")

if __name__ == "__main__":
    main()
