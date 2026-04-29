import pyarrow as pa
import duckdb, random, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor

def generate_sites_chunk(start, end, regions):
    return [
        (
            i,
            f"Site-{i}",
            random.choice(regions),
            round(random.uniform(-60, 60), 4),
            round(random.uniform(-180, 180), 4),
            random.choice(["UTC", "US/Eastern", "Europe/Berlin", "Asia/Tokyo"]),
        )
        for i in range(start, end)
    ]

def generate_devices_chunk(start, end, NS, dtypes, base):
    return [
        (
            i,
            random.randint(1, NS),
            random.choice(dtypes),
            f"Model-{random.choice('ABC')}{random.randint(1, 5)}",
            f"v{random.randint(1, 4)}.{random.randint(0, 9)}",
            (base - timedelta(days=random.randint(0, 730))).date(),
            random.random() > 0.05,
        )
        for i in range(start, end)
    ]

def generate_readings_chunk(start, end, ND, base):
    return [
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
        for i in range(start, end)
    ]

def generate_maintenance_logs_chunk(start, end, ND, base, actions):
    return [
        (
            i,
            random.randint(1, ND),
            base + timedelta(hours=random.randint(0, 4320)),
            random.choice(actions),
            f"Tech-{random.randint(1, 20)}",
        )
        for i in range(start, end)
    ]

def batched_insert(con, table_name, columns, rows):
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")

def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf *= 10
    NS, ND, NR, NML = (
        max(a, int(b * sf)) for a, b in [(3, 30), (10, 150), (100, 200000), (5, 500)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

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
    base = datetime(2023, 1, 1)
    regions = ["NA", "EU", "APAC", "LATAM"]
    dtypes = ["temperature", "humidity", "pressure", "multi", "air_quality"]
    actions = ["calibrate", "replace_battery", "firmware_update", "repair", "inspect"]

    cpu_count = min(4, os.cpu_count() or 1)

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        def run_parallel(gen_func, total, *args):
            chunk_size = max(1, total // cpu_count)
            futures = []
            for i in range(0, total, chunk_size):
                futures.append(executor.submit(gen_func, i + 1, min(i + chunk_size + 1, total + 1), *args))
            rows = []
            for f in futures:
                rows.extend(f.result())
            return rows

        batched_insert(con, "sites", ['site_id', 'name', 'region', 'latitude', 'longitude', 'timezone'], 
                       run_parallel(generate_sites_chunk, NS, regions))
        
        batched_insert(con, "devices", ['device_id', 'site_id', 'device_type', 'model', 'firmware', 'installed_date', 'is_active'],
                       run_parallel(generate_devices_chunk, ND, NS, dtypes, base))
        
        batched_insert(con, "readings", ['reading_id', 'device_id', 'ts', 'temperature_c', 'humidity_pct', 'pressure_hpa', 'battery_pct', 'rssi_dbm', 'error_flag'],
                       run_parallel(generate_readings_chunk, NR, ND, base))
        
        batched_insert(con, "maintenance_logs", ['log_id', 'device_id', 'log_ts', 'action', 'technician'],
                       run_parallel(generate_maintenance_logs_chunk, NML, ND, base, actions))

    con.close()
    print(f"p03 done sites={NS} devices={ND} readings={NR}")

if __name__ == "__main__":
    main()
