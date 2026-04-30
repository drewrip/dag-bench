import duckdb, sys, os
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_sites_chunk(start, end, regions):
    rng = np.random.default_rng(start)
    size = end - start
    reg_idx = rng.integers(0, len(regions), size)
    lat_rand = rng.uniform(-60, 60, size)
    lon_rand = rng.uniform(-180, 180, size)
    tz_idx = rng.integers(0, 4, size)
    timezones = ["UTC", "US/Eastern", "Europe/Berlin", "Asia/Tokyo"]
    return [
        (
            i,
            f"Site-{i}",
            regions[reg_idx[i - start]],
            round(float(lat_rand[i - start]), 4),
            round(float(lon_rand[i - start]), 4),
            timezones[tz_idx[i - start]],
        )
        for i in range(start, end)
    ]

def generate_devices_chunk(start, end, NS, dtypes, base):
    rng = np.random.default_rng(start)
    size = end - start
    site_rand = rng.integers(1, NS + 1, size)
    dtype_idx = rng.integers(0, len(dtypes), size)
    model_abc = rng.integers(0, 3, size)
    model_num = rng.integers(1, 6, size)
    firm_maj = rng.integers(1, 5, size)
    firm_min = rng.integers(0, 10, size)
    days_rand = rng.integers(0, 731, size)
    active_rand = rng.random(size)
    abc = "ABC"
    return [
        (
            i,
            int(site_rand[i - start]),
            dtypes[dtype_idx[i - start]],
            f"Model-{abc[model_abc[i - start]]}{model_num[i - start]}",
            f"v{firm_maj[i - start]}.{firm_min[i - start]}",
            (base - timedelta(days=int(days_rand[i - start]))).date(),
            bool(active_rand[i - start] > 0.05),
        )
        for i in range(start, end)
    ]

def generate_readings_chunk(start, end, ND, base):
    rng = np.random.default_rng(start)
    size = end - start
    dev_rand = rng.integers(1, ND + 1, size)
    sec_rand = rng.integers(0, 180 * 86400 + 1, size)
    temp_rand = rng.normal(20, 8, size)
    hum_rand = rng.uniform(20, 95, size)
    pres_rand = rng.normal(1013, 15, size)
    bat_rand = rng.integers(5, 101, size)
    rssi_rand = rng.integers(-90, -29, size)
    err_rand = rng.random(size)
    return [
        (
            i,
            int(dev_rand[i - start]),
            base + timedelta(seconds=int(sec_rand[i - start])),
            round(float(temp_rand[i - start]), 2),
            round(float(hum_rand[i - start]), 2),
            round(float(pres_rand[i - start]), 2),
            int(bat_rand[i - start]),
            int(rssi_rand[i - start]),
            bool(err_rand[i - start] < 0.02),
        )
        for i in range(start, end)
    ]

def generate_maintenance_logs_chunk(start, end, ND, base, actions):
    rng = np.random.default_rng(start)
    size = end - start
    dev_rand = rng.integers(1, ND + 1, size)
    hour_rand = rng.integers(0, 4321, size)
    act_idx = rng.integers(0, len(actions), size)
    tech_rand = rng.integers(1, 21, size)
    return [
        (
            i,
            int(dev_rand[i - start]),
            base + timedelta(hours=int(hour_rand[i - start])),
            actions[act_idx[i - start]],
            f"Tech-{tech_rand[i - start]}",
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 10.0
    NS, ND, NR, NML = (
        max(a, int(b * sf_adj)) for a, b in [(3, 30), (10, 150), (100, 200000), (5, 500)]
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

    cpu_count = os.cpu_count()
    
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "sites", ['site_id', 'name', 'region', 'latitude', 'longitude', 'timezone'], 
                       run_parallel(executor, generate_sites_chunk, NS, regions))
        
        batched_insert(con, "devices", ['device_id', 'site_id', 'device_type', 'model', 'firmware', 'installed_date', 'is_active'],
                       run_parallel(executor, generate_devices_chunk, ND, NS, dtypes, base))
        
        batched_insert(con, "readings", ['reading_id', 'device_id', 'ts', 'temperature_c', 'humidity_pct', 'pressure_hpa', 'battery_pct', 'rssi_dbm', 'error_flag'],
                       run_parallel(executor, generate_readings_chunk, NR, ND, base))
        
        batched_insert(con, "maintenance_logs", ['log_id', 'device_id', 'log_ts', 'action', 'technician'],
                       run_parallel(executor, generate_maintenance_logs_chunk, NML, ND, base, actions))


    con.close()
    print(f"p03 done sites={NS} devices={ND} readings={NR}")

if __name__ == "__main__":
    main()
