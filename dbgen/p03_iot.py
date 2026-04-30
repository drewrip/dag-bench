import duckdb, numpy as np, sys, os, math
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_sites_chunk(start, end, regions):
    size = end - start
    rng = np.random.default_rng(start)
    region_indices = rng.integers(0, len(regions), size)
    lats = rng.uniform(-60, 60, size)
    lons = rng.uniform(-180, 180, size)
    tz_list = ["UTC", "US/Eastern", "Europe/Berlin", "Asia/Tokyo"]
    tz_indices = rng.integers(0, len(tz_list), size)
    
    site_ids = range(start, end)
    site_names = [f"Site-{i}" for i in site_ids]
    selected_regions = np.take(regions, region_indices).tolist()
    lats_rounded = np.round(lats, 4).tolist()
    lons_rounded = np.round(lons, 4).tolist()
    selected_tzs = np.take(tz_list, tz_indices).tolist()
    
    return list(zip(site_ids, site_names, selected_regions, lats_rounded, lons_rounded, selected_tzs))


def generate_devices_chunk(start, end, NS, dtypes, base):
    size = end - start
    rng = np.random.default_rng(start)
    site_ids = rng.integers(1, NS + 1, size)
    dtype_indices = rng.integers(0, len(dtypes), size)
    model_letters = ['A', 'B', 'C']
    model_letter_indices = rng.integers(0, len(model_letters), size)
    model_numbers = rng.integers(1, 6, size)
    v_majors = rng.integers(1, 5, size)
    v_minors = rng.integers(0, 10, size)
    v_patches = rng.integers(0, 100, size)
    days_back = rng.integers(0, 731, size)
    active_probs = rng.random(size)

    device_ids = range(start, end)
    selected_site_ids = site_ids.tolist()
    selected_dtypes = np.take(dtypes, dtype_indices).tolist()
    
    models = [f"Model-{model_letters[l]}{n}" for l, n in zip(model_letter_indices, model_numbers)]
    firmwares = [f"v{ma}.{mi}.{p}" for ma, mi, p in zip(v_majors, v_minors, v_patches)]
    installed_dates = (np.datetime64(base) - days_back.astype("timedelta64[D]")).astype("datetime64[D]").tolist()
    is_active = (active_probs > 0.05).tolist()
    
    return list(zip(device_ids, selected_site_ids, selected_dtypes, models, firmwares, installed_dates, is_active))


def generate_readings_chunk(start, end, ND, base):
    size = end - start
    rng = np.random.default_rng(start)
    device_ids = rng.integers(1, ND + 1, size)
    seconds_offset = rng.integers(0, 180 * 86400 + 1, size)
    temps = rng.normal(20, 8, size)
    humids = rng.uniform(20, 95, size)
    pressures = rng.normal(1013, 15, size)
    batteries = rng.integers(5, 101, size)
    rssis = rng.integers(-90, -29, size)
    error_probs = rng.random(size)

    reading_ids = range(start, end)
    selected_device_ids = device_ids.tolist()
    tss = (np.datetime64(base) + seconds_offset.astype("timedelta64[s]")).tolist()
    temps_rounded = np.round(temps, 2).tolist()
    humids_rounded = np.round(humids, 2).tolist()
    pressures_rounded = np.round(pressures, 2).tolist()
    selected_batteries = batteries.tolist()
    selected_rssis = rssis.tolist()
    error_flags = (error_probs < 0.02).tolist()
    
    return list(zip(reading_ids, selected_device_ids, tss, temps_rounded, humids_rounded, pressures_rounded, selected_batteries, selected_rssis, error_flags))


def generate_maintenance_logs_chunk(start, end, ND, base, actions):
    size = end - start
    rng = np.random.default_rng(start)
    device_ids = rng.integers(1, ND + 1, size)
    hours_offset = rng.integers(0, 4321, size)
    action_indices = rng.integers(0, len(actions), size)
    tech_ids = rng.integers(1, 21, size)
    note_action_indices = rng.integers(0, len(actions), size)

    log_ids = range(start, end)
    selected_device_ids = device_ids.tolist()
    log_tss = (np.datetime64(base) + hours_offset.astype("timedelta64[h]")).tolist()
    selected_actions = np.take(actions, action_indices).tolist()
    technicians = [f"Tech-{tid}" for tid in tech_ids]
    notes = [f"Performed {actions[idx]} on device" for idx in note_action_indices]
    
    return list(zip(log_ids, selected_device_ids, log_tss, selected_actions, technicians, notes))

def generate_devices_chunk(start, end, NS, dtypes, base):
    size = end - start
    rng = np.random.default_rng(start)
    site_ids = rng.integers(1, NS + 1, size)
    dtype_indices = rng.integers(0, len(dtypes), size)
    model_letters = ['A', 'B', 'C']
    model_letter_indices = rng.integers(0, len(model_letters), size)
    model_numbers = rng.integers(1, 6, size)
    v_majors = rng.integers(1, 5, size)
    v_minors = rng.integers(0, 10, size)
    v_patches = rng.integers(0, 100, size)
    days_back = rng.integers(0, 731, size)
    active_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(site_ids[idx]),
            dtypes[dtype_indices[idx]],
            f"Model-{model_letters[model_letter_indices[idx]]}{model_numbers[idx]}",
            f"v{v_majors[idx]}.{v_minors[idx]}.{v_patches[idx]}",
            (base - timedelta(days=int(days_back[idx]))).date(),
            bool(active_probs[idx] > 0.05),
        ))
    return rows

def generate_readings_chunk(start, end, ND, base):
    size = end - start
    rng = np.random.default_rng(start)
    device_ids = rng.integers(1, ND + 1, size)
    seconds_offset = rng.integers(0, 180 * 86400 + 1, size)
    temps = rng.normal(20, 8, size)
    humids = rng.uniform(20, 95, size)
    pressures = rng.normal(1013, 15, size)
    batteries = rng.integers(5, 101, size)
    rssis = rng.integers(-90, -29, size)
    error_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(device_ids[idx]),
            base + timedelta(seconds=int(seconds_offset[idx])),
            round(float(temps[idx]), 2),
            round(float(humids[idx]), 2),
            round(float(pressures[idx]), 2),
            int(batteries[idx]),
            int(rssis[idx]),
            bool(error_probs[idx] < 0.02),
        ))
    return rows

def generate_maintenance_logs_chunk(start, end, ND, base, actions):
    size = end - start
    rng = np.random.default_rng(start)
    device_ids = rng.integers(1, ND + 1, size)
    hours_offset = rng.integers(0, 4321, size)
    action_indices = rng.integers(0, len(actions), size)
    tech_ids = rng.integers(1, 21, size)
    note_action_indices = rng.integers(0, len(actions), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        action = actions[action_indices[idx]]
        rows.append((
            i,
            int(device_ids[idx]),
            base + timedelta(hours=int(hours_offset[idx])),
            action,
            f"Tech-{tech_ids[idx]}",
            f"Performed {actions[note_action_indices[idx]]} on device",
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 220.0
    NS = max(3, int(30 * sf_adj))
    ND = max(10, int(150 * sf_adj))
    NR = max(100, int(200000 * sf_adj))
    NML = max(5, int(500 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

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

    cpu_count = get_worker_count()
    progress = GenerationProgress("p03_iot", 4)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("sites")
        batched_insert(con, "sites", ['site_id', 'name', 'region', 'latitude', 'longitude', 'timezone'],
                       run_parallel(executor, generate_sites_chunk, NS, regions))
        progress.advance("devices")
        batched_insert(con, "devices", ['device_id', 'site_id', 'device_type', 'model', 'firmware', 'installed_date', 'is_active'],
                       run_parallel(executor, generate_devices_chunk, ND, NS, dtypes, base))
        progress.advance("readings")
        batched_insert(con, "readings", ['reading_id', 'device_id', 'ts', 'temperature_c', 'humidity_pct', 'pressure_hpa', 'battery_pct', 'rssi_dbm', 'error_flag'],
                       run_parallel(executor, generate_readings_chunk, NR, ND, base))
        progress.advance("maintenance_logs")
        batched_insert(con, "maintenance_logs", ['log_id', 'device_id', 'log_ts', 'action', 'technician', 'notes'],
                       run_parallel(executor, generate_maintenance_logs_chunk, NML, ND, base, actions))


    con.close()
    print_generation_summary(
        "p03_iot",
        sf,
        {
            "sites": NS,
            "devices": ND,
            "readings": NR,
            "maintenance_logs": NML,
        },
    )

if __name__ == "__main__":
    main()
