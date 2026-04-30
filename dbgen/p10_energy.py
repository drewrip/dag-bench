import duckdb, numpy as np, sys, os
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_substations_chunk(start, end, regions):
    size = end - start
    rng = np.random.default_rng(start)
    region_indices = rng.integers(0, len(regions), size)
    capacities = rng.uniform(10, 500, size)
    voltages = [11, 33, 66, 110, 132, 220]
    voltage_indices = rng.integers(0, len(voltages), size)
    lats = rng.uniform(25, 50, size)
    lons = rng.uniform(-120, -70, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"SUB-{i:03d}",
            regions[region_indices[idx]],
            round(float(capacities[idx]), 2),
            int(voltages[voltage_indices[idx]]),
            round(float(lats[idx]), 4),
            round(float(lons[idx]), 4),
        ))
    return rows

def generate_meters_chunk(start, end, NSB, mtypes, tariffs, base):
    size = end - start
    rng = np.random.default_rng(start)
    sub_ids = rng.integers(1, NSB + 1, size)
    customer_ids = rng.integers(1, 1000 * 2 + 1, size)
    mtype_indices = rng.integers(0, len(mtypes), size)
    tariff_indices = rng.integers(0, len(tariffs), size)
    days_back = rng.integers(0, 3651, size)
    smart_probs = rng.random(size)
    rated_capacities = rng.uniform(1, 1000, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(sub_ids[idx]),
            int(customer_ids[idx]),
            mtypes[mtype_indices[idx]],
            tariffs[tariff_indices[idx]],
            base - timedelta(days=int(days_back[idx])),
            bool(smart_probs[idx] > 0.3),
            round(float(rated_capacities[idx]), 2),
        ))
    return rows

def generate_readings_chunk(start, end, NMT, bts):
    size = end - start
    rng = np.random.default_rng(start)
    meter_ids = rng.integers(1, NMT + 1, size)
    seconds_offset = rng.integers(0, 364 * 86400 + 1, size)
    kwhs = np.abs(rng.normal(5, 3, size))
    voltages = rng.normal(230, 5, size)
    power_factors = rng.uniform(0.7, 1.0, size)
    estimated_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(meter_ids[idx]),
            bts + timedelta(seconds=int(seconds_offset[idx])),
            round(float(kwhs[idx]), 4),
            round(float(voltages[idx]), 2),
            round(float(power_factors[idx]), 3),
            bool(estimated_probs[idx] < 0.02),
        ))
    return rows

def generate_outages_chunk(start, end, NSB, causes, severities, bts):
    size = end - start
    rng = np.random.default_rng(start)
    sub_ids = rng.integers(1, NSB + 1, size)
    seconds_offset = rng.integers(0, 364 * 86400 + 1, size)
    duration_minutes = rng.integers(5, 1441, size)
    cause_indices = rng.integers(0, len(causes), size)
    affected_meters = rng.integers(1, 501, size)
    severity_indices = rng.integers(0, len(severities), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        st = bts + timedelta(seconds=int(seconds_offset[idx]))
        rows.append((
            i,
            int(sub_ids[idx]),
            st,
            st + timedelta(minutes=int(duration_minutes[idx])),
            causes[cause_indices[idx]],
            int(affected_meters[idx]),
            severities[severity_indices[idx]],
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

    cpu_count = get_worker_count()
    progress = GenerationProgress("p10_energy", 4)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("substations")
        batched_insert(con, "substations", ['sub_id', 'name', 'region', 'capacity_mw', 'voltage_kv', 'lat', 'lon'],
                       run_parallel(executor, generate_substations_chunk, NSB, regions))
        progress.advance("meters")
        batched_insert(con, "meters", ['meter_id', 'sub_id', 'customer_id', 'meter_type', 'tariff_class', 'install_date', 'is_smart', 'rated_capacity_kw'],
                       run_parallel(executor, generate_meters_chunk, NMT, NSB, mtypes, tariffs, base))
        progress.advance("consumption_readings")
        batched_insert(con, "consumption_readings", ['reading_id', 'meter_id', 'read_ts', 'kwh', 'voltage_v', 'power_factor', 'is_estimated'],
                       run_parallel(executor, generate_readings_chunk, NCR, NMT, bts))
        progress.advance("outage_events")
        batched_insert(con, "outage_events", ['outage_id', 'sub_id', 'start_ts', 'end_ts', 'cause', 'affected_meters', 'severity'],
                       run_parallel(executor, generate_outages_chunk, NOE, NSB, causes, severities, bts))


    con.close()
    print_generation_summary(
        "p10_energy",
        sf,
        {
            "substations": NSB,
            "meters": NMT,
            "consumption_readings": NCR,
            "outage_events": NOE,
        },
    )

if __name__ == "__main__":
    main()
