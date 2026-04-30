import duckdb, sys, os
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_substations_chunk(start, end, regions):
    rng = np.random.default_rng(start)
    size = end - start
    reg_idx = rng.integers(0, len(regions), size)
    cap_rand = rng.uniform(10, 500, size)
    volt_idx = rng.integers(0, 5, size)
    lat_rand = rng.uniform(25, 50, size)
    lon_rand = rng.uniform(-120, -70, size)
    voltages = [11, 33, 66, 110, 132]
    return [
        (
            i,
            f"SUB-{i:03d}",
            regions[reg_idx[i - start]],
            round(float(cap_rand[i - start]), 2),
            voltages[volt_idx[i - start]],
            round(float(lat_rand[i - start]), 4),
            round(float(lon_rand[i - start]), 4),
        )
        for i in range(start, end)
    ]


def generate_meters_chunk(start, end, NSB, NMT, mtypes, tariffs, base):
    rng = np.random.default_rng(start)
    size = end - start
    sub_rand = rng.integers(1, NSB + 1, size)
    cust_rand = rng.integers(1, NMT * 2 + 1, size)
    type_idx = rng.integers(0, len(mtypes), size)
    tar_idx = rng.integers(0, len(tariffs), size)
    days_rand = rng.integers(0, 3651, size)
    smart_rand = rng.random(size)
    cap_rand = rng.uniform(1, 1000, size)
    return [
        (
            i,
            int(sub_rand[i - start]),
            int(cust_rand[i - start]),
            mtypes[type_idx[i - start]],
            tariffs[tar_idx[i - start]],
            base - timedelta(days=int(days_rand[i - start])),
            bool(smart_rand[i - start] > 0.3),
            round(float(cap_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]


def generate_consumption_readings_chunk(start, end, NMT, bts):
    rng = np.random.default_rng(start)
    size = end - start
    meter_rand = rng.integers(1, NMT + 1, size)
    sec_rand = rng.integers(0, 364 * 86400 + 1, size)
    kwh_rand = rng.normal(5, 3, size)
    volt_rand = rng.normal(230, 5, size)
    pf_rand = rng.uniform(0.7, 1, size)
    est_rand = rng.random(size)
    return [
        (
            i,
            int(meter_rand[i - start]),
            bts + timedelta(seconds=int(sec_rand[i - start])),
            round(float(abs(kwh_rand[i - start])), 4),
            round(float(volt_rand[i - start]), 2),
            round(float(pf_rand[i - start]), 3),
            bool(est_rand[i - start] < 0.02),
        )
        for i in range(start, end)
    ]


def generate_outage_events_chunk(start, end, NSB, bts, causes, sevs):
    rng = np.random.default_rng(start)
    size = end - start
    sub_rand = rng.integers(1, NSB + 1, size)
    start_sec = rng.integers(0, 364 * 86400 + 1, size)
    dur_min = rng.integers(5, 1441, size)
    cause_idx = rng.integers(0, len(causes), size)
    aff_rand = rng.integers(1, 501, size)
    sev_idx = rng.integers(0, len(sevs), size)
    return [
        (
            i,
            int(sub_rand[i - start]),
            (st := bts + timedelta(seconds=int(start_sec[i - start]))),
            st + timedelta(minutes=int(dur_min[i - start])),
            causes[cause_idx[i - start]],
            int(aff_rand[i - start]),
            sevs[sev_idx[i - start]],
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    NSB, NMT, NCR, NOE = (
        max(a, int(b * sf_adj))
        for a, b in [(5, 50), (20, 1000), (200, 500000), (5, 200)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS outage_events; DROP TABLE IF EXISTS consumption_readings;
    DROP TABLE IF EXISTS meters; DROP TABLE IF EXISTS substations;
    CREATE TABLE substations(sub_id INTEGER PRIMARY KEY,name VARCHAR,region VARCHAR,
      capacity_mw DECIMAL(10,2),voltage_kv INTEGER,lat DOUBLE,lon DOUBLE);
    CREATE TABLE meters(meter_id INTEGER PRIMARY KEY,sub_id INTEGER,customer_id INTEGER,
      meter_type VARCHAR,tariff_class VARCHAR,install_date DATE,is_smart BOOLEAN,
      rated_capacity_kw DECIMAL(8,2));
    CREATE TABLE consumption_readings(reading_id BIGINT PRIMARY KEY,meter_id INTEGER,
      read_ts TIMESTAMP,kwh DECIMAL(12,4),voltage_v DOUBLE,power_factor DOUBLE,is_estimated BOOLEAN);
    CREATE TABLE outage_events(outage_id INTEGER PRIMARY KEY,sub_id INTEGER,start_ts TIMESTAMP,
      end_ts TIMESTAMP,cause VARCHAR,affected_meters INTEGER,severity VARCHAR);
    """)
    bts = datetime(2023, 1, 1)
    base = date(2023, 1, 1)
    regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
    mtypes = ["residential", "commercial", "industrial", "municipal"]
    tariffs = ["standard", "time_of_use", "demand", "green", "low_income"]
    causes = ["equipment_failure", "weather", "third_party", "maintenance", "unknown"]
    sevs = ["minor", "moderate", "major", "critical"]

    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "substations",
            ["sub_id", "name", "region", "capacity_mw", "voltage_kv", "lat", "lon"],
            run_parallel(executor, generate_substations_chunk, NSB, regions),
        )

        batched_insert(
            con,
            "meters",
            [
                "meter_id",
                "sub_id",
                "customer_id",
                "meter_type",
                "tariff_class",
                "install_date",
                "is_smart",
                "rated_capacity_kw",
            ],
            run_parallel(
                executor, generate_meters_chunk, NMT, NSB, NMT, mtypes, tariffs, base
            ),
        )

        batched_insert(
            con,
            "consumption_readings",
            [
                "reading_id",
                "meter_id",
                "read_ts",
                "kwh",
                "voltage_v",
                "power_factor",
                "is_estimated",
            ],
            run_parallel(executor, generate_consumption_readings_chunk, NCR, NMT, bts),
        )

        batched_insert(
            con,
            "outage_events",
            [
                "outage_id",
                "sub_id",
                "start_ts",
                "end_ts",
                "cause",
                "affected_meters",
                "severity",
            ],
            run_parallel(
                executor, generate_outage_events_chunk, NOE, NSB, bts, causes, sevs
            ),
        )

    con.close()
    print(f"p10 done substations={NSB} meters={NMT} readings={NCR}")


if __name__ == "__main__":
    main()
