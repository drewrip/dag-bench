import duckdb, sys, os
import numpy as np
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_patients_chunk(start, end, plans, states, base):
    rng = np.random.default_rng(start)
    size = end - start
    dob_days = rng.integers(365 * 5, 365 * 85 + 1, size)
    gen_idx = rng.integers(0, 3, size)
    zip_rand = rng.integers(10000, 100000, size)
    plan_idx = rng.integers(0, len(plans), size)
    state_idx = rng.integers(0, len(states), size)
    genders = ["M", "F", "U"]
    return [
        (
            i,
            base - timedelta(days=int(dob_days[i - start])),
            genders[gen_idx[i - start]],
            f"{zip_rand[i - start]}",
            plans[plan_idx[i - start]],
            states[state_idx[i - start]],
        )
        for i in range(start, end)
    ]


def generate_providers_chunk(start, end, specs, states):
    rng = np.random.default_rng(start)
    size = end - start
    spec_idx = rng.integers(0, len(specs), size)
    state_idx = rng.integers(0, len(states), size)
    net_rand = rng.random(size)
    return [
        (
            i,
            f"Provider {i}",
            specs[spec_idx[i - start]],
            states[state_idx[i - start]],
            bool(net_rand[i - start] > 0.2),
            f"NPI{i:010d}",
        )
        for i in range(start, end)
    ]


def generate_claims_chunk(start, end, NPA, NPR, base, ctypes, cstats, dreasons):
    rng = np.random.default_rng(start)
    size = end - start
    bill_rand = rng.uniform(100, 50000, size)
    allow_mult = rng.uniform(0.4, 0.95, size)
    paid_mult = rng.uniform(0.5, 1, size)
    paid_prob = rng.random(size)
    pat_rand = rng.integers(1, NPA + 1, size)
    prov_rand = rng.integers(1, NPR + 1, size)
    days_rand = rng.integers(0, 1096, size)
    type_idx = rng.integers(0, len(ctypes), size)
    stat_idx = rng.integers(0, len(cstats), size)
    reason_idx = rng.integers(0, len(dreasons), size)
    
    rows = []
    for i in range(start, end):
        idx = i - start
        bill = round(float(bill_rand[idx]), 2)
        allow = round(bill * float(allow_mult[idx]), 2)
        paid = round(allow * float(paid_mult[idx]) if paid_prob[idx] > 0.1 else 0, 2)
        rows.append(
            (
                i,
                int(pat_rand[idx]),
                int(prov_rand[idx]),
                base + timedelta(days=int(days_rand[idx])),
                ctypes[type_idx[idx]],
                bill,
                allow,
                paid,
                cstats[stat_idx[idx]],
                dreasons[reason_idx[idx]],
            )
        )
    return rows


def generate_claim_lines_chunk(start, end, NCL, cpts):
    rng = np.random.default_rng(start)
    size = end - start
    claim_rand = rng.integers(1, NCL + 1, size)
    cpt_idx = rng.integers(0, len(cpts), size)
    qty_rand = rng.integers(1, 6, size)
    cost_rand = rng.uniform(10, 5000, size)
    allow_rand = rng.uniform(5, 4000, size)
    paid_rand = rng.uniform(0, 3500, size)
    return [
        (
            i,
            int(claim_rand[i - start]),
            cpts[cpt_idx[i - start]],
            int(qty_rand[i - start]),
            round(float(cost_rand[i - start]), 2),
            round(float(allow_rand[i - start]), 2),
            round(float(paid_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]


def generate_diagnoses_chunk(start, end, NCL, icds):
    rng = np.random.default_rng(start)
    size = end - start
    claim_rand = rng.integers(1, NCL + 1, size)
    icd_idx = rng.integers(0, len(icds), size)
    prim_rand = rng.random(size)
    chron_rand = rng.random(size)
    return [
        (
            i,
            int(claim_rand[i - start]),
            icds[icd_idx[i - start]],
            bool(prim_rand[i - start] > 0.3),
            bool(chron_rand[i - start] > 0.6),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    NPA, NPR, NCL, NCLL, NDX = (
        max(a, int(b * sf_adj))
        for a, b in [(20, 1000), (10, 200), (30, 3000), (50, 9000), (10, 100)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS diagnoses; DROP TABLE IF EXISTS claim_lines;
    DROP TABLE IF EXISTS claims; DROP TABLE IF EXISTS providers; DROP TABLE IF EXISTS patients;
    CREATE TABLE patients(patient_id INTEGER PRIMARY KEY,dob DATE,gender VARCHAR,
      zip_code VARCHAR,plan_type VARCHAR,state VARCHAR);
    CREATE TABLE providers(provider_id INTEGER PRIMARY KEY,name VARCHAR,specialty VARCHAR,
      state VARCHAR,is_in_network BOOLEAN,npi VARCHAR);
    CREATE TABLE claims(claim_id INTEGER PRIMARY KEY,patient_id INTEGER,provider_id INTEGER,
      service_date DATE,claim_type VARCHAR,total_billed DECIMAL(12,2),
      total_allowed DECIMAL(12,2),total_paid DECIMAL(12,2),status VARCHAR,denial_reason VARCHAR);
    CREATE TABLE claim_lines(line_id INTEGER PRIMARY KEY,claim_id INTEGER,cpt_code VARCHAR,
      quantity INTEGER,unit_cost DECIMAL(10,2),allowed_amount DECIMAL(10,2),paid_amount DECIMAL(10,2));
    CREATE TABLE diagnoses(diag_id INTEGER PRIMARY KEY,claim_id INTEGER,icd_code VARCHAR,
      is_primary BOOLEAN,chronic_flag BOOLEAN);
    """)
    base = date(2020, 1, 1)
    plans = ["HMO", "PPO", "EPO", "HDHP", "Medicare", "Medicaid"]
    specs = [
        "internal_medicine",
        "cardiology",
        "oncology",
        "orthopedics",
        "primary_care",
        "emergency",
    ]
    ctypes = ["professional", "facility", "pharmacy", "dental"]
    cstats = ["paid", "denied", "pending", "partial"]
    dreasons = ["not_covered", "prior_auth", "out_of_network", None, None, None]
    cpts = [f"CPT{i:05d}" for i in range(1, 51)]
    icds = [f"ICD{i:04d}" for i in range(1, 101)]
    states = ["CA", "TX", "NY", "FL", "IL", "WA"]

    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "patients",
            ["patient_id", "dob", "gender", "zip_code", "plan_type", "state"],
            run_parallel(executor, generate_patients_chunk, NPA, plans, states, base),
        )

        batched_insert(
            con,
            "providers",
            ["provider_id", "name", "specialty", "state", "is_in_network", "npi"],
            run_parallel(executor, generate_providers_chunk, NPR, specs, states),
        )

        batched_insert(
            con,
            "claims",
            [
                "claim_id",
                "patient_id",
                "provider_id",
                "service_date",
                "claim_type",
                "total_billed",
                "total_allowed",
                "total_paid",
                "status",
                "denial_reason",
            ],
            run_parallel(
                executor,
                generate_claims_chunk,
                NCL,
                NPA,
                NPR,
                base,
                ctypes,
                cstats,
                dreasons,
            ),
        )

        batched_insert(
            con,
            "claim_lines",
            [
                "line_id",
                "claim_id",
                "cpt_code",
                "quantity",
                "unit_cost",
                "allowed_amount",
                "paid_amount",
            ],
            run_parallel(executor, generate_claim_lines_chunk, NCLL, NCL, cpts),
        )

        batched_insert(
            con,
            "diagnoses",
            ["diag_id", "claim_id", "icd_code", "is_primary", "chronic_flag"],
            run_parallel(executor, generate_diagnoses_chunk, NDX, NCL, icds),
        )

    con.close()
    print(f"p07 done patients={NPA} claims={NCL}")


if __name__ == "__main__":
    main()
