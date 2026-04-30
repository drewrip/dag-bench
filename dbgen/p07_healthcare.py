import duckdb, numpy as np, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_patients_chunk(start, end, genders, plans, states, base):
    size = end - start
    rng = np.random.default_rng(start)
    days_back = rng.integers(365 * 5, 365 * 85 + 1, size)
    gender_indices = rng.integers(0, len(genders), size)
    zip_codes = rng.integers(10000, 100000, size)
    plan_indices = rng.integers(0, len(plans), size)
    state_indices = rng.integers(0, len(states), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            base - timedelta(days=int(days_back[idx])),
            genders[gender_indices[idx]],
            str(zip_codes[idx]),
            plans[plan_indices[idx]],
            states[state_indices[idx]],
        ))
    return rows

def generate_providers_chunk(start, end, specialties, states):
    size = end - start
    rng = np.random.default_rng(start)
    spec_indices = rng.integers(0, len(specialties), size)
    state_indices = rng.integers(0, len(states), size)
    network_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Provider {i}",
            specialties[spec_indices[idx]],
            states[state_indices[idx]],
            bool(network_probs[idx] > 0.2),
            f"NPI{i:010d}",
        ))
    return rows

def generate_claims_chunk(start, end, NPA, NPR, base, ctypes, cstatuses, denial_reasons):
    size = end - start
    rng = np.random.default_rng(start)
    patient_ids = rng.integers(1, NPA + 1, size)
    provider_ids = rng.integers(1, NPR + 1, size)
    days_offset = rng.integers(0, 1096, size)
    ctype_indices = rng.integers(0, len(ctypes), size)
    bill_amounts = rng.uniform(100, 50000, size)
    allow_mults = rng.uniform(0.4, 0.95, size)
    paid_mults = rng.uniform(0.5, 1.0, size)
    paid_probs = rng.random(size)
    status_indices = rng.integers(0, len(cstatuses), size)
    denial_indices = rng.integers(0, len(denial_reasons), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        bill = round(float(bill_amounts[idx]), 2)
        allow = round(bill * float(allow_mults[idx]), 2)
        paid = round(allow * float(paid_mults[idx]) if paid_probs[idx] > 0.1 else 0.0, 2)
        rows.append((
            i,
            int(patient_ids[idx]),
            int(provider_ids[idx]),
            base + timedelta(days=int(days_offset[idx])),
            ctypes[ctype_indices[idx]],
            bill,
            allow,
            paid,
            cstatuses[status_indices[idx]],
            denial_reasons[denial_indices[idx]],
        ))
    return rows

def generate_claim_lines_chunk(start, end, NCL, cpt_codes):
    size = end - start
    rng = np.random.default_rng(start)
    claim_ids = rng.integers(1, NCL + 1, size)
    cpt_indices = rng.integers(0, len(cpt_codes), size)
    quantities = rng.integers(1, 6, size)
    unit_costs = rng.uniform(10, 5000, size)
    allowed_amounts = rng.uniform(5, 4000, size)
    paid_amounts = rng.uniform(0, 3500, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(claim_ids[idx]),
            cpt_codes[cpt_indices[idx]],
            int(quantities[idx]),
            round(float(unit_costs[idx]), 2),
            round(float(allowed_amounts[idx]), 2),
            round(float(paid_amounts[idx]), 2),
        ))
    return rows

def generate_diagnoses_chunk(start, end, NCL, icd_codes):
    size = end - start
    rng = np.random.default_rng(start)
    claim_ids = rng.integers(1, NCL + 1, size)
    icd_indices = rng.integers(0, len(icd_codes), size)
    primary_probs = rng.random(size)
    chronic_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(claim_ids[idx]),
            icd_codes[icd_indices[idx]],
            bool(primary_probs[idx] > 0.3),
            bool(chronic_probs[idx] > 0.6),
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1000.0
    NPA = max(20, int(1000 * sf_adj))
    NPR = max(10, int(200 * sf_adj))
    NCL = max(30, int(3000 * sf_adj))
    NCLL = max(50, int(9000 * sf_adj))
    NDX = max(10, int(100 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS diagnoses; DROP TABLE IF EXISTS claim_lines;
    DROP TABLE IF EXISTS claims; DROP TABLE IF EXISTS providers;
    DROP TABLE IF EXISTS patients;
    CREATE TABLE patients(patient_id INTEGER PRIMARY KEY, dob DATE,
        gender VARCHAR, zip_code VARCHAR, plan_type VARCHAR, state VARCHAR);
    CREATE TABLE providers(provider_id INTEGER PRIMARY KEY, name VARCHAR,
        specialty VARCHAR, state VARCHAR, is_in_network BOOLEAN, npi VARCHAR);
    CREATE TABLE claims(claim_id INTEGER PRIMARY KEY, patient_id INTEGER,
        provider_id INTEGER, service_date DATE, claim_type VARCHAR,
        total_billed DECIMAL(12,2), total_allowed DECIMAL(12,2),
        total_paid DECIMAL(12,2), status VARCHAR, denial_reason VARCHAR);
    CREATE TABLE claim_lines(line_id INTEGER PRIMARY KEY, claim_id INTEGER,
        cpt_code VARCHAR, quantity INTEGER, unit_cost DECIMAL(10,2),
        allowed_amount DECIMAL(10,2), paid_amount DECIMAL(10,2));
    CREATE TABLE diagnoses(diag_id INTEGER PRIMARY KEY, claim_id INTEGER,
        icd_code VARCHAR, is_primary BOOLEAN, chronic_flag BOOLEAN);
    """)

    base = date(2020, 1, 1)
    genders = ["M", "F", "U"]
    plans = ["HMO", "PPO", "EPO", "HDHP", "Medicare", "Medicaid"]
    specialties = ["internal_medicine", "cardiology", "oncology", "orthopedics", "primary_care", "emergency", "radiology", "psychiatry"]
    ctypes = ["professional", "facility", "pharmacy", "dental"]
    cstatuses = ["paid", "denied", "pending", "partial"]
    denial_reasons = ["not_covered", "prior_auth", "out_of_network", None, None, None]
    cpt_codes = [f"CPT{i:05d}" for i in range(1, 51)]
    icd_codes = [f"ICD{i:04d}" for i in range(1, 101)]
    states = ["CA", "TX", "NY", "FL", "IL", "WA", "OH", "GA"]

    cpu_count = get_worker_count()
    progress = GenerationProgress("p07_healthcare", 5)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("patients")
        batched_insert(con, "patients", ['patient_id', 'dob', 'gender', 'zip_code', 'plan_type', 'state'],
                       run_parallel(executor, generate_patients_chunk, NPA, genders, plans, states, base))
        progress.advance("providers")
        batched_insert(con, "providers", ['provider_id', 'name', 'specialty', 'state', 'is_in_network', 'npi'],
                       run_parallel(executor, generate_providers_chunk, NPR, specialties, states))
        progress.advance("claims")
        batched_insert(con, "claims", ['claim_id', 'patient_id', 'provider_id', 'service_date', 'claim_type', 'total_billed', 'total_allowed', 'total_paid', 'status', 'denial_reason'],
                       run_parallel(executor, generate_claims_chunk, NCL, NPA, NPR, base, ctypes, cstatuses, denial_reasons))
        progress.advance("claim_lines")
        batched_insert(con, "claim_lines", ['line_id', 'claim_id', 'cpt_code', 'quantity', 'unit_cost', 'allowed_amount', 'paid_amount'],
                       run_parallel(executor, generate_claim_lines_chunk, NCLL, NCL, cpt_codes))
        progress.advance("diagnoses")
        batched_insert(con, "diagnoses", ['diag_id', 'claim_id', 'icd_code', 'is_primary', 'chronic_flag'],
                       run_parallel(executor, generate_diagnoses_chunk, NDX, NCL, icd_codes))


    con.close()
    print_generation_summary(
        "p07_healthcare",
        sf,
        {
            "patients": NPA,
            "providers": NPR,
            "claims": NCL,
            "claim_lines": NCLL,
            "diagnoses": NDX,
        },
    )

if __name__ == "__main__":
    main()
