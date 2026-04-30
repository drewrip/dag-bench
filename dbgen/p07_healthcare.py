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

    patient_ids = range(start, end)
    dobs = (np.datetime64(base) - days_back.astype("timedelta64[D]")).tolist()
    selected_genders = np.take(genders, gender_indices).tolist()
    selected_zips = zip_codes.astype(str).tolist()
    selected_plans = np.take(plans, plan_indices).tolist()
    selected_states = np.take(states, state_indices).tolist()
    
    return list(zip(patient_ids, dobs, selected_genders, selected_zips, selected_plans, selected_states))


def generate_providers_chunk(start, end, specialties, states):
    size = end - start
    rng = np.random.default_rng(start)
    spec_indices = rng.integers(0, len(specialties), size)
    state_indices = rng.integers(0, len(states), size)
    network_probs = rng.random(size)

    provider_ids = range(start, end)
    provider_names = [f"Provider {i}" for i in provider_ids]
    selected_specs = np.take(specialties, spec_indices).tolist()
    selected_states = np.take(states, state_indices).tolist()
    is_in_network = (network_probs > 0.2).tolist()
    npis = [f"NPI{i:010d}" for i in provider_ids]
    
    return list(zip(provider_ids, provider_names, selected_specs, selected_states, is_in_network, npis))


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

    claim_ids = range(start, end)
    selected_patients = patient_ids.tolist()
    selected_providers = provider_ids.tolist()
    service_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]")).tolist()
    selected_ctypes = np.take(ctypes, ctype_indices).tolist()
    
    bills = np.round(bill_amounts, 2)
    allows = np.round(bills * allow_mults, 2)
    paid_vals = np.where(paid_probs > 0.1, np.round(allows * paid_mults, 2), 0.0)
    
    selected_statuses = np.take(cstatuses, status_indices).tolist()
    selected_denials = np.take(denial_reasons, denial_indices).tolist()
    
    return list(zip(
        claim_ids,
        selected_patients,
        selected_providers,
        service_dates,
        selected_ctypes,
        bills.tolist(),
        allows.tolist(),
        paid_vals.tolist(),
        selected_statuses,
        selected_denials
    ))


def generate_claim_lines_chunk(start, end, NCL, cpt_codes):
    size = end - start
    rng = np.random.default_rng(start)
    claim_ids = rng.integers(1, NCL + 1, size)
    cpt_indices = rng.integers(0, len(cpt_codes), size)
    quantities = rng.integers(1, 6, size)
    unit_costs = rng.uniform(10, 5000, size)
    allowed_amounts = rng.uniform(5, 4000, size)
    paid_amounts = rng.uniform(0, 3500, size)

    line_ids = range(start, end)
    selected_claims = claim_ids.tolist()
    selected_cpt = np.take(cpt_codes, cpt_indices).tolist()
    selected_quantities = quantities.tolist()
    unit_costs_rounded = np.round(unit_costs, 2).tolist()
    allowed_rounded = np.round(allowed_amounts, 2).tolist()
    paid_rounded = np.round(paid_amounts, 2).tolist()
    
    return list(zip(line_ids, selected_claims, selected_cpt, selected_quantities, unit_costs_rounded, allowed_rounded, paid_rounded))


def generate_diagnoses_chunk(start, end, NCL, icd_codes):
    size = end - start
    rng = np.random.default_rng(start)
    claim_ids = rng.integers(1, NCL + 1, size)
    icd_indices = rng.integers(0, len(icd_codes), size)
    primary_probs = rng.random(size)
    chronic_probs = rng.random(size)

    diag_ids = range(start, end)
    selected_claims = claim_ids.tolist()
    selected_icd = np.take(icd_codes, icd_indices).tolist()
    is_primary = (primary_probs > 0.3).tolist()
    is_chronic = (chronic_probs > 0.6).tolist()
    
    return list(zip(diag_ids, selected_claims, selected_icd, is_primary, is_chronic))


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 4000.0
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
