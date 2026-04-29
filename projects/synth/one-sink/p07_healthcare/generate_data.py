import pyarrow as pa
import duckdb, random, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor

def generate_patients_chunk(start, end, genders, plans, states, base):
    return [
        (
            i,
            base - timedelta(days=random.randint(365 * 5, 365 * 85)),
            random.choice(genders),
            f"{random.randint(10000, 99999)}",
            random.choice(plans),
            random.choice(states),
        )
        for i in range(start, end)
    ]

def generate_providers_chunk(start, end, specialties, states):
    return [
        (
            i,
            f"Provider {i}",
            random.choice(specialties),
            random.choice(states),
            random.random() > 0.2,
            f"NPI{i:010d}",
        )
        for i in range(start, end)
    ]

def generate_claims_chunk(start, end, NPA, NPR, base, ctypes, cstatuses, denial_reasons):
    rows = []
    for i in range(start, end):
        bill = round(random.uniform(100, 50000), 2)
        allow = round(bill * random.uniform(0.4, 0.95), 2)
        paid = round(allow * random.uniform(0.5, 1.0) if random.random() > 0.1 else 0, 2)
        rows.append((
            i,
            random.randint(1, NPA),
            random.randint(1, NPR),
            base + timedelta(days=random.randint(0, 1095)),
            random.choice(ctypes),
            bill,
            allow,
            paid,
            random.choice(cstatuses),
            random.choice(denial_reasons),
        ))
    return rows

def generate_claim_lines_chunk(start, end, NCL, cpt_codes):
    return [
        (
            i,
            random.randint(1, NCL),
            random.choice(cpt_codes),
            random.randint(1, 5),
            round(random.uniform(10, 5000), 2),
            round(random.uniform(5, 4000), 2),
            round(random.uniform(0, 3500), 2),
        )
        for i in range(start, end)
    ]

def generate_diagnoses_chunk(start, end, NCL, icd_codes):
    return [
        (
            i,
            random.randint(1, NCL),
            random.choice(icd_codes),
            random.random() > 0.3,
            random.random() > 0.6,
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
    sf *= 1000
    NPA = max(20, int(1000 * sf))
    NPR = max(10, int(200 * sf))
    NCL = max(30, int(3000 * sf))
    NCLL = max(50, int(9000 * sf))
    NDX = max(10, int(100 * sf))

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

        batched_insert(con, "patients", ['patient_id', 'dob', 'gender', 'zip_code', 'plan_type', 'state'],
                       run_parallel(generate_patients_chunk, NPA, genders, plans, states, base))
        batched_insert(con, "providers", ['provider_id', 'name', 'specialty', 'state', 'is_in_network', 'npi'],
                       run_parallel(generate_providers_chunk, NPR, specialties, states))
        batched_insert(con, "claims", ['claim_id', 'patient_id', 'provider_id', 'service_date', 'claim_type', 'total_billed', 'total_allowed', 'total_paid', 'status', 'denial_reason'],
                       run_parallel(generate_claims_chunk, NCL, NPA, NPR, base, ctypes, cstatuses, denial_reasons))
        batched_insert(con, "claim_lines", ['line_id', 'claim_id', 'cpt_code', 'quantity', 'unit_cost', 'allowed_amount', 'paid_amount'],
                       run_parallel(generate_claim_lines_chunk, NCLL, NCL, cpt_codes))
        batched_insert(con, "diagnoses", ['diag_id', 'claim_id', 'icd_code', 'is_primary', 'chronic_flag'],
                       run_parallel(generate_diagnoses_chunk, NDX, NCL, icd_codes))

    con.close()
    print(f"p07 done: patients={NPA} claims={NCL} lines={NCLL}")

if __name__ == "__main__":
    main()
