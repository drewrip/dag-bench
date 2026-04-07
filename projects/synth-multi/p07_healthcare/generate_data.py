import csv, duckdb, random, sys, os, tempfile
from datetime import date, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NPA, NPR, NCL, NCLL, NDX = (
    max(a, int(b * sf))
    for a, b in [(20, 1000), (10, 200), (30, 3000), (50, 9000), (10, 100)]
)
os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")

def batched_insert(sql, rows):
    rows = list(rows)
    if not rows:
        return
    table_name = sql.split()[2]
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False) as tmp:
        csv.writer(tmp).writerows(rows)
        temp_path = tmp.name
    try:
        con.execute(f"COPY {table_name} FROM '{temp_path}' (FORMAT CSV)")
    finally:
        os.unlink(temp_path)

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
con.execute("BEGIN")
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
batched_insert(
    "INSERT INTO patients VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            base - timedelta(days=random.randint(365 * 5, 365 * 85)),
            random.choice(["M", "F", "U"]),
            f"{random.randint(10000, 99999)}",
            random.choice(plans),
            random.choice(states),
        )
        for i in range(1, NPA + 1)
    ],
)
batched_insert(
    "INSERT INTO providers VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            f"Provider {i}",
            random.choice(specs),
            random.choice(states),
            random.random() > 0.2,
            f"NPI{i:010d}",
        )
        for i in range(1, NPR + 1)
    ],
)
rows = []
for i in range(1, NCL + 1):
    bill = round(random.uniform(100, 50000), 2)
    allow = round(bill * random.uniform(0.4, 0.95), 2)
    paid = round(allow * random.uniform(0.5, 1) if random.random() > 0.1 else 0, 2)
    rows.append(
        (
            i,
            random.randint(1, NPA),
            random.randint(1, NPR),
            base + timedelta(days=random.randint(0, 1095)),
            random.choice(ctypes),
            bill,
            allow,
            paid,
            random.choice(cstats),
            random.choice(dreasons),
        )
    )
batched_insert("INSERT INTO claims VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
batched_insert(
    "INSERT INTO claim_lines VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NCL),
            random.choice(cpts),
            random.randint(1, 5),
            round(random.uniform(10, 5000), 2),
            round(random.uniform(5, 4000), 2),
            round(random.uniform(0, 3500), 2),
        )
        for i in range(1, NCLL + 1)
    ],
)
batched_insert(
    "INSERT INTO diagnoses VALUES(?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NCL),
            random.choice(icds),
            random.random() > 0.3,
            random.random() > 0.6,
        )
        for i in range(1, NDX + 1)
    ],
)
con.commit()
con.close()
print(f"p07 done patients={NPA} claims={NCL}")
