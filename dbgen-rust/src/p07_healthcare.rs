use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 4000.0;
    let npa = (1000.0 * sf_adj).max(20.0) as usize;
    let npr = (200.0 * sf_adj).max(10.0) as usize;
    let ncl = (3000.0 * sf_adj).max(30.0) as usize;
    let ncll = (9000.0 * sf_adj).max(50.0) as usize;
    let ndx = (100.0 * sf_adj).max(10.0) as usize;

    con.execute_batch(
        "DROP TABLE IF EXISTS diagnoses; DROP TABLE IF EXISTS claim_lines;
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
             icd_code VARCHAR, is_primary BOOLEAN, chronic_flag BOOLEAN);",
    )?;

    let base_date = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let genders = ["M", "F", "U"];
    let plans = ["HMO", "PPO", "EPO", "HDHP", "Medicare", "Medicaid"];
    let specialties = [
        "internal_medicine",
        "cardiology",
        "oncology",
        "orthopedics",
        "primary_care",
        "emergency",
        "radiology",
        "psychiatry",
    ];
    let ctypes = ["professional", "facility", "pharmacy", "dental"];
    let cstatuses = ["paid", "denied", "pending", "partial"];
    let denial_reasons = ["not_covered", "prior_auth", "out_of_network"];
    let states = ["CA", "TX", "NY", "FL", "IL", "WA", "OH", "GA"];
    let cpt_codes: Vec<String> = (1..=50).map(|i| format!("CPT{:05}", i)).collect();
    let icd_codes: Vec<String> = (1..=100).map(|i| format!("ICD{:04}", i)).collect();

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Patients
    crate::generate_table_parallel(con, "patients", npa, &pb, "Generating patients...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let dob = base_date - Duration::days(rng.gen_range(365 * 5..365 * 85));
        let gender = genders[rng.gen_range(0..genders.len())];
        let zip = format!("{:05}", rng.gen_range(10000..100000));
        let plan = plans[rng.gen_range(0..plans.len())];
        let state = states[rng.gen_range(0..states.len())];
        (i as i32, dob, gender, zip, plan, state)
    })?;

    // 2. Providers
    crate::generate_table_parallel(con, "providers", npr, &pb, "Generating providers...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Provider {}", i);
        let spec = specialties[rng.gen_range(0..specialties.len())];
        let state = states[rng.gen_range(0..states.len())];
        let network = rng.gen_bool(0.8);
        let npi = format!("NPI{:010}", i);
        (i as i32, name, spec, state, network, npi)
    })?;

    // 3. Claims
    crate::generate_table_parallel(con, "claims", ncl, &pb, "Generating claims...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let pat_id = rng.gen_range(1..=npa) as i32;
        let prov_id = rng.gen_range(1..=npr) as i32;
        let service = base_date + Duration::days(rng.gen_range(0..1096));
        let ctype = ctypes[rng.gen_range(0..ctypes.len())];
        let billed = ((rng.gen_range(100.0..50000.0) * 100.0) as f64).round() / 100.0;
        let allowed = ((billed * rng.gen_range(0.4..0.95) * 100.0) as f64).round() / 100.0;
        let paid = if rng.gen_bool(0.9) {
            ((allowed * rng.gen_range(0.5..1.0) * 100.0) as f64).round() / 100.0
        } else {
            0.0
        };
        let status = cstatuses[rng.gen_range(0..cstatuses.len())];
        let denial = if status == "denied" {
            Some(denial_reasons[rng.gen_range(0..denial_reasons.len())])
        } else {
            None
        };
        (
            i as i32, pat_id, prov_id, service, ctype, billed, allowed, paid, status, denial,
        )
    })?;

    // 4. Claim Lines
    crate::generate_table_parallel(
        con,
        "claim_lines",
        ncll,
        &pb,
        "Generating claim lines...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let cl_id = rng.gen_range(1..=ncl) as i32;
            let cpt = &cpt_codes[rng.gen_range(0..cpt_codes.len())];
            let qty = rng.gen_range(1..6);
            let cost = ((rng.gen_range(10.0..5000.0) * 100.0) as f64).round() / 100.0;
            let allowed = ((rng.gen_range(5.0..4000.0) * 100.0) as f64).round() / 100.0;
            let paid = ((rng.gen_range(0.0..3500.0) * 100.0) as f64).round() / 100.0;
            (i as i32, cl_id, cpt.clone(), qty, cost, allowed, paid)
        },
    )?;

    // 5. Diagnoses
    crate::generate_table_parallel(con, "diagnoses", ndx, &pb, "Generating diagnoses...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let cl_id = rng.gen_range(1..=ncl) as i32;
        let icd = &icd_codes[rng.gen_range(0..icd_codes.len())];
        let primary = rng.gen_bool(0.7);
        let chronic = rng.gen_bool(0.4);
        (i as i32, cl_id, icd.clone(), primary, chronic)
    })?;

    pb.finish_with_message("p07_healthcare complete");

    Ok(())
}
