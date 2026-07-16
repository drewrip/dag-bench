use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let npa = (3099146.0 * sf).max(20.0) as usize;
    let npr = (619829.0 * sf).max(10.0) as usize;
    // SPEC.md §2.8: claims/claim_lines/diagnoses are fan-out ratios off patients/claims.
    let ncl = ((npa as f64) * 3.0).max(30.0) as usize;
    let ncll = ((ncl as f64) * 3.0).max(50.0) as usize;
    let ndx = ((ncl as f64) * 1.75).max(10.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

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

    // SPEC.md §3.8 fixed vocabularies.
    let genders = ["female", "male", "other_unknown"];
    let gender_weights = [51.0, 48.0, 1.0];
    let plans = ["PPO", "HMO", "Medicare", "EPO", "Medicaid", "POS"];
    let plan_weights = [35.0, 30.0, 15.0, 10.0, 7.0, 3.0];
    let states = [
        "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI", "NJ", "VA", "WA", "AZ", "MA",
    ];
    let state_weights = [12.0, 9.0, 7.0, 6.0, 4.0, 4.0, 4.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0];
    let specialties = [
        "primary_care", "emergency_medicine", "cardiology", "orthopedics", "psychiatry",
        "general_surgery", "dermatology", "radiology", "pediatrics", "oncology",
    ];
    let specialty_weights = [25.0, 12.0, 10.0, 9.0, 8.0, 8.0, 7.0, 7.0, 7.0, 7.0];
    let claim_types = ["professional", "institutional", "pharmacy", "dental", "vision"];
    let claim_type_weights = [45.0, 25.0, 18.0, 7.0, 5.0];
    let claim_statuses = ["paid", "pending", "denied", "partially_paid"];
    let claim_status_weights = [78.0, 10.0, 7.0, 5.0];
    let denial_reasons = [
        "not_medically_necessary", "out_of_network", "missing_authorization",
        "incorrect_coding", "coverage_terminated", "duplicate_claim",
    ];
    let denial_reason_weights = [30.0, 22.0, 20.0, 15.0, 8.0, 5.0];

    let cpt_codes: Vec<String> = (1..=50).map(|i| format!("CPT{:05}", i)).collect();
    // ~10 "common visit" codes account for ~50% of lines (SPEC.md §3.8).
    let mut cpt_weights = vec![1.25f64; 50];
    for w in cpt_weights.iter_mut().take(10) {
        *w = 5.0;
    }
    let cpt_base_cost: Vec<f64> = (0..50)
        .map(|i| {
            let mut r = SmallRng::seed_from_u64((i as u64) + 7_000);
            round_to(r.gen_range(10.0..2000.0), 2)
        })
        .collect();

    let icd_codes: Vec<String> = (1..=100).map(|i| format!("ICD{:04}", i)).collect();
    // ~15 common chronic/acute diagnoses account for ~40% of rows (SPEC.md §3.8).
    let mut icd_weights = vec![60.0 / 85.0; 100];
    for w in icd_weights.iter_mut().take(15) {
        *w = 40.0 / 15.0;
    }

    // SPEC.md §2.8: claim volume is driven by a per-patient utilization tier (70% low, 25%
    // medium, 5% high) rather than a raw Pareto exponent - more interpretable for a
    // healthcare context and gives downstream "high-utilizer" models a clean signal.
    let tier_weights = [70.0, 25.0, 5.0];
    let tier_multipliers = [1.0, 5.0, 20.0];
    let patient_claim_factors: Vec<f64> = (0..npa)
        .map(|i| {
            let mut rng = SmallRng::seed_from_u64((i as u64) + 100_000);
            tier_multipliers[weighted_choice(&mut rng, &[0usize, 1, 2], &tier_weights)]
        })
        .collect();
    let patient_popularity = PopularityWeights::from_factors(&patient_claim_factors);
    let provider_popularity = PopularityWeights::new(npr, 0.9, 161);
    // claim_lines/diagnoses fan out off claims with a mild skew (some encounters are far more
    // complex than others, SPEC.md §2.8).
    let claim_line_popularity = PopularityWeights::new(ncl, 0.8, 171);
    let claim_diag_popularity = PopularityWeights::new(ncl, 0.8, 172);

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
        let gender = weighted_choice(&mut rng, &genders, &gender_weights);
        let zip = format!("{:05}", rng.gen_range(10000..100000));
        let plan = weighted_choice(&mut rng, &plans, &plan_weights);
        let state = weighted_choice(&mut rng, &states, &state_weights);
        (i as i32, dob, gender, zip, plan, state)
    })?;

    // 2. Providers
    crate::generate_table_parallel(con, "providers", npr, &pb, "Generating providers...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Provider {}", i);
        let spec = weighted_choice(&mut rng, &specialties, &specialty_weights);
        let state = weighted_choice(&mut rng, &states, &state_weights);
        let network = rng.gen_bool(0.8);
        let npi = format!("NPI{:010}", i);
        (i as i32, name, spec, state, network, npi)
    })?;

    // 3. Claims (total_billed/total_allowed/total_paid start as placeholders and are rolled
    // up from real claim_lines below, SPEC.md §1.7/§2.8)
    crate::generate_table_parallel(con, "claims", ncl, &pb, "Generating claims...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let pat_id = patient_popularity.sample(&mut rng) as i32;
        let prov_id = provider_popularity.sample(&mut rng) as i32;
        let service = base_date + Duration::days(rng.gen_range(0..1096));
        let ctype = weighted_choice(&mut rng, &claim_types, &claim_type_weights);
        let status = weighted_choice(&mut rng, &claim_statuses, &claim_status_weights);
        let denial = if status == "denied" {
            Some(weighted_choice(&mut rng, &denial_reasons, &denial_reason_weights))
        } else {
            None
        };
        (
            i as i32, pat_id, prov_id, service, ctype, 0.0_f64, 0.0_f64, 0.0_f64, status, denial,
        )
    })?;

    // Materialize claim status so line-level paid_amount can respect denied claims
    // (SPEC.md §1.7) instead of being independent of it.
    let mut stmt = con.prepare("SELECT status FROM claims ORDER BY claim_id")?;
    let claim_status: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 4. Claim Lines (every claim gets >=1 line via a stratified minimum, SPEC.md §2.8)
    crate::generate_table_parallel(
        con,
        "claim_lines",
        ncll,
        &pb,
        "Generating claim lines...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let claim_id = if i <= ncl {
                i
            } else {
                claim_line_popularity.sample(&mut rng)
            };
            let cpt_idx = weighted_choice(&mut rng, &(0..cpt_codes.len()).collect::<Vec<_>>(), &cpt_weights);
            let cpt = &cpt_codes[cpt_idx];
            let qty = rng.gen_range(1..6);
            let unit_cost = round_to(cpt_base_cost[cpt_idx] * rng.gen_range(0.9..1.1), 2);
            // Allowed/paid are fractions of the line's own cost, not independent draws
            // (SPEC.md §1.7): paid <= allowed <= billed is enforced by construction.
            let allowed = round_to((unit_cost * qty as f64) * rng.gen_range(0.5..0.9), 2);
            let denied = claim_status[claim_id - 1] == "denied";
            let paid = if denied {
                0.0
            } else {
                round_to(allowed * rng.gen_range(0.6..1.0), 2)
            };
            (i as i32, claim_id as i32, cpt.clone(), qty, unit_cost, allowed, paid)
        },
    )?;

    // Roll claims totals up from their real claim_lines (SPEC.md §1.7/§2.8): the one place
    // in this schema where a header total is a literal rollup of its details, since
    // insurance claims really do reconcile this way operationally.
    con.execute_batch(
        "UPDATE claims SET
             total_billed = t.billed,
             total_allowed = t.allowed,
             total_paid = t.paid
         FROM (
             SELECT claim_id,
                    SUM(quantity * unit_cost) AS billed,
                    SUM(allowed_amount) AS allowed,
                    SUM(paid_amount) AS paid
             FROM claim_lines GROUP BY claim_id
         ) t
         WHERE claims.claim_id = t.claim_id;",
    )?;

    // 5. Diagnoses [FIX per SPEC.md §2.8]: references claims.claim_id unambiguously (every
    // claim gets >=1 diagnosis via a stratified minimum).
    crate::generate_table_parallel(con, "diagnoses", ndx, &pb, "Generating diagnoses...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let claim_id = if i <= ncl {
            i
        } else {
            claim_diag_popularity.sample(&mut rng)
        };
        let icd_idx = weighted_choice(&mut rng, &(0..icd_codes.len()).collect::<Vec<_>>(), &icd_weights);
        let icd = &icd_codes[icd_idx];
        let primary = rng.gen_bool(0.7);
        let chronic = rng.gen_bool(0.4);
        (i as i32, claim_id as i32, icd.clone(), primary, chronic)
    })?;

    pb.finish_with_message("p08_healthcare complete");

    Ok(())
}
