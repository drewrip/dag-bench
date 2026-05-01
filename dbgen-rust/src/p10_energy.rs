use chrono::{Duration, NaiveDate};
use duckdb::{params, Connection};
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand_distr::{Distribution, Normal};
use rayon::prelude::*;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 90.0;
    let nsb = (50.0 * sf_adj).max(5.0) as usize;
    let nmt = (1000.0 * sf_adj).max(20.0) as usize;
    let ncr = (500000.0 * sf_adj).max(200.0) as usize;
    let noe = (200.0 * sf_adj).max(5.0) as usize;

    con.execute_batch(
        "DROP TABLE IF EXISTS outage_events; DROP TABLE IF EXISTS consumption_readings;
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
             affected_meters INTEGER, severity VARCHAR);",
    )?;

    let base_ts = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"];
    let mtypes = ["residential", "commercial", "industrial", "municipal"];
    let tariffs = ["standard", "time_of_use", "demand", "green", "low_income"];
    let causes = ["equipment_failure", "weather", "third_party", "maintenance", "unknown"];
    let severities = ["minor", "moderate", "major", "critical"];
    let voltages = [11, 33, 66, 110, 132, 220];

    let pb = ProgressBar::new(4);
    pb.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
        .unwrap());

    // 1. Substations
    pb.set_message("Generating substations...");
    let mut appender = con.appender("substations")?;
    for i in 1..=nsb {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let name = format!("SUB-{:03}", i);
        let region = regions[rng.gen_range(0..regions.len())];
        let cap = ((rng.gen_range(10.0..500.0) * 100.0) as f64).round() / 100.0;
        let volt = voltages[rng.gen_range(0..voltages.len())];
        let lat = ((rng.gen_range(25.0..50.0) * 10000.0) as f64).round() / 10000.0;
        let lon = ((rng.gen_range(-120.0..-70.0) * 10000.0) as f64).round() / 10000.0;
        appender.append_row(params![i as i32, name, region, cap, volt, lat, lon])?;
    }
    drop(appender);
    pb.inc(1);

    // 2. Meters
    pb.set_message("Generating meters...");
    let chunk_size = 100_000;
    for chunk_start in (1..=nmt).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nmt + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let sub_id = rng.gen_range(1..=nsb) as i32;
            let cust_id = rng.gen_range(1..=2001) as i32;
            let mtype = mtypes[rng.gen_range(0..mtypes.len())];
            let tariff = tariffs[rng.gen_range(0..tariffs.len())];
            let install = (base_ts - Duration::days(rng.gen_range(0..3651))).date();
            let smart = rng.gen_bool(0.7);
            let cap = ((rng.gen_range(1.0..1000.0) * 100.0) as f64).round() / 100.0;
            (i as i32, sub_id, cust_id, mtype, tariff, install, smart, cap)
        }).collect();

        let mut appender = con.appender("meters")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7])?;
        }
    }
    pb.inc(1);

    // 3. Consumption Readings
    pb.set_message("Generating consumption readings...");
    for chunk_start in (1..=ncr).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(ncr + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let meter_id = rng.gen_range(1..=nmt) as i32;
            let ts = base_ts + Duration::seconds(rng.gen_range(0..364 * 86400));
            let kwh_dist = Normal::new(5.0, 3.0).unwrap();
            let kwh = (((kwh_dist.sample(&mut rng) as f64).abs() * 10000.0) as f64).round() / 10000.0;
            let volt_dist = Normal::new(230.0, 5.0).unwrap();
            let volt = ((volt_dist.sample(&mut rng) * 100.0) as f64).round() / 100.0;
            let pf = ((rng.gen_range(0.7..1.0) * 1000.0) as f64).round() / 1000.0;
            let estimated = rng.gen_bool(0.02);
            (i as i64, meter_id, ts, kwh, volt, pf, estimated)
        }).collect();

        let mut appender = con.appender("consumption_readings")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6])?;
        }
    }
    pb.inc(1);

    // 4. Outage Events
    pb.set_message("Generating outage events...");
    for chunk_start in (1..=noe).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(noe + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let sub_id = rng.gen_range(1..=nsb) as i32;
            let start = base_ts + Duration::seconds(rng.gen_range(0..364 * 86400));
            let end = start + Duration::minutes(rng.gen_range(5..1441));
            let cause = causes[rng.gen_range(0..causes.len())];
            let affected = rng.gen_range(1..501);
            let sev = severities[rng.gen_range(0..severities.len())];
            (i as i32, sub_id, start, end, cause, affected, sev)
        }).collect();

        let mut appender = con.appender("outage_events")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6])?;
        }
    }
    pb.finish_with_message("p10_energy complete");

    Ok(())
}
