use crate::common::{weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Normal};

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let ns = (6642.0 * sf).max(3.0) as usize;
    // SPEC.md §2.1: devices ~= 5 per site on average, so device count scales with site count
    // rather than an unrelated flat constant.
    let nd = ((ns as f64) * 5.0).max(10.0) as usize;
    // SPEC.md §2.1: readings/maintenance_logs are fan-out off of devices, not independent bases.
    let nr = ((nd as f64) * 1300.0).max(100.0) as usize;
    let nml = ((nd as f64) * 3.3).max(5.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(
        "DROP TABLE IF EXISTS maintenance_logs; DROP TABLE IF EXISTS readings;
         DROP TABLE IF EXISTS devices; DROP TABLE IF EXISTS sites;
         CREATE TABLE sites(site_id INTEGER PRIMARY KEY, name VARCHAR,
             region VARCHAR, latitude DOUBLE, longitude DOUBLE, timezone VARCHAR);
         CREATE TABLE devices(device_id INTEGER PRIMARY KEY, site_id INTEGER,
             device_type VARCHAR, model VARCHAR, firmware VARCHAR,
             installed_date DATE, is_active BOOLEAN);
         CREATE TABLE readings(reading_id BIGINT PRIMARY KEY, device_id INTEGER,
             ts TIMESTAMP, temperature_c DOUBLE, humidity_pct DOUBLE,
             pressure_hpa DOUBLE, battery_pct TINYINT, rssi_dbm SMALLINT,
             error_flag BOOLEAN);
         CREATE TABLE maintenance_logs(log_id INTEGER PRIMARY KEY, device_id INTEGER,
             log_ts TIMESTAMP, action VARCHAR, technician VARCHAR, notes VARCHAR);",
    )?;

    let base_ts = NaiveDate::from_ymd_opt(2023, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    // SPEC.md §3.1 fixed vocabularies.
    let regions = ["north_america", "europe", "apac", "latin_america", "middle_east_africa"];
    let region_weights = [35.0, 25.0, 25.0, 10.0, 5.0];
    let tz_by_region: [&[&str]; 5] = [
        &["America/New_York", "America/Chicago", "America/Los_Angeles"],
        &["Europe/London", "Europe/Berlin", "Europe/Paris"],
        &["Asia/Tokyo", "Asia/Singapore", "Asia/Kolkata"],
        &["America/Sao_Paulo"],
        &["Africa/Johannesburg", "Asia/Dubai"],
    ];

    let device_types = [
        "temperature_sensor",
        "humidity_sensor",
        "pressure_sensor",
        "multi_sensor",
        "gateway",
    ];
    let device_type_weights = [30.0, 20.0, 15.0, 25.0, 10.0];
    let models_by_type: [&[&str]; 5] = [
        &["TS-100", "TS-200"],
        &["HM-100", "HM-220"],
        &["PR-050", "PR-090"],
        &["MX-300", "MX-500", "MX-700"],
        &["GW-500", "GW-900"],
    ];
    let firmware_major_weights = [5.0, 15.0, 40.0, 40.0]; // majors 1..4

    let actions = [
        "routine_inspection",
        "battery_replacement",
        "firmware_update",
        "repair",
        "recalibration",
        "decommission",
    ];
    let action_weights = [35.0, 25.0, 20.0, 12.0, 5.0, 3.0];

    // SPEC.md §1.3a: devices skew toward a few larger/busier sites; readings/maintenance
    // skew toward a few always-on devices. Weights are precomputed once and shared
    // read-only across the parallel per-row closures.
    let site_popularity = PopularityWeights::new(ns, 0.9, 11);
    let device_popularity = PopularityWeights::new(nd, 0.9, 22);

    let pb = ProgressBar::new(4);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Sites
    crate::generate_table_parallel(con, "sites", ns, &pb, "Generating sites...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Site-{}", i);
        let region_idx = {
            // weighted_choice over indices so we can look up the matching tz pool
            let target = weighted_choice(&mut rng, &(0..regions.len()).collect::<Vec<_>>(), &region_weights);
            target
        };
        let region = regions[region_idx];
        let lat = ((rng.gen_range(-60.0..60.0) * 10000.0) as f64).round() / 10000.0;
        let lon = ((rng.gen_range(-180.0..180.0) * 10000.0) as f64).round() / 10000.0;
        let tz_pool = tz_by_region[region_idx];
        let tz = tz_pool[rng.gen_range(0..tz_pool.len())];
        (i as i32, name, region, lat, lon, tz)
    })?;

    // 2. Devices
    crate::generate_table_parallel(con, "devices", nd, &pb, "Generating devices...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let site_id = site_popularity.sample(&mut rng) as i32;
        let dtype_idx = weighted_choice(&mut rng, &(0..device_types.len()).collect::<Vec<_>>(), &device_type_weights);
        let dtype = device_types[dtype_idx];
        let model_pool = models_by_type[dtype_idx];
        let model = model_pool[rng.gen_range(0..model_pool.len())];
        let major = weighted_choice(&mut rng, &[1, 2, 3, 4], &firmware_major_weights);
        let firmware = format!("v{}.{}.{}", major, rng.gen_range(0..10), rng.gen_range(0..100));
        // SPEC.md §2.1: installed_date predates base_date by 0-3 years (founding offset).
        let installed_date = (base_ts - Duration::days(rng.gen_range(0..1096))).date();
        let is_active = rng.gen_bool(0.95);
        (
            i as i32,
            site_id,
            dtype,
            model,
            firmware,
            installed_date,
            is_active,
        )
    })?;

    // 3. Readings
    crate::generate_table_parallel(con, "readings", nr, &pb, "Generating readings...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        // Every device gets a guaranteed reading among the first `nd` rows (stratified
        // minimum, SPEC.md §2.1); the rest are popularity-weighted across devices.
        let device_id = if i <= nd {
            i as i32
        } else {
            device_popularity.sample(&mut rng) as i32
        };
        let seconds_since_base = rng.gen_range(0..180 * 86400);
        let ts = base_ts + Duration::seconds(seconds_since_base);
        let temp_dist = Normal::new(20.0, 8.0).unwrap();
        let temp: f64 = temp_dist.sample(&mut rng);
        let temp = temp.clamp(-20.0, 60.0);
        let temp = ((temp * 100.0) as f64).round() / 100.0;
        let humid = ((rng.gen_range(20.0..95.0) * 100.0) as f64).round() / 100.0;
        let press_dist = Normal::new(1013.0, 15.0).unwrap();
        let press = ((press_dist.sample(&mut rng) * 100.0) as f64).round() / 100.0;
        // Battery decays across a ~30-day maintenance cycle and resets, rather than being
        // drawn independently (SPEC.md §2.1), so battery level correlates with maintenance.
        let cycle_days = 30;
        let day_in_cycle = (seconds_since_base / 86400) % cycle_days;
        let battery = (100.0 - (day_in_cycle as f64 / cycle_days as f64) * 90.0)
            .clamp(5.0, 100.0) as i8;
        let rssi = rng.gen_range(-90..-29) as i16;
        // Error rate correlates with low battery (SPEC.md §2.1): base 1.5%, ~3x when low.
        let error_rate = if battery < 15 { 0.045 } else { 0.015 };
        let error = rng.gen_bool(error_rate);
        (
            i as i64, device_id, ts, temp, humid, press, battery, rssi, error,
        )
    })?;

    // 4. Maintenance Logs
    crate::generate_table_parallel(
        con,
        "maintenance_logs",
        nml,
        &pb,
        "Generating maintenance logs...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let device_id = device_popularity.sample(&mut rng) as i32;
            let ts = base_ts + Duration::hours(rng.gen_range(0..4321));
            let action_idx = weighted_choice(&mut rng, &(0..actions.len()).collect::<Vec<_>>(), &action_weights);
            let action = actions[action_idx];
            let tech = format!("Tech-{}", rng.gen_range(1..21));
            let note = format!("Performed {} on device", action);
            (i as i32, device_id, ts, action, tech, note)
        },
    )?;

    pb.finish_with_message("p01_iot complete");

    Ok(())
}
