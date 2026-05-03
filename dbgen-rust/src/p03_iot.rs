use chrono::{Duration, NaiveDate};
use duckdb::{Connection, DuckdbConnectionManager};
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Normal};

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let sf_adj = sf * 220.0;
    let ns = (30.0 * sf_adj).max(3.0) as usize;
    let nd = (150.0 * sf_adj).max(10.0) as usize;
    let nr = (200000.0 * sf_adj).max(100.0) as usize;
    let nml = (500.0 * sf_adj).max(5.0) as usize;

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
    let regions = ["NA", "EU", "APAC", "LATAM"];
    let dtypes = [
        "temperature",
        "humidity",
        "pressure",
        "multi",
        "air_quality",
    ];
    let actions = [
        "calibrate",
        "replace_battery",
        "firmware_update",
        "repair",
        "inspect",
    ];
    let tz_list = ["UTC", "US/Eastern", "Europe/Berlin", "Asia/Tokyo"];

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
        let region = regions[rng.gen_range(0..regions.len())];
        let lat = ((rng.gen_range(-60.0..60.0) * 10000.0) as f64).round() / 10000.0;
        let lon = ((rng.gen_range(-180.0..180.0) * 10000.0) as f64).round() / 10000.0;
        let tz = tz_list[rng.gen_range(0..tz_list.len())];
        (i as i32, name, region, lat, lon, tz)
    })?;

    // 2. Devices
    crate::generate_table_parallel(con, "devices", nd, &pb, "Generating devices...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let site_id = rng.gen_range(1..=ns) as i32;
        let dtype = dtypes[rng.gen_range(0..dtypes.len())];
        let model = format!(
            "Model-{}{}",
            ['A', 'B', 'C'][rng.gen_range(0..3)],
            rng.gen_range(1..6)
        );
        let firmware = format!(
            "v{}.{}.{}",
            rng.gen_range(1..5),
            rng.gen_range(0..10),
            rng.gen_range(0..100)
        );
        let installed_date = (base_ts - Duration::days(rng.gen_range(0..731))).date();
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
        let device_id = rng.gen_range(1..=nd) as i32;
        let ts = base_ts + Duration::seconds(rng.gen_range(0..180 * 86400));
        let temp_dist = Normal::new(20.0, 8.0).unwrap();
        let temp = ((temp_dist.sample(&mut rng) * 100.0) as f64).round() / 100.0;
        let humid = ((rng.gen_range(20.0..95.0) * 100.0) as f64).round() / 100.0;
        let press_dist = Normal::new(1013.0, 15.0).unwrap();
        let press = ((press_dist.sample(&mut rng) * 100.0) as f64).round() / 100.0;
        let battery = rng.gen_range(5..=101) as i8;
        let rssi = rng.gen_range(-90..-29) as i16;
        let error = rng.gen_bool(0.02);
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
            let device_id = rng.gen_range(1..=nd) as i32;
            let ts = base_ts + Duration::hours(rng.gen_range(0..4321));
            let action = actions[rng.gen_range(0..actions.len())];
            let tech = format!("Tech-{}", rng.gen_range(1..21));
            let note = format!(
                "Performed {} on device",
                actions[rng.gen_range(0..actions.len())]
            );
            (i as i32, device_id, ts, action, tech, note)
        },
    )?;

    pb.finish_with_message("p03_iot complete");

    Ok(())
}
