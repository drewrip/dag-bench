use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Normal};

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let nsb = (4562.0 * sf).max(5.0) as usize;
    // Meters/readings/outages are fan-out ratios off substations/meters.
    let avg_meters_per_substation = 20.0;
    let avg_readings_per_meter = 500.0;
    let avg_outages_per_substation = 3.5;
    let nmt = ((nsb as f64) * avg_meters_per_substation).max(20.0) as usize;
    let ncr = ((nmt as f64) * avg_readings_per_meter).max(200.0) as usize;
    let noe = ((nsb as f64) * avg_outages_per_substation).max(5.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

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

    let base_ts = NaiveDate::from_ymd_opt(2023, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    // Fixed vocabularies.
    let regions = ["north_america", "europe", "apac", "latin_america"];
    let region_weights = [35.0, 30.0, 25.0, 10.0];
    let meter_types = ["residential", "commercial", "industrial"];
    let meter_type_weights = [75.0, 20.0, 5.0];
    let residential_tariffs = ["residential_standard", "residential_tou"];
    let residential_tariff_weights = [75.0, 25.0];
    let commercial_tariffs = ["commercial_standard", "commercial_demand"];
    let commercial_tariff_weights = [70.0, 30.0];
    let causes = [
        "weather", "equipment_failure", "vegetation", "vehicle_accident", "animal_contact",
        "planned_maintenance", "cyberattack",
    ];
    let cause_weights = [35.0, 25.0, 15.0, 10.0, 8.0, 6.0, 1.0];
    let severities = ["minor", "moderate", "major", "critical"];
    let voltages = [11, 33, 66, 110, 132, 220];

    let pb = ProgressBar::new(4);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Substations
    crate::generate_table_parallel(
        pool,
        "substations",
        nsb,
        &pb,
        "Generating substations...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let name = format!("SUB-{:03}", i);
            let region = weighted_choice(&mut rng, &regions, &region_weights);
            let cap = round_to(rng.gen_range(10.0..500.0), 2);
            let volt = voltages[rng.gen_range(0..voltages.len())];
            let lat = ((rng.gen_range(25.0..50.0) * 10000.0) as f64).round() / 10000.0;
            let lon = ((rng.gen_range(-120.0..-70.0) * 10000.0) as f64).round() / 10000.0;
            (i as i32, name, region, cap, volt, lat, lon)
        },
    )?;

    // Materialize substation capacity so meter count/rating can correlate with it
    // instead of an unrelated flat distribution.
    let mut stmt = con.prepare("SELECT capacity_mw FROM substations ORDER BY sub_id")?;
    let substation_capacity: Vec<f64> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let substation_popularity = PopularityWeights::from_factors(&substation_capacity);

    // 2. Meters
    crate::generate_table_parallel(pool, "meters", nmt, &pb, "Generating meters...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let sub_id = substation_popularity.sample(&mut rng) as i32;
        let cust_id = rng.gen_range(1..=((nmt as f64 * 1.05) as i32).max(1));
        let mtype = weighted_choice(&mut rng, &meter_types, &meter_type_weights);
        let tariff = match mtype {
            "residential" => weighted_choice(&mut rng, &residential_tariffs, &residential_tariff_weights),
            "commercial" => weighted_choice(&mut rng, &commercial_tariffs, &commercial_tariff_weights),
            _ => "industrial_demand",
        };
        let install = (base_ts - Duration::days(rng.gen_range(0..3651))).date();
        let smart = rng.gen_bool(0.7);
        let cap = round_to(
            match mtype {
                "industrial" => rng.gen_range(200.0..1000.0),
                "commercial" => rng.gen_range(20.0..200.0),
                _ => rng.gen_range(1.0..20.0),
            },
            2,
        );
        (
            i as i32, sub_id, cust_id, mtype, tariff, install, smart, cap,
        )
    })?;

    // Materialize meter_type/tariff so kwh can be drawn per-tariff instead of one global
    // distribution [CHANGE from dbgen], and so outages can size
    // affected_meters off each substation's real downstream meter count.
    let mut stmt = con.prepare("SELECT sub_id, meter_type FROM meters ORDER BY meter_id")?;
    let meter_facts: Vec<(i32, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut meters_per_sub = vec![0i64; nsb];
    for (sub_id, _) in &meter_facts {
        meters_per_sub[(*sub_id as usize) - 1] += 1;
    }
    let meter_popularity = PopularityWeights::new(nmt, 0.9, 191);

    // 3. Consumption Readings (kwh mean scales with tariff class)
    crate::generate_table_parallel(
        pool,
        "consumption_readings",
        ncr,
        &pb,
        "Generating consumption readings...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let meter_id = meter_popularity.sample(&mut rng);
            let ts = base_ts + Duration::seconds(rng.gen_range(0..364 * 86400));
            let (kwh_mean, kwh_sd) = match meter_facts[meter_id - 1].1.as_str() {
                "industrial" => (60.0, 25.0),
                "commercial" => (18.0, 8.0),
                _ => (5.0, 3.0),
            };
            let kwh_dist = Normal::new(kwh_mean, kwh_sd).unwrap();
            let kwh_sample: f64 = kwh_dist.sample(&mut rng);
            let kwh = round_to(kwh_sample.abs(), 4);
            let volt_dist = Normal::new(230.0, 5.0).unwrap();
            let volt = round_to(volt_dist.sample(&mut rng), 2);
            let pf = round_to(rng.gen_range(0.7..1.0), 3);
            let estimated = rng.gen_bool(0.02);
            (i as i64, meter_id as i32, ts, kwh, volt, pf, estimated)
        },
    )?;

    // 4. Outage Events (affected_meters as a fraction of the substation's real downstream
    // meter count, severity correlated with that fraction)
    crate::generate_table_parallel(
        pool,
        "outage_events",
        noe,
        &pb,
        "Generating outage events...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let sub_id = rng.gen_range(1..=nsb) as i32;
            let start = base_ts + Duration::seconds(rng.gen_range(0..364 * 86400));
            let end = start + Duration::seconds(rng.gen_range(5 * 60..1441 * 60));
            let cause = weighted_choice(&mut rng, &causes, &cause_weights);
            let total_meters = meters_per_sub[(sub_id as usize) - 1].max(1);
            let fraction = rng.gen_range(0.02..0.9);
            let affected = ((total_meters as f64) * fraction).round().max(1.0) as i32;
            let severity = if fraction > 0.6 {
                weighted_choice(&mut rng, &severities, &[5.0, 15.0, 35.0, 45.0])
            } else if fraction > 0.3 {
                weighted_choice(&mut rng, &severities, &[15.0, 40.0, 30.0, 15.0])
            } else {
                weighted_choice(&mut rng, &severities, &[60.0, 30.0, 8.0, 2.0])
            };
            (i as i32, sub_id, start, end, cause, affected, severity)
        },
    )?;

    pb.finish_with_message("p10_energy complete");

    Ok(())
}
