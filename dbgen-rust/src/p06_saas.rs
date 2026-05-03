use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use duckdb::arrow::array::{
    BooleanArray, Date32Array, Date64Array, Float64Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampMillisecondArray, TimestampSecondArray,
};
use duckdb::arrow::datatypes::{
    ArrowTimestampType, Date32Type, Date64Type, TimestampMillisecondType, TimestampSecondType,
};
use duckdb::{Connection, DuckdbConnectionManager};
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let sf_adj = sf * 800.0;
    let nac = (500.0 * sf_adj).max(10.0) as usize;
    let nsb = (700.0 * sf_adj).max(10.0) as usize;
    let nev = (50000.0 * sf_adj).max(100.0) as usize;
    let nfu = (5000.0 * sf_adj).max(20.0) as usize;
    let nst = (2000.0 * sf_adj).max(10.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(
        "DROP TABLE IF EXISTS support_tickets; DROP TABLE IF EXISTS feature_usage;
         DROP TABLE IF EXISTS events; DROP TABLE IF EXISTS subscriptions;
         DROP TABLE IF EXISTS accounts;
         CREATE TABLE accounts(account_id INTEGER PRIMARY KEY, name VARCHAR,
             industry VARCHAR, country VARCHAR, arr DECIMAL(12,2),
             created_date DATE, csm_id INTEGER, health_score TINYINT);
         CREATE TABLE subscriptions(sub_id INTEGER PRIMARY KEY, account_id INTEGER,
             plan VARCHAR, seats INTEGER, mrr DECIMAL(10,2),
             start_date DATE, end_date DATE, is_active BOOLEAN, renewal_date DATE);
         CREATE TABLE events(event_id BIGINT PRIMARY KEY, account_id INTEGER,
             user_id INTEGER, event_type VARCHAR, event_ts TIMESTAMP,
             session_id VARCHAR, platform VARCHAR);
         CREATE TABLE feature_usage(fu_id INTEGER PRIMARY KEY, account_id INTEGER,
             feature_name VARCHAR, usage_date DATE, usage_count INTEGER);
         CREATE TABLE support_tickets(ticket_id INTEGER PRIMARY KEY, account_id INTEGER,
             created_ts TIMESTAMP, resolved_ts TIMESTAMP, priority VARCHAR,
             category VARCHAR, csat_score TINYINT, is_resolved BOOLEAN);",
    )?;

    let base_date = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let base_ts = base_date.and_hms_opt(0, 0, 0).unwrap();
    let industries = [
        "fintech",
        "healthtech",
        "edtech",
        "ecommerce",
        "manufacturing",
        "media",
    ];
    let plans = ["starter", "growth", "enterprise", "enterprise_plus"];
    let etypes = [
        "login",
        "page_view",
        "feature_click",
        "export",
        "api_call",
        "report_view",
    ];
    let features = [
        "dashboard",
        "reports",
        "api",
        "integrations",
        "automations",
        "analytics",
        "exports",
    ];
    let priorities = ["low", "medium", "high", "critical"];
    let ticket_cats = [
        "billing",
        "technical",
        "feature_request",
        "onboarding",
        "other",
    ];
    let countries = ["US", "UK", "DE", "Fregister_arrowR", "CA", "AU"];
    let platforms = ["web", "mobile", "api"];

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Accounts
    crate::generate_table(
        pool,
        "accounts",
        nac,
        &pb,
        "Generating accounts...",
        |start, end| {
            let mut rng = SmallRng::seed_from_u64(start as u64);
            let i: Vec<i64> = (start..end).map(|i| i as i64).collect();
            let name: Vec<String> = (start..end).map(|i| format!("Account {}", i)).collect();
            let industry: Vec<&str> = (start..end)
                .map(|_| industries[rng.gen_range(0..industries.len())])
                .collect();
            let country: Vec<&str> = (start..end)
                .map(|_| countries[rng.gen_range(0..countries.len())])
                .collect();
            let arr: Vec<f64> = (start..end)
                .map(|_| ((rng.gen_range(5000.0..500000.0) * 100.0) as f64).round() / 100.0)
                .collect();
            let created: Vec<i32> = (start..end)
                .map(|_| {
                    Date32Type::from_naive_date(base_date + Duration::days(rng.gen_range(0..701)))
                })
                .collect();
            let csm: Vec<i32> = (start..end).map(|_| rng.gen_range(1..21)).collect();
            let health: Vec<i8> = (start..end).map(|_| rng.gen_range(1..=100) as i8).collect();
            vec![
                Arc::new(Int64Array::from(i)),
                Arc::new(StringArray::from(name)),
                Arc::new(StringArray::from(industry)),
                Arc::new(StringArray::from(country)),
                Arc::new(Float64Array::from(arr)),
                Arc::new(Date32Array::from(created)),
                Arc::new(Int32Array::from(csm)),
                Arc::new(Int8Array::from(health)),
            ]
        },
    )?;

    // 2. Subscriptions
    crate::generate_table(
        pool,
        "subscriptions",
        nsb,
        &pb,
        "Generating subscriptions...",
        |start, end| {
            let mut rng = SmallRng::seed_from_u64(start as u64);
            let i: Vec<i64> = (start..end).map(|i| i as i64).collect();
            let acc_id: Vec<i32> = (start..end)
                .map(|_| rng.gen_range(1..=nac) as i32)
                .collect();
            let plan: Vec<&str> = (start..end)
                .map(|_| plans[rng.gen_range(0..plans.len())])
                .collect();
            let seats: Vec<i32> = (start..end).map(|_| rng.gen_range(1..201)).collect();
            let mrr: Vec<f64> = (start..end)
                .map(|_| ((rng.gen_range(99.0..9999.0) * 100.0) as f64).round() / 100.0)
                .collect();
            let start_date: Vec<i32> = (start..end)
                .map(|_| {
                    Date32Type::from_naive_date(base_date + Duration::days(rng.gen_range(0..601)))
                })
                .collect();
            let end_date: Vec<i32> = start_date
                .iter()
                .map(|s| {
                    Date32Type::from_naive_date(
                        Date32Type::to_naive_date_opt(*s).unwrap() + Duration::days(365),
                    )
                })
                .collect();
            let active: Vec<bool> = (start..end).map(|_| rng.gen_bool(0.9)).collect();
            vec![
                Arc::new(Int64Array::from(i)),
                Arc::new(Int32Array::from(acc_id)),
                Arc::new(StringArray::from(plan)),
                Arc::new(Int32Array::from(seats)),
                Arc::new(Float64Array::from(mrr)),
                Arc::new(Date32Array::from(start_date)),
                Arc::new(Date32Array::from(end_date.clone())),
                Arc::new(BooleanArray::from(active)),
                Arc::new(Date32Array::from(end_date)),
            ]
        },
    )?;

    // 3. Events
    crate::generate_table(
        pool,
        "events",
        nev,
        &pb,
        "Generating events...",
        |start, end| {
            let mut rng = SmallRng::seed_from_u64(start as u64);
            let i: Vec<i64> = (start..end).map(|i| i as i64).collect();
            let acc_id: Vec<i32> = (start..end)
                .map(|_| rng.gen_range(1..=nac) as i32)
                .collect();
            let user_id: Vec<i32> = (start..end)
                .map(|_| rng.gen_range(1..=nac * 5) as i32)
                .collect();
            let etype: Vec<&str> = (start..end)
                .map(|_| etypes[rng.gen_range(0..etypes.len())])
                .collect();
            let ts: Vec<i64> = (start..end)
                .map(|_| {
                    TimestampMillisecondType::from_naive_datetime(
                        base_ts + Duration::seconds(rng.gen_range(0..700 * 86400)),
                        None,
                    )
                    .unwrap()
                })
                .collect();
            let session: Vec<String> = (start..end)
                .map(|_| format!("sess_{}", rng.gen_range(1..nac * 20)))
                .collect();
            let platform: Vec<&str> = (start..end)
                .map(|_| platforms[rng.gen_range(0..platforms.len())])
                .collect();
            vec![
                Arc::new(Int64Array::from(i)),
                Arc::new(Int32Array::from(acc_id)),
                Arc::new(Int32Array::from(user_id)),
                Arc::new(StringArray::from(etype)),
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(StringArray::from(session)),
                Arc::new(StringArray::from(platform)),
            ]
        },
    )?;

    // 4. Feature Usage
    crate::generate_table(
        pool,
        "feature_usage",
        nfu,
        &pb,
        "Generating feature usage...",
        |start, end| {
            let mut rng = SmallRng::seed_from_u64(start as u64);
            let i: Vec<i64> = (start..end).map(|i| i as i64).collect();
            let acc_id: Vec<i32> = (start..end)
                .map(|_| rng.gen_range(1..=nac) as i32)
                .collect();
            let feature: Vec<&str> = (start..end)
                .map(|_| features[rng.gen_range(0..features.len())])
                .collect();
            let date: Vec<i32> = (start..end)
                .map(|_| {
                    Date32Type::from_naive_date(base_date + Duration::days(rng.gen_range(0..701)))
                })
                .collect();
            let count: Vec<i32> = (start..end).map(|_| rng.gen_range(1..1001)).collect();
            vec![
                Arc::new(Int64Array::from(i)),
                Arc::new(Int32Array::from(acc_id)),
                Arc::new(StringArray::from(feature)),
                Arc::new(Date32Array::from(date)),
                Arc::new(Int32Array::from(count)),
            ]
        },
    )?;

    // 5. Support Tickets
    crate::generate_table(
        pool,
        "support_tickets",
        nst,
        &pb,
        "Generating support tickets...",
        |start, end| {
            let mut rng = SmallRng::seed_from_u64(start as u64);
            let i: Vec<i64> = (start..end).map(|i| i as i64).collect();
            let acc_id: Vec<i32> = (start..end)
                .map(|_| rng.gen_range(1..=nac) as i32)
                .collect();
            let created: Vec<i64> = (start..end)
                .map(|_| {
                    TimestampMillisecondType::from_naive_datetime(
                        base_ts + Duration::seconds(rng.gen_range(0..700 * 86400)),
                        None,
                    )
                    .unwrap()
                })
                .collect();
            let resolved: Vec<Option<i64>> = (start..end)
                .map(|_| {
                    if rng.gen_bool(0.8) {
                        Some(
                            TimestampMillisecondType::from_naive_datetime(
                                base_ts + Duration::seconds(rng.gen_range(0..700 * 86400)),
                                None,
                            )
                            .unwrap(),
                        )
                    } else {
                        None
                    }
                })
                .collect();
            let priority: Vec<&str> = (start..end)
                .map(|_| priorities[rng.gen_range(0..priorities.len())])
                .collect();
            let cat: Vec<&str> = (start..end)
                .map(|_| ticket_cats[rng.gen_range(0..ticket_cats.len())])
                .collect();
            let csat: Vec<Option<i8>> = (start..end)
                .map(|_| {
                    if rng.gen_bool(0.7) {
                        Some(rng.gen_range(1..6) as i8)
                    } else {
                        None
                    }
                })
                .collect();
            let is_resolved: Vec<bool> = (start..end).map(|_| rng.gen_bool(0.8)).collect();
            vec![
                Arc::new(Int64Array::from(i)),
                Arc::new(Int32Array::from(acc_id)),
                Arc::new(TimestampMillisecondArray::from(created)),
                Arc::new(TimestampMillisecondArray::from(resolved)),
                Arc::new(StringArray::from(priority)),
                Arc::new(StringArray::from(cat)),
                Arc::new(Int8Array::from(csat)),
                Arc::new(BooleanArray::from(is_resolved)),
            ]
        },
    )?;

    pb.finish_with_message("p06_saas complete");

    Ok(())
}
