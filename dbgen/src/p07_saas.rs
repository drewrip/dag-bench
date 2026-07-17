use std::sync::Arc;

use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampMillisecondArray,
};
use duckdb::arrow::datatypes::{ArrowTimestampType, Date32Type, TimestampMillisecondType};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>, no_constraints: bool) -> duckdb::Result<()> {
    let nac = (343952.0 * sf).max(10.0) as usize;
    // Subscriptions/events/feature_usage are fan-out ratios off accounts.
    let nsb = ((nac as f64) * 1.4).max(10.0) as usize;
    let avg_events_per_account = 100.0;
    let avg_feature_usage_per_account = 10.0;
    let nev = ((nac as f64) * avg_events_per_account).max(100.0) as usize;
    let nfu = ((nac as f64) * avg_feature_usage_per_account).max(20.0) as usize;
    let nst = ((nac as f64) * 4.0).max(10.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(&crate::common::schema_sql(
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
        no_constraints,
    ))?;

    let base_date = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let base_ts = base_date.and_hms_opt(0, 0, 0).unwrap();

    // Fixed vocabularies.
    let industries = [
        "Software/SaaS", "Financial Services", "Retail/E-commerce", "Healthcare",
        "Manufacturing", "Media/Entertainment", "Education", "Government/Public Sector", "Other",
    ];
    let industry_weights = [20.0, 15.0, 12.0, 12.0, 10.0, 10.0, 10.0, 6.0, 5.0];
    let countries = ["US", "UK", "CA", "DE", "AU", "FR", "IN", "BR", "JP"];
    let country_weights = [40.0, 12.0, 10.0, 10.0, 8.0, 6.0, 6.0, 4.0, 4.0];
    let plans = ["starter", "professional", "business", "enterprise"];
    let plan_weights = [35.0, 35.0, 20.0, 10.0];
    let plan_per_seat_rate = [20.0, 50.0, 90.0, 150.0];
    let etypes = ["page_view", "feature_used", "login", "api_call", "export", "report_generated", "invite_sent"];
    let etype_weights = [30.0, 25.0, 15.0, 12.0, 8.0, 6.0, 4.0];
    let platforms = ["web", "api", "ios", "android"];
    let platform_weights = [65.0, 20.0, 8.0, 7.0];
    let features = [
        "dashboards", "reporting", "alerts", "integrations", "api_access", "sso", "automation",
        "exports", "search", "collaboration", "audit_logs", "custom_fields", "mobile_app",
    ];
    let feature_weights = [22.0, 18.0, 15.0, 8.0, 7.0, 6.0, 6.0, 5.0, 4.0, 3.0, 2.0, 2.0, 2.0];
    let priorities = ["low", "medium", "high", "urgent"];
    let priority_weights = [40.0, 35.0, 18.0, 7.0];
    let ticket_cats = ["technical", "billing", "bug", "onboarding", "feature_request", "account_access"];
    let ticket_cat_weights = [30.0, 18.0, 18.0, 15.0, 12.0, 7.0];

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
                .map(|_| weighted_choice(&mut rng, &industries, &industry_weights))
                .collect();
            let country: Vec<&str> = (start..end)
                .map(|_| weighted_choice(&mut rng, &countries, &country_weights))
                .collect();
            let arr: Vec<f64> = (start..end)
                .map(|_| round_to(rng.gen_range(5000.0..500000.0), 2))
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

    // Materialize arr/health_score so downstream tables can correlate with them
    // instead of drawing account activity independently.
    let mut stmt = con.prepare("SELECT arr, health_score FROM accounts ORDER BY account_id")?;
    let account_facts: Vec<(f64, i8)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let arr_health_factor: Vec<f64> = account_facts
        .iter()
        .map(|(arr, health)| (*arr).sqrt() * (*health as f64 / 100.0).max(0.05))
        .collect();
    let ticket_factor: Vec<f64> = account_facts
        .iter()
        .map(|(_, health)| (100.0 - *health as f64).max(1.0))
        .collect();
    let account_engagement_popularity = PopularityWeights::from_factors(&arr_health_factor);
    let account_ticket_popularity = PopularityWeights::from_factors(&ticket_factor);

    // 2. Subscriptions: mrr is a deterministic function of plan/seats plus small noise,
    // not independent.
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
            let plan_idx: Vec<usize> = (start..end)
                .map(|_| weighted_choice(&mut rng, &(0..plans.len()).collect::<Vec<_>>(), &plan_weights))
                .collect();
            let plan: Vec<&str> = plan_idx.iter().map(|&p| plans[p]).collect();
            let seats: Vec<i32> = (start..end).map(|_| rng.gen_range(1..201)).collect();
            let mrr: Vec<f64> = plan_idx
                .iter()
                .zip(seats.iter())
                .map(|(&p, &s)| {
                    round_to(
                        (s as f64) * plan_per_seat_rate[p] * rng.gen_range(0.9..1.1),
                        2,
                    )
                })
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
                // renewal_date is a denormalized copy of end_date, not an independently
                // random field.
                Arc::new(Date32Array::from(end_date)),
            ]
        },
    )?;

    // 3. Events (popularity-weighted by account arr/health [CHANGE from dbgen])
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
                .map(|_| account_engagement_popularity.sample(&mut rng) as i32)
                .collect();
            let user_id: Vec<i32> = acc_id
                .iter()
                .map(|&a| {
                    // roughly one synthetic user pool per account, sized off seat count
                    let pool_size = 20;
                    (a - 1) * pool_size + rng.gen_range(1..=pool_size)
                })
                .collect();
            let etype: Vec<&str> = (start..end)
                .map(|_| weighted_choice(&mut rng, &etypes, &etype_weights))
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
            let session: Vec<String> = acc_id
                .iter()
                .map(|&a| format!("sess_{}_{}", a, rng.gen_range(1..500)))
                .collect();
            let platform: Vec<&str> = (start..end)
                .map(|_| weighted_choice(&mut rng, &platforms, &platform_weights))
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

    // 4. Feature Usage (popularity-weighted like events)
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
                .map(|_| account_engagement_popularity.sample(&mut rng) as i32)
                .collect();
            let feature: Vec<&str> = (start..end)
                .map(|_| weighted_choice(&mut rng, &features, &feature_weights))
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

    // 5. Support Tickets: volume correlates inversely with health_score
    // [CHANGE from dbgen] rather than being independent of it.
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
                .map(|_| account_ticket_popularity.sample(&mut rng) as i32)
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
                .map(|_| weighted_choice(&mut rng, &priorities, &priority_weights))
                .collect();
            let cat: Vec<&str> = (start..end)
                .map(|_| weighted_choice(&mut rng, &ticket_cats, &ticket_cat_weights))
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

    pb.finish_with_message("p07_saas complete");

    Ok(())
}
