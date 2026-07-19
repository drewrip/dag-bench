use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(
    sf: f64,
    pool: &mut Pool<DuckdbConnectionManager>,
    no_constraints: bool,
) -> duckdb::Result<()> {
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
        "Software/SaaS",
        "Financial Services",
        "Retail/E-commerce",
        "Healthcare",
        "Manufacturing",
        "Media/Entertainment",
        "Education",
        "Government/Public Sector",
        "Other",
    ];
    let industry_weights = [20.0, 15.0, 12.0, 12.0, 10.0, 10.0, 10.0, 6.0, 5.0];
    let countries = ["US", "UK", "CA", "DE", "AU", "FR", "IN", "BR", "JP"];
    let country_weights = [40.0, 12.0, 10.0, 10.0, 8.0, 6.0, 6.0, 4.0, 4.0];
    let plans = ["starter", "professional", "business", "enterprise"];
    let plan_weights = [35.0, 35.0, 20.0, 10.0];
    let plan_per_seat_rate = [20.0, 50.0, 90.0, 150.0];
    let etypes = [
        "page_view",
        "feature_used",
        "login",
        "api_call",
        "export",
        "report_generated",
        "invite_sent",
    ];
    let etype_weights = [30.0, 25.0, 15.0, 12.0, 8.0, 6.0, 4.0];
    let platforms = ["web", "api", "ios", "android"];
    let platform_weights = [65.0, 20.0, 8.0, 7.0];
    let features = [
        "dashboards",
        "reporting",
        "alerts",
        "integrations",
        "api_access",
        "sso",
        "automation",
        "exports",
        "search",
        "collaboration",
        "audit_logs",
        "custom_fields",
        "mobile_app",
    ];
    let feature_weights = [
        22.0, 18.0, 15.0, 8.0, 7.0, 6.0, 6.0, 5.0, 4.0, 3.0, 2.0, 2.0, 2.0,
    ];
    let priorities = ["low", "medium", "high", "urgent"];
    let priority_weights = [40.0, 35.0, 18.0, 7.0];
    let ticket_cats = [
        "technical",
        "billing",
        "bug",
        "onboarding",
        "feature_request",
        "account_access",
    ];
    let ticket_cat_weights = [30.0, 18.0, 18.0, 15.0, 12.0, 7.0];

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Accounts
    crate::generate_table_parallel(pool, "accounts", nac, &pb, "Generating accounts...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Account {}", i);
        let industry = weighted_choice(&mut rng, &industries, &industry_weights);
        let country = weighted_choice(&mut rng, &countries, &country_weights);
        let arr = round_to(rng.gen_range(5000.0..500000.0), 2);
        let created = base_date + Duration::days(rng.gen_range(0..701));
        let csm = rng.gen_range(1..21);
        let health = rng.gen_range(1..=100) as i8;
        (
            i as i32, name, industry, country, arr, created, csm, health,
        )
    })?;

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
    crate::generate_table_parallel(
        pool,
        "subscriptions",
        nsb,
        &pb,
        "Generating subscriptions...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let acc_id = rng.gen_range(1..=nac) as i32;
            let plan_idx = weighted_choice(
                &mut rng,
                &(0..plans.len()).collect::<Vec<_>>(),
                &plan_weights,
            );
            let plan = plans[plan_idx];
            let seats = rng.gen_range(1..201);
            let mrr = round_to(
                (seats as f64) * plan_per_seat_rate[plan_idx] * rng.gen_range(0.9..1.1),
                2,
            );
            let start_date = base_date + Duration::days(rng.gen_range(0..601));
            let end_date = start_date + Duration::days(365);
            let active = rng.gen_bool(0.9);
            (
                i as i32,
                acc_id,
                plan,
                seats,
                mrr,
                start_date,
                end_date,
                active,
                // renewal_date is a denormalized copy of end_date, not an independently
                // random field.
                end_date,
            )
        },
    )?;

    // 3. Events (popularity-weighted by account arr/health)
    crate::generate_table_parallel(pool, "events", nev, &pb, "Generating events...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let acc_id = account_engagement_popularity.sample(&mut rng) as i32;
        // roughly one synthetic user pool per account, sized off seat count
        let pool_size = 20;
        let user_id = (acc_id - 1) * pool_size + rng.gen_range(1..=pool_size);
        let etype = weighted_choice(&mut rng, &etypes, &etype_weights);
        let ts = base_ts + Duration::seconds(rng.gen_range(0..700 * 86400));
        let session = format!("sess_{}_{}", acc_id, rng.gen_range(1..500));
        let platform = weighted_choice(&mut rng, &platforms, &platform_weights);
        (
            i as i64, acc_id, user_id, etype, ts, session, platform,
        )
    })?;

    // 4. Feature Usage (popularity-weighted like events)
    crate::generate_table_parallel(
        pool,
        "feature_usage",
        nfu,
        &pb,
        "Generating feature usage...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let acc_id = account_engagement_popularity.sample(&mut rng) as i32;
            let feature = weighted_choice(&mut rng, &features, &feature_weights);
            let date = base_date + Duration::days(rng.gen_range(0..701));
            let count = rng.gen_range(1..1001);
            (i as i32, acc_id, feature, date, count)
        },
    )?;

    // 5. Support Tickets: volume correlates inversely with health_score
    // rather than being independent of it.
    crate::generate_table_parallel(
        pool,
        "support_tickets",
        nst,
        &pb,
        "Generating support tickets...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let acc_id = account_ticket_popularity.sample(&mut rng) as i32;
            let created: NaiveDateTime = base_ts + Duration::seconds(rng.gen_range(0..700 * 86400));
            let resolved: Option<NaiveDateTime> = if rng.gen_bool(0.8) {
                Some(base_ts + Duration::seconds(rng.gen_range(0..700 * 86400)))
            } else {
                None
            };
            let priority = weighted_choice(&mut rng, &priorities, &priority_weights);
            let cat = weighted_choice(&mut rng, &ticket_cats, &ticket_cat_weights);
            let csat: Option<i8> = if rng.gen_bool(0.7) {
                Some(rng.gen_range(1..6) as i8)
            } else {
                None
            };
            let is_resolved = rng.gen_bool(0.8);
            (
                i as i32,
                acc_id,
                created,
                resolved,
                priority,
                cat,
                csat,
                is_resolved,
            )
        },
    )?;

    pb.finish_with_message("p07_saas complete");

    Ok(())
}
