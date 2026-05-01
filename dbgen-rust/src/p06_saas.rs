use chrono::{Duration, NaiveDate};
use duckdb::{params, Connection};
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rayon::prelude::*;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 800.0;
    let nac = (500.0 * sf_adj).max(10.0) as usize;
    let nsb = (700.0 * sf_adj).max(10.0) as usize;
    let nev = (50000.0 * sf_adj).max(100.0) as usize;
    let nfu = (5000.0 * sf_adj).max(20.0) as usize;
    let nst = (2000.0 * sf_adj).max(10.0) as usize;

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
    let industries = ["fintech", "healthtech", "edtech", "ecommerce", "manufacturing", "media"];
    let plans = ["starter", "growth", "enterprise", "enterprise_plus"];
    let etypes = ["login", "page_view", "feature_click", "export", "api_call", "report_view"];
    let features = ["dashboard", "reports", "api", "integrations", "automations", "analytics", "exports"];
    let priorities = ["low", "medium", "high", "critical"];
    let ticket_cats = ["billing", "technical", "feature_request", "onboarding", "other"];
    let countries = ["US", "UK", "DE", "FR", "CA", "AU"];
    let platforms = ["web", "mobile", "api"];

    let pb = ProgressBar::new(5);
    pb.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
        .unwrap());

    // 1. Accounts
    pb.set_message("Generating accounts...");
    let mut appender = con.appender("accounts")?;
    for i in 1..=nac {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let name = format!("Account {}", i);
        let industry = industries[rng.gen_range(0..industries.len())];
        let country = countries[rng.gen_range(0..countries.len())];
        let arr = ((rng.gen_range(5000.0..500000.0) * 100.0) as f64).round() / 100.0;
        let created = base_date + Duration::days(rng.gen_range(0..701));
        let csm = rng.gen_range(1..21);
        let health = rng.gen_range(1..=100) as i8;
        appender.append_row(params![i as i32, name, industry, country, arr, created, csm, health])?;
    }
    drop(appender);
    pb.inc(1);

    // 2. Subscriptions
    pb.set_message("Generating subscriptions...");
    let chunk_size = 100_000;
    for chunk_start in (1..=nsb).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nsb + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let acc_id = rng.gen_range(1..=nac) as i32;
            let plan = plans[rng.gen_range(0..plans.len())];
            let seats = rng.gen_range(1..201);
            let mrr = ((rng.gen_range(99.0..9999.0) * 100.0) as f64).round() / 100.0;
            let start = base_date + Duration::days(rng.gen_range(0..601));
            let end = start + Duration::days(365);
            let active = rng.gen_bool(0.9);
            (i as i32, acc_id, plan, seats, mrr, start, end, active, end)
        }).collect();

        let mut appender = con.appender("subscriptions")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8])?;
        }
    }
    pb.inc(1);

    // 3. Events
    pb.set_message("Generating events...");
    for chunk_start in (1..=nev).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nev + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let acc_id = rng.gen_range(1..=nac) as i32;
            let user_id = rng.gen_range(1..=nac * 5) as i32;
            let etype = etypes[rng.gen_range(0..etypes.len())];
            let ts = base_ts + Duration::seconds(rng.gen_range(0..700 * 86400));
            let session = format!("sess_{}", rng.gen_range(1..nac * 20));
            let platform = platforms[rng.gen_range(0..platforms.len())];
            (i as i64, acc_id, user_id, etype, ts, session, platform)
        }).collect();

        let mut appender = con.appender("events")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6])?;
        }
    }
    pb.inc(1);

    // 4. Feature Usage
    pb.set_message("Generating feature usage...");
    for chunk_start in (1..=nfu).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nfu + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let acc_id = rng.gen_range(1..=nac) as i32;
            let feature = features[rng.gen_range(0..features.len())];
            let date = base_date + Duration::days(rng.gen_range(0..701));
            let count = rng.gen_range(1..1001);
            (i as i32, acc_id, feature, date, count)
        }).collect();

        let mut appender = con.appender("feature_usage")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4])?;
        }
    }
    pb.inc(1);

    // 5. Support Tickets
    pb.set_message("Generating support tickets...");
    for chunk_start in (1..=nst).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nst + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let acc_id = rng.gen_range(1..=nac) as i32;
            let created = base_ts + Duration::seconds(rng.gen_range(0..700 * 86400));
            let resolved = if rng.gen_bool(0.8) {
                Some(base_ts + Duration::seconds(rng.gen_range(0..700 * 86400)))
            } else {
                None
            };
            let priority = priorities[rng.gen_range(0..priorities.len())];
            let cat = ticket_cats[rng.gen_range(0..ticket_cats.len())];
            let csat = if rng.gen_bool(0.7) {
                Some(rng.gen_range(1..6) as i8)
            } else {
                None
            };
            let is_resolved = rng.gen_bool(0.8);
            (i as i32, acc_id, created, resolved, priority, cat, csat, is_resolved)
        }).collect();

        let mut appender = con.appender("support_tickets")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7])?;
        }
    }
    pb.finish_with_message("p06_saas complete");

    Ok(())
}
