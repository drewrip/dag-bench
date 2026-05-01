use chrono::{Duration, NaiveDate};
use duckdb::{params, Connection};
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rayon::prelude::*;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 2200.0;
    let na = (1000.0 * sf_adj).max(10.0) as usize;
    let nm = (300.0 * sf_adj).max(10.0) as usize;
    let nt = (20000.0 * sf_adj).max(50.0) as usize;
    let nal = (500.0 * sf_adj).max(5.0) as usize;

    con.execute_batch(
        "DROP TABLE IF EXISTS alerts; DROP TABLE IF EXISTS transactions;
         DROP TABLE IF EXISTS merchants; DROP TABLE IF EXISTS accounts;
         CREATE TABLE accounts(account_id INTEGER PRIMARY KEY, holder_name VARCHAR,
             account_type VARCHAR, country VARCHAR, credit_limit DECIMAL(12,2),
             opened_date DATE, is_frozen BOOLEAN);
         CREATE TABLE merchants(merchant_id INTEGER PRIMARY KEY, name VARCHAR,
             category VARCHAR, country VARCHAR, risk_tier VARCHAR,
             avg_txn_amount DECIMAL(10,2));
         CREATE TABLE transactions(txn_id INTEGER PRIMARY KEY, account_id INTEGER,
             merchant_id INTEGER, amount DECIMAL(12,2), txn_ts TIMESTAMP,
             channel VARCHAR, currency VARCHAR, is_declined BOOLEAN,
             is_flagged BOOLEAN, response_code VARCHAR);
         CREATE TABLE alerts(alert_id INTEGER PRIMARY KEY, txn_id INTEGER,
             alert_type VARCHAR, severity VARCHAR, created_ts TIMESTAMP,
             resolved BOOLEAN, resolution VARCHAR);",
    )?;

    let base_ts = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let atypes = ["checking", "savings", "credit", "business"];
    let cats = ["retail", "travel", "grocery", "online", "gaming", "crypto", "atm"];
    let risks = ["low", "medium", "high", "critical"];
    let chans = ["pos", "web", "mobile", "atm", "wire"];
    let currs = ["USD", "EUR", "GBP", "JPY", "BTC"];
    let rcodes = ["00", "01", "05", "14", "51", "57", "96"];
    let atypes2 = ["velocity", "geo_anomaly", "amount_spike", "card_not_present", "identity"];
    let sevs = ["info", "warning", "critical"];
    let ress = ["confirmed_fraud", "false_positive", "under_review"];
    let countries_acc = ["US", "GB", "DE", "FR", "CA"];
    let countries_merch = ["US", "GB", "NG", "CN", "RU"];

    let pb = ProgressBar::new(4);
    pb.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
        .unwrap());

    // 1. Accounts
    pb.set_message("Generating accounts...");
    let chunk_size = 100_000;
    for chunk_start in (1..=na).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(na + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let holder_name = format!("Holder {}", i);
            let atype = atypes[rng.gen_range(0..atypes.len())];
            let country = countries_acc[rng.gen_range(0..countries_acc.len())];
            let limit = ((rng.gen_range(500.0..50000.0) * 100.0) as f64).round() / 100.0;
            let opened_date = (base_ts - Duration::days(rng.gen_range(30..3651))).date();
            let is_frozen = rng.gen_bool(0.03);
            (i as i32, holder_name, atype, country, limit, opened_date, is_frozen)
        }).collect();

        let mut appender = con.appender("accounts")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6])?;
        }
    }
    pb.inc(1);

    // 2. Merchants
    pb.set_message("Generating merchants...");
    for chunk_start in (1..=nm).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nm + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let name = format!("Merchant {}", i);
            let cat = cats[rng.gen_range(0..cats.len())];
            let country = countries_merch[rng.gen_range(0..countries_merch.len())];
            let risk = risks[rng.gen_range(0..risks.len())];
            let avg_amount = ((rng.gen_range(5.0..500.0) * 100.0) as f64).round() / 100.0;
            (i as i32, name, cat, country, risk, avg_amount)
        }).collect();

        let mut appender = con.appender("merchants")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5])?;
        }
    }
    pb.inc(1);

    // 3. Transactions
    pb.set_message("Generating transactions...");
    for chunk_start in (1..=nt).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nt + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let acc_id = rng.gen_range(1..=na) as i32;
            let merch_id = rng.gen_range(1..=nm) as i32;
            let amount = ((rng.gen_range(1.0..5000.0) * 100.0) as f64).round() / 100.0;
            let ts = base_ts + Duration::seconds(rng.gen_range(0..365 * 86400));
            let channel = chans[rng.gen_range(0..chans.len())];
            let curr = currs[rng.gen_range(0..currs.len())];
            let declined = rng.gen_bool(0.05);
            let flagged = rng.gen_bool(0.04);
            let rcode = rcodes[rng.gen_range(0..rcodes.len())];
            (i as i32, acc_id, merch_id, amount, ts, channel, curr, declined, flagged, rcode)
        }).collect();

        let mut appender = con.appender("transactions")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9])?;
        }
    }
    pb.inc(1);

    // Get flagged IDs for alerts
    let mut stmt = con.prepare("SELECT txn_id FROM transactions WHERE is_flagged ORDER BY txn_id LIMIT ?")?;
    let flagged_ids: Vec<i32> = stmt.query_map([nal as i32], |row| row.get(0))?
        .collect::<Result<Vec<i32>, _>>()?;

    // 4. Alerts
    pb.set_message("Generating alerts...");
    for chunk_start in (1..=nal).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(nal + 1);
        let rows: Vec<_> = (chunk_start..chunk_end).into_par_iter().map(|i| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let txn_id = if !flagged_ids.is_empty() {
                flagged_ids[rng.gen_range(0..flagged_ids.len())]
            } else {
                rng.gen_range(1..=nt) as i32
            };
            let atype2 = atypes2[rng.gen_range(0..atypes2.len())];
            let sev = sevs[rng.gen_range(0..sevs.len())];
            let ts = base_ts + Duration::seconds(rng.gen_range(0..365 * 86400));
            let resolved = rng.gen_bool(0.6);
            let res = if rng.gen_bool(0.6) {
                Some(ress[rng.gen_range(0..ress.len())])
            } else {
                None
            };
            (i as i32, txn_id, atype2, sev, ts, resolved, res)
        }).collect();

        let mut appender = con.appender("alerts")?;
        for row in rows {
            appender.append_row(params![row.0, row.1, row.2, row.3, row.4, row.5, row.6])?;
        }
    }
    pb.finish_with_message("p02_fraud complete");

    Ok(())
}
