use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let na = (1988346.0 * sf).max(10.0) as usize;
    let nm = (596340.0 * sf).max(10.0) as usize;
    // Transactions fan out off accounts; alerts fan out off flagged txns.
    let avg_txn_per_account = 20.0;
    let nt = ((na as f64) * avg_txn_per_account).max(50.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

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

    let base_ts = NaiveDate::from_ymd_opt(2022, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    // Fixed vocabularies.
    let account_types = ["checking", "savings", "credit_card", "business"];
    let account_type_weights = [45.0, 30.0, 20.0, 5.0];
    let countries_acc = ["US", "CA", "UK", "DE", "FR", "MX", "AU", "JP", "BR", "IN"];
    let countries_acc_weights = [60.0, 6.0, 6.0, 5.0, 5.0, 5.0, 4.0, 3.0, 3.0, 3.0];
    let merchant_categories = [
        "grocery", "restaurant", "online_retail", "gas_station", "electronics", "travel",
        "entertainment", "utilities", "other",
    ];
    let merchant_category_weights = [18.0, 16.0, 15.0, 12.0, 10.0, 8.0, 8.0, 7.0, 6.0];
    let countries_merch = ["US", "GB", "CN", "NG", "RU", "MX", "DE"];
    let countries_merch_weights = [45.0, 15.0, 12.0, 10.0, 8.0, 6.0, 4.0];
    let risk_tiers = ["low", "medium", "high"];
    let risk_tier_weights = [60.0, 30.0, 10.0];
    let txn_channels = ["card_present", "online", "mobile_wallet", "card_not_present", "atm"];
    let txn_channel_weights = [45.0, 30.0, 15.0, 7.0, 3.0];
    let currencies = ["USD", "EUR", "GBP", "CAD", "other"];
    let currency_weights = [85.0, 6.0, 4.0, 3.0, 2.0];
    let decline_codes = [
        "insufficient_funds",
        "do_not_honor",
        "expired_card",
        "invalid_pin",
        "fraud_suspected",
    ];
    let decline_code_weights = [3.0, 2.0, 1.5, 1.0, 0.5];
    let alert_types = ["unusual_amount", "velocity", "geo_anomaly", "card_not_present", "identity_theft"];
    let alert_type_weights = [30.0, 25.0, 20.0, 15.0, 10.0];
    let severities = ["low", "medium", "high", "critical"];
    let severity_weights = [35.0, 35.0, 22.0, 8.0];
    let resolutions = ["false_positive", "customer_verified", "confirmed_fraud"];
    let resolution_weights = [45.0, 30.0, 25.0];

    // Transactions skew toward a few high-activity accounts and merchants.
    let account_popularity = PopularityWeights::new(na, 1.0, 71);
    let merchant_popularity = PopularityWeights::new(nm, 1.1, 82);

    let pb = ProgressBar::new(4);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Accounts
    crate::generate_table_parallel(pool, "accounts", na, &pb, "Generating accounts...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let holder_name = format!("Holder {}", i);
        let atype = weighted_choice(&mut rng, &account_types, &account_type_weights);
        let country = weighted_choice(&mut rng, &countries_acc, &countries_acc_weights);
        let limit = round_to(rng.gen_range(500.0..50000.0), 2);
        let opened_date = (base_ts - Duration::days(rng.gen_range(30..3651))).date();
        let is_frozen = rng.gen_bool(0.03);
        (
            i as i32,
            holder_name,
            atype,
            country,
            limit,
            opened_date,
            is_frozen,
        )
    })?;

    // 2. Merchants
    crate::generate_table_parallel(pool, "merchants", nm, &pb, "Generating merchants...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Merchant {}", i);
        let cat = weighted_choice(&mut rng, &merchant_categories, &merchant_category_weights);
        let country = weighted_choice(&mut rng, &countries_merch, &countries_merch_weights);
        let risk = weighted_choice(&mut rng, &risk_tiers, &risk_tier_weights);
        let avg_amount = round_to(rng.gen_range(5.0..500.0), 2);
        (i as i32, name, cat, country, risk, avg_amount)
    })?;

    // Materialize risk tier / frozen status so transactions can correlate with them,
    // instead of drawing is_flagged/is_declined independently.
    let mut stmt = con.prepare("SELECT risk_tier FROM merchants ORDER BY merchant_id")?;
    let merchant_risk: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut stmt = con.prepare("SELECT is_frozen FROM accounts ORDER BY account_id")?;
    let account_frozen: Vec<bool> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 3. Transactions
    crate::generate_table_parallel(
        pool,
        "transactions",
        nt,
        &pb,
        "Generating transactions...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let acc_id = account_popularity.sample(&mut rng);
            let merch_id = merchant_popularity.sample(&mut rng);
            let amount = round_to(rng.gen_range(1.0..5000.0), 2);
            let ts = base_ts + Duration::seconds(rng.gen_range(0..365 * 86400));
            let channel = weighted_choice(&mut rng, &txn_channels, &txn_channel_weights);
            let curr = weighted_choice(&mut rng, &currencies, &currency_weights);

            // Decline odds spike sharply for frozen accounts.
            let frozen = account_frozen[acc_id - 1];
            let decline_rate = if frozen { 0.85 } else { 0.05 };
            let declined = rng.gen_bool(decline_rate);

            // Flag odds scale with amount and merchant risk tier, tuned so
            // ~3-5% of transactions end up flagged overall.
            let risk = merchant_risk[merch_id - 1].as_str();
            let base_flag_rate: f64 = match risk {
                "high" => 0.10,
                "medium" => 0.04,
                _ => 0.02,
            };
            let amount_bump = if amount > 2000.0 { 1.8 } else { 1.0 };
            let flagged = rng.gen_bool((base_flag_rate * amount_bump).min(0.5));

            let rcode = if declined {
                weighted_choice(&mut rng, &decline_codes, &decline_code_weights)
            } else {
                "approved"
            };
            (
                i as i32, acc_id as i32, merch_id as i32, amount, ts, channel, curr, declined,
                flagged, rcode,
            )
        },
    )?;

    // Get flagged IDs for alerts, sized off the actual flagged count rather
    // than an unrelated flat constant.
    let flagged_count: i64 =
        con.query_row("SELECT COUNT(*) FROM transactions WHERE is_flagged", [], |row| {
            row.get(0)
        })?;
    let nal = ((flagged_count as f64) * 1.1).max(5.0) as usize;

    let mut stmt =
        con.prepare("SELECT txn_id FROM transactions WHERE is_flagged ORDER BY txn_id")?;
    let flagged_ids: Vec<i32> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<i32>, _>>()?;

    // 4. Alerts (filtered-subset sampled from flagged transactions)
    crate::generate_table_parallel(pool, "alerts", nal, &pb, "Generating alerts...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let txn_id = if !flagged_ids.is_empty() {
            flagged_ids[rng.gen_range(0..flagged_ids.len())]
        } else {
            rng.gen_range(1..=nt) as i32
        };
        let atype = weighted_choice(&mut rng, &alert_types, &alert_type_weights);
        let sev = weighted_choice(&mut rng, &severities, &severity_weights);
        let ts = base_ts + Duration::seconds(rng.gen_range(0..365 * 86400));
        let resolved = rng.gen_bool(0.6);
        let res = if resolved {
            Some(weighted_choice(&mut rng, &resolutions, &resolution_weights))
        } else {
            None
        };
        (i as i32, txn_id, atype, sev, ts, resolved, res)
    })?;

    pb.finish_with_message("p04_fraud complete");

    Ok(())
}
