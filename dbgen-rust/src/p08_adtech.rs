use chrono::{Duration, NaiveDate, NaiveDateTime};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 84.0;
    let nca = (200.0 * sf_adj).max(10.0) as usize;
    let nimp = (500000.0 * sf_adj).max(100.0) as usize;
    let ncl = (15000.0 * sf_adj).max(20.0) as usize;
    let ncv = (3000.0 * sf_adj).max(5.0) as usize;

    con.execute_batch(
        "DROP TABLE IF EXISTS conversions; DROP TABLE IF EXISTS clicks;
         DROP TABLE IF EXISTS impressions; DROP TABLE IF EXISTS campaigns;
         CREATE TABLE campaigns(campaign_id INTEGER PRIMARY KEY, name VARCHAR,
             advertiser VARCHAR, channel VARCHAR, objective VARCHAR,
             start_date DATE, end_date DATE, budget DECIMAL(12,2), cpm_target DECIMAL(6,2));
         CREATE TABLE impressions(imp_id BIGINT PRIMARY KEY, campaign_id INTEGER,
             user_id BIGINT, imp_ts TIMESTAMP, device VARCHAR, geo VARCHAR,
             placement VARCHAR, cost_usd DECIMAL(8,6));
         CREATE TABLE clicks(click_id BIGINT PRIMARY KEY, imp_id BIGINT,
             campaign_id INTEGER, user_id BIGINT, click_ts TIMESTAMP,
             landing_url VARCHAR, device VARCHAR);
         CREATE TABLE conversions(conv_id INTEGER PRIMARY KEY, click_id BIGINT,
             campaign_id INTEGER, user_id BIGINT, conv_ts TIMESTAMP,
             conv_type VARCHAR, revenue DECIMAL(10,2));",
    )?;

    let base_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let base_ts = base_date.and_hms_opt(0, 0, 0).unwrap();
    let channels = ["search", "social", "display", "video", "email", "affiliate"];
    let objectives = ["awareness", "traffic", "leads", "sales", "retention"];
    let devices = ["desktop", "mobile", "tablet", "ctv"];
    let geos = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "BR"];
    let placements = [
        "header",
        "sidebar",
        "feed",
        "pre-roll",
        "interstitial",
        "sponsored",
    ];
    let ctypes = ["purchase", "lead", "signup", "download", "call"];

    let pb = ProgressBar::new(4);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Campaigns
    crate::generate_table_parallel(con, "campaigns", nca, &pb, "Generating campaigns...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Campaign {}", i);
        let advertiser = format!("Brand {}", rng.gen_range(1..21));
        let channel = channels[rng.gen_range(0..channels.len())];
        let objective = objectives[rng.gen_range(0..objectives.len())];
        let start = base_date + Duration::days(rng.gen_range(0..201));
        let end = base_date + Duration::days(rng.gen_range(200..366));
        let budget = ((rng.gen_range(5000.0..500000.0) * 100.0) as f64).round() / 100.0;
        let cpm = ((rng.gen_range(0.5..15.0) * 100.0) as f64).round() / 100.0;
        (
            i as i32, name, advertiser, channel, objective, start, end, budget, cpm,
        )
    })?;

    // 2. Impressions
    crate::generate_table_parallel(
        con,
        "impressions",
        nimp,
        &pb,
        "Generating impressions...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let campaign_id = rng.gen_range(1..=nca) as i32;
            let user_id = rng.gen_range(1..=nimp as i64 * 100);
            let ts = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
            let device = devices[rng.gen_range(0..devices.len())];
            let geo = geos[rng.gen_range(0..geos.len())];
            let placement = placements[rng.gen_range(0..placements.len())];
            let cost = ((rng.gen_range(0.0001..0.05) * 1000000.0) as f64).round() / 1000000.0;
            (
                i as i64,
                campaign_id,
                user_id,
                ts,
                device,
                geo,
                placement,
                cost,
            )
        },
    )?;

    // Get samples for clicks
    let mut stmt = con.prepare(
        "SELECT imp_id, campaign_id, user_id, imp_ts, device FROM impressions USING SAMPLE ? ROWS",
    )?;
    let imp_refs: Vec<(i64, i32, i64, NaiveDateTime, String)> = stmt
        .query_map([ncl], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // 3. Clicks
    crate::generate_table_parallel(con, "clicks", ncl, &pb, "Generating clicks...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let ref_idx = rng.gen_range(0..imp_refs.len());
        let (imp_id, camp_id, user_id, imp_ts, device) = &imp_refs[ref_idx];
        let click_ts = *imp_ts + Duration::seconds(rng.gen_range(1..3601));
        let url = format!("https://brand.com/lp/{}", rng.gen_range(1..21));
        (
            i as i64,
            *imp_id,
            *camp_id,
            *user_id,
            click_ts,
            url,
            device.clone(),
        )
    })?;

    // Get samples for conversions
    let mut stmt =
        con.prepare("SELECT click_id, campaign_id, user_id FROM clicks USING SAMPLE ? ROWS")?;
    let click_refs: Vec<(i64, i32, i64)> = stmt
        .query_map([ncv], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    // 4. Conversions
    crate::generate_table_parallel(
        con,
        "conversions",
        ncv,
        &pb,
        "Generating conversions...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let ref_idx = rng.gen_range(0..click_refs.len());
            let (click_id, camp_id, user_id) = click_refs[ref_idx];
            let conv_ts = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
            let ctype = ctypes[rng.gen_range(0..ctypes.len())];
            let rev = ((rng.gen_range(0.0..500.0) * 100.0) as f64).round() / 100.0;
            (i as i32, click_id, camp_id, user_id, conv_ts, ctype, rev)
        },
    )?;

    pb.finish_with_message("p08_adtech complete");

    Ok(())
}
