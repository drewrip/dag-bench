use crate::common::{lognormal_clamped, round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Exp, Gamma};

pub fn run(
    sf: f64,
    pool: &mut Pool<DuckdbConnectionManager>,
    no_constraints: bool,
) -> duckdb::Result<()> {
    let nca = (17608.0 * sf).max(10.0) as usize;
    // Impressions/clicks/conversions are fan-out ratios (avg per campaign,
    // CTR, CVR), not independent base constants.
    let avg_impressions_per_campaign = 2500.0;
    let ctr = 0.03;
    let cvr = 0.175;
    let nimp = ((nca as f64) * avg_impressions_per_campaign).max(100.0) as usize;
    let ncl = ((nimp as f64) * ctr).max(20.0) as usize;
    let ncv = ((ncl as f64) * cvr).max(5.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(&crate::common::schema_sql(
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
        no_constraints,
    ))?;

    let base_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let base_ts = base_date.and_hms_opt(0, 0, 0).unwrap();

    // Fixed vocabularies.
    let channels = ["search", "social", "display", "video", "native", "email"];
    let channel_weights = [30.0, 25.0, 20.0, 12.0, 8.0, 5.0];
    let objectives = ["conversion", "awareness", "consideration", "retargeting"];
    let objective_weights = [35.0, 25.0, 20.0, 20.0];
    let devices = ["mobile", "desktop", "tablet", "ctv"];
    let device_weights = [55.0, 30.0, 10.0, 5.0];
    let geos = [
        "US", "UK", "CA", "DE", "AU", "FR", "BR", "IN", "JP", "MX", "IT", "ES", "NL", "SE", "KR",
    ];
    let geo_weights = [
        40.0, 10.0, 8.0, 7.0, 6.0, 4.0, 4.0, 4.0, 3.0, 3.0, 2.0, 2.0, 2.0, 2.5, 2.5,
    ];
    let placements = ["banner", "native", "video", "interstitial"];
    let placement_weights = [35.0, 25.0, 25.0, 15.0];
    let ctypes = ["purchase", "lead", "signup", "app_install", "subscription"];
    let ctype_weights = [45.0, 20.0, 15.0, 12.0, 8.0];

    let pb = ProgressBar::new(4);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );
    // 1. Campaigns
    crate::generate_table_parallel(
        pool,
        "campaigns",
        nca,
        &pb,
        "Generating campaigns...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let name = format!("Campaign {}", i);
            let advertiser = format!("Brand {}", rng.gen_range(1..21));
            let channel_idx = weighted_choice(
                &mut rng,
                &(0..channels.len()).collect::<Vec<_>>(),
                &channel_weights,
            );
            let channel = channels[channel_idx];
            let objective = weighted_choice(&mut rng, &objectives, &objective_weights);
            let start = base_date + Duration::days(rng.gen_range(0..201));
            let end = base_date + Duration::days(rng.gen_range(200..366));
            let budget = round_to(
                lognormal_clamped(&mut rng, 60000.0, 0.8, 5000.0, 2_000_000.0),
                2,
            );
            let cpm = round_to(rng.gen_range(0.5..15.0), 2);
            (
                i as i32, name, advertiser, channel, objective, start, end, budget, cpm,
            )
        },
    )?;
    // Materialize each campaign's budget/cpm so impression volume and cost can correlate
    // with them, instead of drawing impression fields independently.
    let mut stmt = con.prepare("SELECT budget, cpm_target FROM campaigns ORDER BY campaign_id")?;
    let campaign_facts: Vec<(f64, f64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let budgets: Vec<f64> = campaign_facts.iter().map(|(b, _)| *b).collect();
    let campaign_popularity = PopularityWeights::from_factors(&budgets);

    // 2. Impressions
    crate::generate_table_parallel(
        pool,
        "impressions",
        nimp,
        &pb,
        "Generating impressions...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let campaign_id = campaign_popularity.sample(&mut rng);
            let cpm_target = campaign_facts[campaign_id - 1].1;
            let user_id = rng.gen_range(1..=nimp as i64 * 3);
            let ts = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
            let device = weighted_choice(&mut rng, &devices, &device_weights);
            let geo_idx =
                weighted_choice(&mut rng, &(0..geos.len()).collect::<Vec<_>>(), &geo_weights);
            let geo = geos[geo_idx];
            let placement = weighted_choice(&mut rng, &placements, &placement_weights);
            // Cost tracks the owning campaign's target CPM, not independently.
            let cost = round_to((cpm_target / 1000.0) * rng.gen_range(0.7..1.3), 6);
            (
                i as i64,
                campaign_id as i32,
                user_id,
                ts,
                device,
                geo,
                placement,
                cost,
            )
        },
    )?;
    // Get samples for clicks, weighted toward higher-traffic campaigns via SQL sampling.
    // `USING SAMPLE n ROWS` uses DuckDB's reservoir algorithm, which runs single-threaded and
    // scans the whole table; `PERCENT (system)` samples per-vector during a normal (parallel)
    // scan and is orders of magnitude faster. Exact pool size doesn't matter since callers
    // only draw from it with replacement.
    let click_sample_pct = (ncl as f64 / nimp as f64 * 100.0).min(100.0);
    let mut stmt = con.prepare(&format!(
        "SELECT imp_id, campaign_id, user_id, imp_ts, device FROM impressions USING SAMPLE {} PERCENT (system)",
        click_sample_pct
    ))?;
    let imp_refs: Vec<(i64, i32, i64, NaiveDateTime, String)> = stmt
        .query_map([], |row| {
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
    crate::generate_table_parallel(pool, "clicks", ncl, &pb, "Generating clicks...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let ref_idx = rng.gen_range(0..imp_refs.len());
        let (imp_id, camp_id, user_id, imp_ts, device) = &imp_refs[ref_idx];
        // Most clicks happen fast after the impression; a long tail happens later
        // (Exponential rather than flat uniform), clipped to a few hours.
        let click_delay: f64 = Exp::new(1.0 / 90.0).unwrap().sample(&mut rng);
        let click_delay = click_delay.min(3600.0 * 4.0);
        let click_ts = *imp_ts + Duration::seconds(click_delay.max(1.0) as i64);
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
    // Get samples for conversions (see click sampling above for why PERCENT (system) over ROWS).
    let conv_sample_pct = (ncv as f64 / ncl as f64 * 100.0).min(100.0);
    let mut stmt = con.prepare(&format!(
        "SELECT click_id, campaign_id, user_id, click_ts FROM clicks USING SAMPLE {} PERCENT (system)",
        conv_sample_pct
    ))?;
    let click_refs: Vec<(i64, i32, i64, NaiveDateTime)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    // 4. Conversions
    crate::generate_table_parallel(
        pool,
        "conversions",
        ncv,
        &pb,
        "Generating conversions...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let ref_idx = rng.gen_range(0..click_refs.len());
            let (click_id, camp_id, user_id, click_ts) = click_refs[ref_idx];
            // Conversions can lag by hours to days, unlike near-instant clicks.
            let lag_days = Gamma::new(2.0, 1.0).unwrap().sample(&mut rng);
            let conv_ts = click_ts + Duration::seconds((lag_days * 86400.0) as i64);
            let ctype = weighted_choice(&mut rng, &ctypes, &ctype_weights);
            // Revenue correlates with the campaign's target CPM tier.
            let cpm_target = campaign_facts[(camp_id as usize) - 1].1;
            let rev = round_to(
                lognormal_clamped(&mut rng, 15.0 + cpm_target * 3.0, 0.6, 1.0, 5000.0),
                2,
            );
            (i as i32, click_id, camp_id, user_id, conv_ts, ctype, rev)
        },
    )?;
    pb.finish_with_message("p02_adtech complete");

    Ok(())
}
