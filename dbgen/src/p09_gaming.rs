use crate::common::{pareto_weight_vec, round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let npl = (762872.0 * sf).max(20.0) as usize;
    let nlv = (19074.0 * sf).max(10.0) as usize;
    // Sessions/events/purchases are fan-out ratios off players/sessions.
    let avg_sessions_per_player = 5.0;
    let avg_events_per_session = 8.0;
    let avg_purchases_per_paid_player = 4.0;
    let nss = ((npl as f64) * avg_sessions_per_player).max(50.0) as usize;
    let nev = ((nss as f64) * avg_events_per_session).max(200.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(
        "DROP TABLE IF EXISTS purchases; DROP TABLE IF EXISTS events;
         DROP TABLE IF EXISTS sessions; DROP TABLE IF EXISTS levels;
         DROP TABLE IF EXISTS players;
         CREATE TABLE players(player_id INTEGER PRIMARY KEY, username VARCHAR,
             country VARCHAR, platform VARCHAR, created_ts TIMESTAMP,
             age_group VARCHAR, is_paid_user BOOLEAN);
         CREATE TABLE levels(level_id INTEGER PRIMARY KEY, level_name VARCHAR,
             world VARCHAR, difficulty VARCHAR, par_time_sec INTEGER,
             reward_coins INTEGER, unlock_level INTEGER);
         CREATE TABLE sessions(session_id BIGINT PRIMARY KEY, player_id INTEGER,
             session_start TIMESTAMP, session_end TIMESTAMP, platform VARCHAR,
             version VARCHAR, levels_attempted INTEGER, coins_earned INTEGER);
         CREATE TABLE events(event_id BIGINT PRIMARY KEY, session_id BIGINT,
             player_id INTEGER, event_type VARCHAR, event_ts TIMESTAMP,
             level_id INTEGER, value DOUBLE, metadata VARCHAR);
         CREATE TABLE purchases(purchase_id INTEGER PRIMARY KEY, player_id INTEGER,
             purchase_ts TIMESTAMP, item_type VARCHAR, item_name VARCHAR,
             price_usd DECIMAL(8,2), currency VARCHAR, is_refunded BOOLEAN);",
    )?;

    let base_ts = NaiveDate::from_ymd_opt(2023, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    // Fixed vocabularies.
    let countries = ["US", "BR", "IN", "UK", "DE", "JP", "KR", "FR", "CA", "MX", "AU", "RU", "IT"];
    let country_weights = [25.0, 10.0, 10.0, 8.0, 6.0, 6.0, 6.0, 6.0, 6.0, 5.0, 4.0, 4.0, 4.0];
    let platforms = ["android", "ios", "steam", "playstation", "xbox", "nintendo_switch"];
    let platform_weights = [50.0, 35.0, 10.0, 3.0, 1.5, 0.5];
    let age_groups = ["18_24", "25_34", "13_17", "35_44", "45_plus", "under_13"];
    let age_group_weights = [25.0, 28.0, 15.0, 18.0, 10.0, 4.0];
    let worlds = ["Forest", "Desert", "Ice Caverns", "Volcano", "Sky Kingdom", "Void"];
    let difficulties = ["easy", "medium", "hard", "expert"];
    let versions = ["2.4.0", "2.5.0", "2.6.1", "2.7.0"];
    let version_weights = [10.0, 20.0, 30.0, 40.0];
    let etypes = [
        "level_start", "level_complete", "item_collected", "level_fail", "tutorial_step",
        "achievement_unlocked", "purchase_prompt_shown", "session_start", "session_end",
    ];
    let etype_weights = [25.0, 18.0, 18.0, 15.0, 8.0, 7.0, 5.0, 2.0, 2.0];
    let itypes = ["coin_pack", "skin", "booster", "battle_pass", "character_unlock", "remove_ads"];
    let itype_weights = [35.0, 20.0, 18.0, 15.0, 8.0, 4.0];
    let currencies = ["USD", "EUR", "GBP", "BRL", "JPY", "other"];
    let currency_weights = [70.0, 12.0, 6.0, 5.0, 4.0, 3.0];

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Players
    crate::generate_table_parallel(pool, "players", npl, &pb, "Generating players...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let username = format!("Player_{}", i);
        let country = weighted_choice(&mut rng, &countries, &country_weights);
        let platform = weighted_choice(&mut rng, &platforms, &platform_weights);
        let ts = base_ts + Duration::seconds(rng.gen_range(0..200 * 86400));
        let age = weighted_choice(&mut rng, &age_groups, &age_group_weights);
        let paid = rng.gen_bool(0.4);
        (i as i32, username, country, platform, ts, age, paid)
    })?;

    // 2. Levels: contiguous blocks per world, difficulty trends harder with world index, and
    // unlock_level references a strictly earlier level.
    let n_worlds = worlds.len();
    crate::generate_table_parallel(pool, "levels", nlv, &pb, "Generating levels...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Level_{}", i);
        let world_idx = ((i - 1) * n_worlds / nlv.max(1)).min(n_worlds - 1);
        let world = worlds[world_idx];
        let base_diff = (world_idx * difficulties.len() / n_worlds).min(difficulties.len() - 1);
        let diff_idx = (base_diff as i64 + rng.gen_range(-1..2)).clamp(0, difficulties.len() as i64 - 1) as usize;
        let diff = difficulties[diff_idx];
        let par = rng.gen_range(60..601);
        let reward = rng.gen_range(10..501);
        let unlock = (i as i32 - rng.gen_range(1..5)).max(1);
        (i as i32, name, world, diff, par, reward, unlock)
    })?;

    // Materialize is_paid_user so session/purchase volume can correlate with it
    // instead of drawing player activity independently.
    let mut stmt = con.prepare("SELECT is_paid_user FROM players ORDER BY player_id")?;
    let player_paid: Vec<bool> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let base_player_weights = pareto_weight_vec(npl, 1.0, 181);
    let session_factors: Vec<f64> = base_player_weights
        .iter()
        .zip(player_paid.iter())
        .map(|(w, paid)| if *paid { w * 3.0 } else { *w })
        .collect();
    let session_popularity = PopularityWeights::from_factors(&session_factors);

    // 3. Sessions (strongly popularity-weighted toward paid/engaged players)
    crate::generate_table_parallel(pool, "sessions", nss, &pb, "Generating sessions...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let player_id = session_popularity.sample(&mut rng) as i32;
        let start = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
        let dur = rng.gen_range(60..7201);
        let end = start + Duration::seconds(dur);
        let platform = weighted_choice(&mut rng, &platforms, &platform_weights);
        let version = weighted_choice(&mut rng, &versions, &version_weights);
        let attempts = rng.gen_range(0..11);
        let coins = rng.gen_range(0..1001);
        (
            i as i64, player_id, start, end, platform, version, attempts, coins,
        )
    })?;

    // Get samples for events; sessions are already popularity-distributed across players, so
    // uniform sampling here still reflects player-level skew.
    let mut stmt = con.prepare(&format!(
        "SELECT session_id, player_id, session_start FROM sessions USING SAMPLE {} ROWS",
        nev
    ))?;
    let session_refs: Vec<(i64, i32, NaiveDateTime)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    // Early levels get seen far more often than late ones (drop-off funnel):
    // an unshuffled, monotonically decaying weight over level_id (not a rank shuffle, since
    // the funnel really does follow level order).
    let level_funnel_weights: Vec<f64> = (1..=nlv).map(|k| (k as f64).powf(-0.7)).collect();
    let level_funnel = PopularityWeights::from_factors(&level_funnel_weights);

    // 4. Events (materialized-parent sampled from real sessions)
    crate::generate_table_parallel(pool, "events", nev, &pb, "Generating events...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let ref_idx = rng.gen_range(0..session_refs.len());
        let (sess_id, player_id, sess_start) = session_refs[ref_idx];
        let etype = weighted_choice(&mut rng, &etypes, &etype_weights);
        let ts = sess_start + Duration::seconds(rng.gen_range(0..7201));
        let level_id = level_funnel.sample(&mut rng) as i32;
        let value = round_to(rng.gen_range(0.0..1000.0), 2);
        let meta = format!("meta_{}", i);
        (
            i as i64, sess_id, player_id, etype, ts, level_id, value, meta,
        )
    })?;

    // Purchases are restricted to is_paid_user=true players only [CHANGE from
    // dbgen] - free players never generate purchase rows.
    let paid_player_ids: Vec<usize> = player_paid
        .iter()
        .enumerate()
        .filter(|(_, paid)| **paid)
        .map(|(idx, _)| idx + 1)
        .collect();
    let paid_player_factors: Vec<f64> = paid_player_ids
        .iter()
        .map(|&id| base_player_weights[id - 1])
        .collect();
    let paid_player_popularity = PopularityWeights::from_factors(&paid_player_factors);
    let npu = ((paid_player_ids.len() as f64) * avg_purchases_per_paid_player).max(10.0) as usize;

    // 5. Purchases
    crate::generate_table_parallel(pool, "purchases", npu, &pb, "Generating purchases...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let pool_idx = paid_player_popularity.sample(&mut rng) - 1;
        let player_id = paid_player_ids[pool_idx] as i32;
        let ts = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
        let itype = weighted_choice(&mut rng, &itypes, &itype_weights);
        let name = format!("Item_{}", rng.gen_range(1..51));
        let price = round_to(rng.gen_range(0.99..99.99), 2);
        let curr = weighted_choice(&mut rng, &currencies, &currency_weights);
        let refunded = rng.gen_bool(0.03);
        (i as i32, player_id, ts, itype, name, price, curr, refunded)
    })?;

    pb.finish_with_message("p09_gaming complete");

    Ok(())
}
