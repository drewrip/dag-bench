use chrono::{Duration, NaiveDate, NaiveDateTime};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 400.0;
    let npl = (2000.0 * sf_adj).max(20.0) as usize;
    let nss = (10000.0 * sf_adj).max(50.0) as usize;
    let nev = (80000.0 * sf_adj).max(200.0) as usize;
    let npu = (3000.0 * sf_adj).max(10.0) as usize;
    let nlv = (50.0 * sf_adj).max(10.0) as usize;

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
    let countries = ["US", "CN", "DE", "JP", "BR", "KR", "RU", "GB", "CA", "FR"];
    let platforms = [
        "PC",
        "Mobile_iOS",
        "Mobile_Android",
        "Console_PS",
        "Console_Xbox",
    ];
    let age_groups = ["<13", "13-17", "18-24", "25-34", "35-44", "45+"];
    let worlds = ["Forest", "Desert", "Ocean", "Space", "Underground", "Sky"];
    let difficulties = ["easy", "normal", "hard", "nightmare"];
    let etypes = [
        "level_start",
        "level_complete",
        "level_fail",
        "achievement",
        "item_pickup",
        "death",
        "checkpoint",
        "boss_kill",
    ];
    let itypes = [
        "coin_pack",
        "skin",
        "level_skip",
        "power_up",
        "subscription",
        "loot_box",
    ];
    let currencies = ["USD", "EUR", "GBP", "JPY", "BRL"];

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Players
    crate::generate_table_parallel(con, "players", npl, &pb, "Generating players...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let username = format!("Player_{}", i);
        let country = countries[rng.gen_range(0..countries.len())];
        let platform = platforms[rng.gen_range(0..platforms.len())];
        let ts = base_ts + Duration::seconds(rng.gen_range(0..200 * 86400));
        let age = age_groups[rng.gen_range(0..age_groups.len())];
        let paid = rng.gen_bool(0.4);
        (i as i32, username, country, platform, ts, age, paid)
    })?;

    // 2. Levels
    crate::generate_table_parallel(con, "levels", nlv, &pb, "Generating levels...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let name = format!("Level_{}", i);
        let world = worlds[rng.gen_range(0..worlds.len())];
        let diff = difficulties[rng.gen_range(0..difficulties.len())];
        let par = rng.gen_range(60..601);
        let reward = rng.gen_range(10..501);
        let unlock = (i as i32 - rng.gen_range(0..4)).max(1);
        (i as i32, name, world, diff, par, reward, unlock)
    })?;

    // 3. Sessions
    crate::generate_table_parallel(con, "sessions", nss, &pb, "Generating sessions...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let player_id = rng.gen_range(1..=npl) as i32;
        let start = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
        let dur = rng.gen_range(60..7201);
        let end = start + Duration::seconds(dur);
        let platform = platforms[rng.gen_range(0..platforms.len())];
        let version = format!("v{}.{}", rng.gen_range(1..4), rng.gen_range(0..10));
        let attempts = rng.gen_range(0..11);
        let coins = rng.gen_range(0..1001);
        (
            i as i64, player_id, start, end, platform, version, attempts, coins,
        )
    })?;

    // Get samples for events
    let mut stmt = con
        .prepare("SELECT session_id, player_id, session_start FROM sessions USING SAMPLE ? ROWS")?;
    let session_refs: Vec<(i64, i32, NaiveDateTime)> = stmt
        .query_map([nev], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    // 4. Events
    crate::generate_table_parallel(con, "events", nev, &pb, "Generating events...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let ref_idx = rng.gen_range(0..session_refs.len());
        let (sess_id, player_id, sess_start) = session_refs[ref_idx];
        let etype = etypes[rng.gen_range(0..etypes.len())];
        let ts = sess_start + Duration::seconds(rng.gen_range(0..7201));
        let level_id = rng.gen_range(1..=nlv) as i32;
        let value = ((rng.gen_range(0.0..1000.0) * 100.0) as f64).round() / 100.0;
        let meta = format!("meta_{}", i);
        (
            i as i64, sess_id, player_id, etype, ts, level_id, value, meta,
        )
    })?;

    // 5. Purchases
    crate::generate_table_parallel(con, "purchases", npu, &pb, "Generating purchases...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let player_id = rng.gen_range(1..=npl) as i32;
        let ts = base_ts + Duration::seconds(rng.gen_range(0..300 * 86400));
        let itype = itypes[rng.gen_range(0..itypes.len())];
        let name = format!("Item_{}", rng.gen_range(1..51));
        let price = ((rng.gen_range(0.99..99.99) * 100.0) as f64).round() / 100.0;
        let curr = currencies[rng.gen_range(0..currencies.len())];
        let refunded = rng.gen_bool(0.03);
        (i as i32, player_id, ts, itype, name, price, curr, refunded)
    })?;

    pb.finish_with_message("p09_gaming complete");

    Ok(())
}
