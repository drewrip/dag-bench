use chrono::{Duration, NaiveDate};
use duckdb::{Connection, DuckdbConnectionManager};
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let sf_adj = sf * 6000.0;
    let nsup = (100.0 * sf_adj).max(5.0) as usize;
    let nwh = (20.0 * sf_adj).max(3.0) as usize;
    let nsh = (5000.0 * sf_adj).max(20.0) as usize;
    let nin = (1000.0 * sf_adj).max(10.0) as usize;
    let npo = (2000.0 * sf_adj).max(10.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(
        "DROP TABLE IF EXISTS purchase_orders; DROP TABLE IF EXISTS inventory;
         DROP TABLE IF EXISTS shipments; DROP TABLE IF EXISTS warehouses;
         DROP TABLE IF EXISTS suppliers;
         CREATE TABLE suppliers(supplier_id INTEGER PRIMARY KEY, name VARCHAR,
             country VARCHAR, reliability_score DECIMAL(4,2), lead_time_days INTEGER,
             category VARCHAR, is_preferred BOOLEAN);
         CREATE TABLE warehouses(wh_id INTEGER PRIMARY KEY, name VARCHAR,
             country VARCHAR, region VARCHAR, capacity_m3 INTEGER, is_active BOOLEAN);
         CREATE TABLE shipments(shipment_id INTEGER PRIMARY KEY, supplier_id INTEGER,
             wh_id INTEGER, sku VARCHAR, quantity INTEGER, unit_cost DECIMAL(10,2),
             shipped_date DATE, received_date DATE, status VARCHAR,
             freight_cost DECIMAL(10,2));
         CREATE TABLE inventory(inv_id INTEGER PRIMARY KEY, wh_id INTEGER,
             sku VARCHAR, qty_on_hand INTEGER, qty_reserved INTEGER,
             reorder_point INTEGER, snapshot_date DATE);
         CREATE TABLE purchase_orders(po_id INTEGER PRIMARY KEY, supplier_id INTEGER,
             sku VARCHAR, ordered_qty INTEGER, unit_price DECIMAL(10,2),
             order_date DATE, expected_date DATE, received_qty INTEGER,
             status VARCHAR);",
    )?;

    let base_date = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap();
    let cats = [
        "raw_materials",
        "components",
        "packaging",
        "finished_goods",
        "consumables",
    ];
    let regions = ["NA", "EU", "APAC", "ME", "SA"];
    let statuses = ["delivered", "in_transit", "delayed", "cancelled", "lost"];
    let po_statuses = ["open", "partial", "complete", "cancelled"];
    let skus: Vec<String> = (1..=50).map(|i| format!("SKU-{:05}", i)).collect();
    let countries_sup = ["CN", "IN", "DE", "US", "MX", "BR"];
    let countries_wh = ["US", "DE", "SG", "BR", "AU"];

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Suppliers
    crate::generate_table_parallel(
        con,
        "suppliers",
        nsup,
        &pb,
        "Generating suppliers...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let name = format!("Supplier {}", i);
            let country = countries_sup[rng.gen_range(0..countries_sup.len())];
            let score = ((rng.gen_range(0.5..1.0) * 100.0) as f64).round() / 100.0;
            let lead_time = rng.gen_range(3..61);
            let cat = cats[rng.gen_range(0..cats.len())];
            let preferred = rng.gen_bool(0.5);
            (i as i32, name, country, score, lead_time, cat, preferred)
        },
    )?;

    // 2. Warehouses
    crate::generate_table_parallel(
        con,
        "warehouses",
        nwh,
        &pb,
        "Generating warehouses...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let name = format!("WH-{}", i);
            let country = countries_wh[rng.gen_range(0..countries_wh.len())];
            let region = regions[rng.gen_range(0..regions.len())];
            let cap = rng.gen_range(1000..50001);
            let active = rng.gen_bool(0.95);
            (i as i32, name, country, region, cap, active)
        },
    )?;

    // 3. Shipments
    crate::generate_table_parallel(con, "shipments", nsh, &pb, "Generating shipments...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let sup_id = rng.gen_range(1..=nsup) as i32;
        let wh_id = rng.gen_range(1..=nwh) as i32;
        let sku = &skus[rng.gen_range(0..skus.len())];
        let qty = rng.gen_range(10..10001);
        let cost = ((rng.gen_range(1.0..500.0) * 100.0) as f64).round() / 100.0;
        let shipped = base_date + Duration::days(rng.gen_range(0..1001));
        let received = shipped + Duration::days(rng.gen_range(3..46));
        let status = statuses[rng.gen_range(0..statuses.len())];
        let freight = ((rng.gen_range(50.0..5000.0) * 100.0) as f64).round() / 100.0;
        (
            i as i32,
            sup_id,
            wh_id,
            sku.clone(),
            qty,
            cost,
            shipped,
            received,
            status,
            freight,
        )
    })?;

    // 4. Inventory
    crate::generate_table_parallel(con, "inventory", nin, &pb, "Generating inventory...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let wh_id = rng.gen_range(1..=nwh) as i32;
        let sku = &skus[rng.gen_range(0..skus.len())];
        let on_hand = rng.gen_range(0..10001);
        let reserved = rng.gen_range(0..501);
        let reorder = rng.gen_range(100..1001);
        let snapshot = base_date + Duration::days(rng.gen_range(800..1001));
        (
            i as i32,
            wh_id,
            sku.clone(),
            on_hand,
            reserved,
            reorder,
            snapshot,
        )
    })?;

    // 5. Purchase Orders
    crate::generate_table_parallel(
        con,
        "purchase_orders",
        npo,
        &pb,
        "Generating purchase orders...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let sup_id = rng.gen_range(1..=nsup) as i32;
            let sku = &skus[rng.gen_range(0..skus.len())];
            let ordered = rng.gen_range(100..5001);
            let price = ((rng.gen_range(1.0..500.0) * 100.0) as f64).round() / 100.0;
            let order_date = base_date + Duration::days(rng.gen_range(0..901));
            let expected = order_date + Duration::days(rng.gen_range(7..61));
            let received = rng.gen_range(0..5001);
            let status = po_statuses[rng.gen_range(0..po_statuses.len())];
            (
                i as i32,
                sup_id,
                sku.clone(),
                ordered,
                price,
                order_date,
                expected,
                received,
                status,
            )
        },
    )?;

    pb.finish_with_message("p05_logistics complete");

    Ok(())
}
