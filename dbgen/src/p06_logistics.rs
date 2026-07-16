use crate::common::{pareto_weight_vec, round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let nsup = (267835.0 * sf).max(5.0) as usize;
    let nwh = (1034.0 * sf).max(3.0) as usize;
    // SPEC.md §2.6 [CHANGE from dbgen]: SKU catalog scales with sf like every other
    // dimension, instead of a fixed 50-value pool.
    let n_sku = (10352.0 * sf).max(50.0) as usize;
    // SPEC.md §2.6: shipments/purchase_orders are fan-out ratios off suppliers; inventory is
    // one row per (warehouse, SKU) per snapshot.
    let avg_shipments_per_supplier = 50.0;
    let avg_po_per_supplier = 20.0;
    let n_snapshots = 3usize;
    let nsh = ((nsup as f64) * avg_shipments_per_supplier).max(20.0) as usize;
    let npo = ((nsup as f64) * avg_po_per_supplier).max(10.0) as usize;
    let nin = nwh * n_sku * n_snapshots;

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

    // SPEC.md §3.6 fixed vocabularies.
    let supplier_categories = [
        "electronics_components", "packaging", "raw_materials", "hardware", "textiles",
        "food_ingredients",
    ];
    let supplier_category_weights = [25.0, 20.0, 18.0, 15.0, 12.0, 10.0];
    let countries_sup = ["CN", "US", "VN", "MX", "DE", "IN", "BR", "ID", "PL", "TH", "KR", "IT"];
    let countries_sup_weights = [25.0, 15.0, 10.0, 10.0, 8.0, 8.0, 6.0, 5.0, 4.0, 4.0, 3.0, 2.0];
    let warehouse_regions = ["north_america", "europe", "apac", "latin_america"];
    let warehouse_region_weights = [35.0, 25.0, 30.0, 10.0];
    let countries_wh = ["US", "DE", "SG", "BR", "AU"];
    let shipment_statuses = ["delivered", "in_transit", "delayed", "customs_hold", "cancelled"];
    let shipment_status_weights = [70.0, 15.0, 8.0, 4.0, 3.0];
    let po_statuses = ["received", "approved", "partially_received", "submitted", "draft", "cancelled"];
    let po_status_weights = [55.0, 15.0, 12.0, 10.0, 5.0, 3.0];

    let skus: Vec<String> = (1..=n_sku).map(|i| format!("SKU-{:05}", i)).collect();
    let sku_base_cost: Vec<f64> = (0..n_sku)
        .map(|i| {
            let mut r = SmallRng::seed_from_u64((i as u64) + 9_000);
            round_to(r.gen_range(1.0..500.0), 2)
        })
        .collect();

    // SPEC.md §1.3a: shipments/POs skew toward a handful of high-volume suppliers/SKUs;
    // warehouses skew mildly by size.
    let sku_popularity = PopularityWeights::new(n_sku, 1.0, 121);
    let warehouse_popularity = PopularityWeights::new(nwh, 0.8, 131);

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
            let country = weighted_choice(&mut rng, &countries_sup, &countries_sup_weights);
            let score = round_to(rng.gen_range(0.5..1.0), 2);
            let lead_time = rng.gen_range(3..61);
            let cat = weighted_choice(&mut rng, &supplier_categories, &supplier_category_weights);
            let preferred = rng.gen_bool(0.5);
            (i as i32, name, country, score, lead_time, cat, preferred)
        },
    )?;

    // Materialize is_preferred/lead_time so shipment volume/dates can correlate with them
    // (SPEC.md §2.6) rather than being independent.
    let mut stmt = con.prepare("SELECT is_preferred, lead_time_days FROM suppliers ORDER BY supplier_id")?;
    let supplier_facts: Vec<(bool, i32)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let base_supplier_weights = pareto_weight_vec(nsup, 1.0, 141);
    let supplier_factors: Vec<f64> = base_supplier_weights
        .iter()
        .zip(supplier_facts.iter())
        .map(|(w, (preferred, _))| if *preferred { w * 2.5 } else { *w })
        .collect();
    let supplier_popularity = PopularityWeights::from_factors(&supplier_factors);

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
            let region = weighted_choice(&mut rng, &warehouse_regions, &warehouse_region_weights);
            let cap = rng.gen_range(1000..50001);
            let active = rng.gen_bool(0.95);
            (i as i32, name, country, region, cap, active)
        },
    )?;

    // 3. Shipments (received_date derived from the owning supplier's real lead_time, SPEC §1.7)
    crate::generate_table_parallel(con, "shipments", nsh, &pb, "Generating shipments...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let sup_id = supplier_popularity.sample(&mut rng);
        let wh_id = warehouse_popularity.sample(&mut rng) as i32;
        let sku_idx = sku_popularity.sample(&mut rng) - 1;
        let sku = &skus[sku_idx];
        let qty = rng.gen_range(10..10001);
        let cost = round_to(sku_base_cost[sku_idx] * rng.gen_range(0.9..1.1), 2);
        let shipped = base_date + Duration::days(rng.gen_range(0..1001));
        let lead_time_days = supplier_facts[sup_id - 1].1;
        let received = shipped + Duration::days((lead_time_days + rng.gen_range(-2..3)).max(1) as i64);
        let status = weighted_choice(&mut rng, &shipment_statuses, &shipment_status_weights);
        let freight = round_to(rng.gen_range(50.0..5000.0), 2);
        (
            i as i32,
            sup_id as i32,
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

    // 4. Inventory: one row per (warehouse, SKU, snapshot) for full coverage (SPEC.md §2.6)
    // rather than an unrelated flat row count, so joins against shipments/POs by SKU aren't
    // sparse.
    crate::generate_table_parallel(con, "inventory", nin, &pb, "Generating inventory...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let idx = i - 1;
        let snapshot_idx = idx % n_snapshots;
        let tmp = idx / n_snapshots;
        let sku_idx = tmp % n_sku;
        let wh_idx = tmp / n_sku;
        let wh_id = (wh_idx + 1) as i32;
        let sku = &skus[sku_idx];
        let on_hand = rng.gen_range(0..10001);
        let reserved = rng.gen_range(0..501);
        let reorder = rng.gen_range(100..1001);
        let snapshot = base_date + Duration::days(700 + (snapshot_idx as i64) * 120);
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

    // 5. Purchase Orders (received_qty close to ordered_qty with a small chance of a real
    // shortfall, SPEC.md §2.6, rather than fully independent of ordered_qty)
    crate::generate_table_parallel(
        con,
        "purchase_orders",
        npo,
        &pb,
        "Generating purchase orders...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let sup_id = supplier_popularity.sample(&mut rng) as i32;
            let sku_idx = sku_popularity.sample(&mut rng) - 1;
            let sku = &skus[sku_idx];
            let ordered = rng.gen_range(100..5001);
            let price = round_to(sku_base_cost[sku_idx] * rng.gen_range(0.95..1.15), 2);
            let order_date = base_date + Duration::days(rng.gen_range(0..901));
            let expected = order_date + Duration::days(rng.gen_range(7..61));
            let fulfillment_fraction = if rng.gen_bool(0.85) {
                rng.gen_range(0.95..1.0)
            } else {
                rng.gen_range(0.3..0.9)
            };
            let received = ((ordered as f64) * fulfillment_fraction).round() as i32;
            let status = weighted_choice(&mut rng, &po_statuses, &po_status_weights);
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

    pb.finish_with_message("p06_logistics complete");

    Ok(())
}
