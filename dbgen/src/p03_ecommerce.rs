use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>, no_constraints: bool) -> duckdb::Result<()> {
    let nc = (3255392.0 * sf).max(10.0) as usize;
    let nct = (32555.0 * sf).max(5.0) as usize;
    let np = (813848.0 * sf).max(20.0) as usize;
    // Orders/order_items/reviews are fan-out ratios off their parents.
    let avg_orders_per_customer = 4.0;
    let avg_items_per_order = 2.8;
    let review_rate = 0.225; // of eligible (completed) order_items
    let no = ((nc as f64) * avg_orders_per_customer).max(30.0) as usize;
    let ni = ((no as f64) * avg_items_per_order).max(50.0) as usize;
    let nmo = (1139387.0 * sf).max(10.0) as usize;

    let con = &pool.get().expect("couldn't get connection");
    con.execute_batch(&crate::common::schema_sql(
        "DROP TABLE IF EXISTS reviews; DROP TABLE IF EXISTS order_items;
         DROP TABLE IF EXISTS orders;  DROP TABLE IF EXISTS products;
         DROP TABLE IF EXISTS categories; DROP TABLE IF EXISTS customers;
         DROP TABLE IF EXISTS marketplace_orders;
         CREATE TABLE customers(customer_id INTEGER PRIMARY KEY, full_name VARCHAR,
             email VARCHAR, country VARCHAR, signup_date DATE, is_active BOOLEAN,
             lifetime_spend DECIMAL(20,2));
         CREATE TABLE categories(category_id INTEGER PRIMARY KEY, name VARCHAR,
             parent_id INTEGER, display_rank INTEGER);
         CREATE TABLE products(product_id INTEGER PRIMARY KEY, category_id INTEGER,
             sku VARCHAR, name VARCHAR, price DECIMAL(10,2), cost DECIMAL(10,2),
             weight_kg DECIMAL(6,3), is_active BOOLEAN, stock_qty INTEGER);
         CREATE TABLE orders(order_id INTEGER PRIMARY KEY, customer_id INTEGER,
             order_date DATE, status VARCHAR, channel VARCHAR,
             discount_pct DECIMAL(5,2), shipping_cost DECIMAL(8,2));
         CREATE TABLE order_items(item_id INTEGER PRIMARY KEY, order_id INTEGER,
             product_id INTEGER, quantity INTEGER, unit_price DECIMAL(10,2));
         CREATE TABLE reviews(review_id INTEGER PRIMARY KEY, product_id INTEGER,
             customer_id INTEGER, rating TINYINT, review_date DATE, helpful_votes INTEGER);
         CREATE TABLE marketplace_orders(external_order_id VARCHAR PRIMARY KEY,
             customer_id INTEGER, order_date DATE, marketplace_name VARCHAR,
             partner_status VARCHAR, gross_amount DECIMAL(10,2), commission_fee DECIMAL(8,2));",
        no_constraints,
    ))?;

    let base_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();

    // Fixed vocabularies.
    let countries = ["US", "CA", "GB", "DE", "FR", "AU", "JP", "BR", "IN", "MX", "IT", "ES", "NL", "SE", "KR"];
    let country_weights = [40.0, 8.0, 8.0, 6.0, 5.0, 5.0, 4.0, 4.0, 4.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0];
    let statuses = ["completed", "cancelled", "returned", "pending", "processing"];
    let status_weights = [75.0, 8.0, 7.0, 5.0, 5.0];
    let channels = ["web", "mobile_app", "phone", "in_store_kiosk"];
    let channel_weights = [55.0, 35.0, 5.0, 5.0];
    let marketplace_names = ["Amazon", "eBay", "Walmart Marketplace", "Etsy"];
    let marketplace_weights = [55.0, 20.0, 15.0, 10.0];
    let partner_statuses = ["shipped", "pending", "cancelled", "refunded"];
    let partner_status_weights = [70.0, 12.0, 10.0, 8.0];
    let cats_names = [
        "Electronics",
        "Home & Kitchen",
        "Apparel",
        "Beauty & Personal Care",
        "Sports & Outdoors",
        "Toys & Games",
        "Books",
        "Grocery",
    ];
    let qty_values = [1, 2, 3, 4, 5, 6, 7, 8];
    let qty_weights = [35.0, 25.0, 15.0, 10.0, 7.0, 4.0, 2.0, 2.0];

    // A few categories/products/customers/orders should dominate rather than
    // uniform draws.
    let category_popularity = PopularityWeights::new(nct, 1.0, 31);
    let customer_popularity = PopularityWeights::new(nc, 1.1, 42);
    let product_popularity = PopularityWeights::new(np, 1.0, 53);
    let order_popularity = PopularityWeights::new(no, 1.0, 64);

    let pb = ProgressBar::new(7);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Categories: shallow 2-level tree - first 8 are top-level, rest are children of one.
    let n_top_level = cats_names.len().min(nct);
    crate::generate_table_parallel(
        pool,
        "categories",
        nct,
        &pb,
        "Generating categories...",
        |i| {
            let name = cats_names[(i - 1) % cats_names.len()];
            let parent_id = if i > n_top_level {
                let mut rng = SmallRng::seed_from_u64(i as u64);
                Some(rng.gen_range(1..=n_top_level) as i32)
            } else {
                None
            };
            (i as i32, name, parent_id, i as i32)
        },
    )?;

    // 2. Customers (lifetime_spend starts at 0, rolled up from real orders below)
    crate::generate_table_parallel(pool, "customers", nc, &pb, "Generating customers...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let full_name = format!("Cust {}", i);
        let email = format!("u{}@ex.com", i);
        let country = weighted_choice(&mut rng, &countries, &country_weights);
        let signup_date = base_date + Duration::days(rng.gen_range(0..2001));
        let is_active = rng.gen_bool(0.9);
        (
            i as i32,
            full_name,
            email,
            country,
            signup_date,
            is_active,
            0.0_f64,
        )
    })?;

    // 3. Products
    crate::generate_table_parallel(pool, "products", np, &pb, "Generating products...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let cat_id = category_popularity.sample(&mut rng) as i32;
        let sku = format!("SKU-{:060}", i);
        let name = format!("Prod {}", i);
        let cost = round_to(rng.gen_range(1.0..400.0), 2);
        // Price is a markup over cost, not independently random.
        let price = round_to(cost * rng.gen_range(1.3..3.0), 2);
        let weight = round_to(rng.gen_range(0.1..20.0), 3);
        let is_active = rng.gen_bool(0.95);
        let stock_qty = rng.gen_range(0..=1000);
        (
            i as i32, cat_id, sku, name, price, cost, weight, is_active, stock_qty,
        )
    })?;

    // 4. Orders
    crate::generate_table_parallel(pool, "orders", no, &pb, "Generating orders...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let cust_id = customer_popularity.sample(&mut rng) as i32;
        let order_date = base_date + Duration::days(rng.gen_range(0..2001));
        let status = weighted_choice(&mut rng, &statuses, &status_weights);
        let channel = weighted_choice(&mut rng, &channels, &channel_weights);
        let discount = if rng.gen_bool(0.4) {
            round_to(rng.gen_range(0.0..30.0), 2)
        } else {
            0.0
        };
        let shipping_cost = round_to(rng.gen_range(0.0..25.0), 2);
        (
            i as i32,
            cust_id,
            order_date,
            status,
            channel,
            discount,
            shipping_cost,
        )
    })?;

    // 5. Order Items (every order gets >=1 item via a stratified minimum)
    crate::generate_table_parallel(
        pool,
        "order_items",
        ni,
        &pb,
        "Generating order items...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let order_id = if i <= no {
                i as i32
            } else {
                order_popularity.sample(&mut rng) as i32
            };
            let product_id = product_popularity.sample(&mut rng) as i32;
            let quantity = weighted_choice(&mut rng, &qty_values, &qty_weights);
            let unit_price = round_to(rng.gen_range(5.0..500.0), 2);
            (i as i32, order_id, product_id, quantity, unit_price)
        },
    )?;

    // Roll customers.lifetime_spend up from their real orders: never generate
    // two numbers that are supposed to reconcile independently.
    //
    // Kept as an `UPDATE` (unlike the claims rollup in p08) since this only touches one
    // numeric column on the smaller `customers` table - a CTAS rebuild here would have to
    // re-copy the wider `full_name`/`email` string columns for no benefit and measured
    // slower end-to-end.
    con.execute_batch(
        "UPDATE customers SET lifetime_spend = t.total
         FROM (
             SELECT o.customer_id AS cid, SUM(oi.quantity * oi.unit_price) AS total
             FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
             GROUP BY o.customer_id
         ) t
         WHERE customers.customer_id = t.cid;",
    )?;

    // 6. Reviews: filtered-subset sampled from completed orders' real order_items only
    // - you cannot review what you haven't received. Only "roughly" a 20-25% rate is
    // required, so a single-pass Bernoulli percentage sample (each
    // eligible item independently kept w.p. `review_rate`) is equivalent in distribution to
    // the old COUNT(*) + exact-ROWS-reservoir-sample, but needs one join scan instead of two.
    let mut stmt = con.prepare(&format!(
        "SELECT oi.product_id, o.customer_id FROM order_items oi
         JOIN orders o ON oi.order_id = o.order_id
         WHERE o.status = 'completed' USING SAMPLE {} PERCENT (bernoulli)",
        review_rate * 100.0
    ))?;
    let eligible_refs: Vec<(i32, i32)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let nr = eligible_refs.len().max(20);

    crate::generate_table_parallel(pool, "reviews", nr, &pb, "Generating reviews...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let ref_idx = rng.gen_range(0..eligible_refs.len());
        let (product_id, customer_id) = eligible_refs[ref_idx];
        let rating = rng.gen_range(1..6) as i8;
        let review_date = base_date + Duration::days(rng.gen_range(0..2001));
        let helpful_votes = rng.gen_range(0..201);
        (
            i as i32,
            product_id,
            customer_id,
            rating,
            review_date,
            helpful_votes,
        )
    })?;

    // 7. Marketplace orders (separate partner feed: own id space, status vocabulary, no line
    // items - a deliberate lower-fidelity external source).
    crate::generate_table_parallel(
        pool,
        "marketplace_orders",
        nmo,
        &pb,
        "Generating marketplace orders...",
        |i| {
            let mut rng = SmallRng::seed_from_u64((i as u64) ^ 0xA5A5_5A5A);
            let external_order_id = format!("MKT-{:010}", i);
            let cust_id = customer_popularity.sample(&mut rng) as i32;
            let order_date = base_date + Duration::days(rng.gen_range(0..2001));
            let marketplace_name = weighted_choice(&mut rng, &marketplace_names, &marketplace_weights);
            let partner_status = weighted_choice(&mut rng, &partner_statuses, &partner_status_weights);
            let gross_amount = round_to(rng.gen_range(10.0..600.0), 2);
            let commission_fee = round_to(gross_amount * rng.gen_range(0.08..0.20), 2);
            (
                external_order_id,
                cust_id,
                order_date,
                marketplace_name,
                partner_status,
                gross_amount,
                commission_fee,
            )
        },
    )?;

    pb.finish_with_message("p03_ecommerce complete");
    Ok(())
}
