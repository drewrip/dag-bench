use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 1550.0;
    let nc = (2000.0 * sf_adj).max(10.0) as usize;
    let nct = (20.0 * sf_adj).max(5.0) as usize;
    let np = (500.0 * sf_adj).max(20.0) as usize;
    let no = (8000.0 * sf_adj).max(30.0) as usize;
    let ni = (24000.0 * sf_adj).max(50.0) as usize;
    let nr = (6000.0 * sf_adj).max(20.0) as usize;

    con.execute_batch(
        "DROP TABLE IF EXISTS reviews; DROP TABLE IF EXISTS order_items;
         DROP TABLE IF EXISTS orders;  DROP TABLE IF EXISTS products;
         DROP TABLE IF EXISTS categories; DROP TABLE IF EXISTS customers;
         CREATE TABLE customers(customer_id INTEGER PRIMARY KEY, full_name VARCHAR,
             email VARCHAR, country VARCHAR, signup_date DATE, is_active BOOLEAN,
             lifetime_spend DECIMAL(12,2));
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
             customer_id INTEGER, rating TINYINT, review_date DATE, helpful_votes INTEGER);",
    )?;

    let base_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
    let countries = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "MX"];
    let statuses = ["completed", "pending", "shipped", "cancelled", "refunded"];
    let channels = ["web", "mobile", "in-store", "marketplace"];
    let cats_names = [
        "Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Food",
        "Garden", "Automotive", "Health", "Office", "Jewelry", "Music", "Movies", "Games",
        "Travel", "Pets", "Tools", "Baby",
    ];

    let pb = ProgressBar::new(6);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Categories
    crate::generate_table_sequential(con, "categories", nct, &pb, "Generating categories...", |i| {
        let name = cats_names[(i - 1) % cats_names.len()];
        let parent_id = if i > 4 {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            Some(rng.gen_range(1..i))
        } else {
            None
        };
        (i as i32, name, parent_id, i as i32)
    })?;

    // 2. Customers
    crate::generate_table_parallel(con, "customers", nc, &pb, "Generating customers...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let full_name = format!("Cust {}", i);
        let email = format!("u{}@ex.com", i);
        let country = countries[rng.gen_range(0..countries.len())];
        let signup_date = base_date + Duration::days(rng.gen_range(0..2001));
        let is_active = rng.gen_bool(0.9);
        let lifetime_spend = ((rng.gen_range(0.0..15000.0) * 100.0) as f64).round() / 100.0;
        (i as i32, full_name, email, country, signup_date, is_active, lifetime_spend)
    })?;

    // 3. Products
    crate::generate_table_parallel(con, "products", np, &pb, "Generating products...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let cat_id = rng.gen_range(1..=nct) as i32;
        let sku = format!("SKU-{:060}", i);
        let name = format!("Prod {}", i);
        let cost = ((rng.gen_range(1.0..400.0) * 100.0) as f64).round() / 100.0;
        let price = ((cost * rng.gen_range(1.1..4.0) * 100.0) as f64).round() / 100.0;
        let weight = ((rng.gen_range(0.1..20.0) * 1000.0) as f64).round() / 1000.0;
        let is_active = rng.gen_bool(0.95);
        let stock_qty = rng.gen_range(0..=1000);
        (i as i32, cat_id, sku, name, price, cost, weight, is_active, stock_qty)
    })?;

    // 4. Orders
    crate::generate_table_parallel(con, "orders", no, &pb, "Generating orders...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let cust_id = rng.gen_range(1..=nc) as i32;
        let order_date = base_date + Duration::days(rng.gen_range(0..2001));
        let status = statuses[rng.gen_range(0..statuses.len())];
        let channel = channels[rng.gen_range(0..channels.len())];
        let discount = if rng.gen_bool(0.4) {
            ((rng.gen_range(0.0..30.0) * 100.0) as f64).round() / 100.0
        } else {
            0.0
        };
        let shipping_cost = ((rng.gen_range(0.0..25.0) * 100.0) as f64).round() / 100.0;
        (i as i32, cust_id, order_date, status, channel, discount, shipping_cost)
    })?;

    // 5. Order Items
    crate::generate_table_parallel(con, "order_items", ni, &pb, "Generating order items...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let order_id = rng.gen_range(1..=no) as i32;
        let product_id = rng.gen_range(1..=np) as i32;
        let quantity = rng.gen_range(1..6);
        let unit_price = ((rng.gen_range(5.0..500.0) * 100.0) as f64).round() / 100.0;
        (i as i32, order_id, product_id, quantity, unit_price)
    })?;

    // 6. Reviews
    crate::generate_table_parallel(con, "reviews", nr, &pb, "Generating reviews...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let product_id = rng.gen_range(1..=np) as i32;
        let customer_id = rng.gen_range(1..=nc) as i32;
        let rating = rng.gen_range(1..6) as i8;
        let review_date = base_date + Duration::days(rng.gen_range(0..2001));
        let helpful_votes = rng.gen_range(0..201);
        (i as i32, product_id, customer_id, rating, review_date, helpful_votes)
    })?;

    pb.finish_with_message("p01_ecommerce complete");
    Ok(())
}
