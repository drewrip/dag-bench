import duckdb, sys, os
import numpy as np
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_categories_chunk(start, end, cats):
    rng = np.random.default_rng(start)
    size = end - start
    parent_rand = rng.integers(1, np.maximum(2, np.arange(start, end)), size)
    return [
        (i, cats[(i - 1) % len(cats)], int(parent_rand[i - start]) if i > 4 else None, i)
        for i in range(start, end)
    ]

def generate_customers_chunk(start, end, cn, base):
    rng = np.random.default_rng(start)
    size = end - start
    cn_idx = rng.integers(0, len(cn), size)
    days_rand = rng.integers(0, 2001, size)
    active_rand = rng.random(size)
    spend_rand = rng.uniform(0, 15000, size)
    return [
        (
            i,
            f"Cust {i}",
            f"u{i}@ex.com",
            cn[cn_idx[i - start]],
            base + timedelta(days=int(days_rand[i - start])),
            bool(active_rand[i - start] > 0.1),
            round(float(spend_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]

def generate_products_chunk(start, end, NCT, base):
    rng = np.random.default_rng(start)
    size = end - start
    cat_rand = rng.integers(1, NCT + 1, size)
    cost_rand = rng.uniform(1, 400, size)
    price_mult = rng.uniform(1.1, 4, size)
    weight_rand = rng.uniform(0.1, 20, size)
    active_rand = rng.random(size)
    stock_rand = rng.integers(0, 1001, size)
    return [
        (
            i,
            int(cat_rand[i - start]),
            f"SKU-{i:06d}",
            f"Prod {i}",
            round((c := round(float(cost_rand[i - start]), 2)) * float(price_mult[i - start]), 2),
            c,
            round(float(weight_rand[i - start]), 3),
            bool(active_rand[i - start] > 0.05),
            int(stock_rand[i - start]),
        )
        for i in range(start, end)
    ]

def generate_orders_chunk(start, end, NC, st, ch, base):
    rng = np.random.default_rng(start)
    size = end - start
    cust_rand = rng.integers(1, NC + 1, size)
    days_rand = rng.integers(0, 2001, size)
    st_idx = rng.integers(0, len(st), size)
    ch_idx = rng.integers(0, len(ch), size)
    disc_rand = rng.uniform(0, 30, size)
    disc_prob = rng.random(size)
    ship_rand = rng.uniform(0, 25, size)
    return [
        (
            i,
            int(cust_rand[i - start]),
            base + timedelta(days=int(days_rand[i - start])),
            st[st_idx[i - start]],
            ch[ch_idx[i - start]],
            round(float(disc_rand[i - start]), 2) if disc_prob[i - start] > 0.6 else 0,
            round(float(ship_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]

def generate_order_items_chunk(start, end, NO, NP):
    rng = np.random.default_rng(start)
    size = end - start
    ord_rand = rng.integers(1, NO + 1, size)
    prod_rand = rng.integers(1, NP + 1, size)
    qty_rand = rng.integers(1, 6, size)
    price_rand = rng.uniform(5, 500, size)
    return [
        (
            i,
            int(ord_rand[i - start]),
            int(prod_rand[i - start]),
            int(qty_rand[i - start]),
            round(float(price_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]

def generate_reviews_chunk(start, end, NP, NC, base):
    rng = np.random.default_rng(start)
    size = end - start
    prod_rand = rng.integers(1, NP + 1, size)
    cust_rand = rng.integers(1, NC + 1, size)
    rat_rand = rng.integers(1, 6, size)
    days_rand = rng.integers(0, 2001, size)
    votes_rand = rng.integers(0, 201, size)
    return [
        (
            i,
            int(prod_rand[i - start]),
            int(cust_rand[i - start]),
            int(rat_rand[i - start]),
            base + timedelta(days=int(days_rand[i - start])),
            int(votes_rand[i - start]),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 100.0
    NC, NCT, NP, NO, NI, NR = (
        max(a, int(b * sf_adj))
        for a, b in [(10, 2000), (5, 20), (20, 500), (30, 8000), (50, 24000), (20, 6000)]
    )

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS reviews; DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS orders; DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS categories; DROP TABLE IF EXISTS customers;
    CREATE TABLE customers(customer_id INTEGER PRIMARY KEY,full_name VARCHAR,
      email VARCHAR,country VARCHAR,signup_date DATE,is_active BOOLEAN,lifetime_spend DECIMAL(12,2));
    CREATE TABLE categories(category_id INTEGER PRIMARY KEY,name VARCHAR,parent_id INTEGER,display_rank INTEGER);
    CREATE TABLE products(product_id INTEGER PRIMARY KEY,category_id INTEGER,sku VARCHAR,
      name VARCHAR,price DECIMAL(10,2),cost DECIMAL(10,2),weight_kg DECIMAL(6,3),
      is_active BOOLEAN,stock_qty INTEGER);
    CREATE TABLE orders(order_id INTEGER PRIMARY KEY,customer_id INTEGER,order_date DATE,
      status VARCHAR,channel VARCHAR,discount_pct DECIMAL(5,2),shipping_cost DECIMAL(8,2));
    CREATE TABLE order_items(item_id INTEGER PRIMARY KEY,order_id INTEGER,product_id INTEGER,
      quantity INTEGER,unit_price DECIMAL(10,2));
    CREATE TABLE reviews(review_id INTEGER PRIMARY KEY,product_id INTEGER,customer_id INTEGER,
      rating TINYINT,review_date DATE,helpful_votes INTEGER);
    """)

    base = date(2018, 1, 1)
    cn = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "MX"]
    st = ["completed", "pending", "shipped", "cancelled", "refunded"]
    ch = ["web", "mobile", "in-store", "marketplace"]
    cats = [
        "Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Food", 
        "Garden", "Auto", "Health", "Office", "Jewelry", "Music", "Movies", "Games", 
        "Travel", "Pets", "Tools", "Baby",
    ]

    cpu_count = os.cpu_count()
    
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "categories", ['category_id', 'name', 'parent_id', 'display_rank'], 
                       run_parallel(executor, generate_categories_chunk, NCT, cats))
        batched_insert(con, "customers", ['customer_id', 'full_name', 'email', 'country', 'signup_date', 'is_active', 'lifetime_spend'], 
                       run_parallel(executor, generate_customers_chunk, NC, cn, base))
        batched_insert(con, "products", ['product_id', 'category_id', 'sku', 'name', 'price', 'cost', 'weight_kg', 'is_active', 'stock_qty'], 
                       run_parallel(executor, generate_products_chunk, NP, NCT, base))
        batched_insert(con, "orders", ['order_id', 'customer_id', 'order_date', 'status', 'channel', 'discount_pct', 'shipping_cost'], 
                       run_parallel(executor, generate_orders_chunk, NO, NC, st, ch, base))
        batched_insert(con, "order_items", ['item_id', 'order_id', 'product_id', 'quantity', 'unit_price'], 
                       run_parallel(executor, generate_order_items_chunk, NI, NO, NP))
        batched_insert(con, "reviews", ['review_id', 'product_id', 'customer_id', 'rating', 'review_date', 'helpful_votes'], 
                       run_parallel(executor, generate_reviews_chunk, NR, NP, NC, base))


    con.close()
    print(f"p01 done sf={sf} customers={NC} orders={NO} items={NI}")

if __name__ == "__main__":
    main()
