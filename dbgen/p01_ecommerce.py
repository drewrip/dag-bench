import duckdb, sys, os, numpy as np
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_categories_chunk(start, end, cats):
    size = end - start
    rng = np.random.default_rng(start)
    ids = np.arange(start, end)
    # parent_id needs to be less than i.
    # Use 2 as a safe minimum high value for rng.integers to avoid errors when i <= 1.
    p_ids_all = rng.integers(1, np.maximum(ids, 2))
    
    rows = []
    for idx, i in enumerate(ids):
        p_id = int(p_ids_all[idx]) if i > 4 else None
        rows.append((int(i), cats[(i - 1) % len(cats)], p_id, int(i)))
    return rows


def generate_customers_chunk(start, end, cn, base):
    size = end - start
    rng = np.random.default_rng(start)
    country_indices = rng.integers(0, len(cn), size)
    days_offset = rng.integers(0, 2001, size)
    active_probs = rng.random(size)
    spendings = rng.uniform(0, 15000, size)
    
    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Cust {i}",
            f"u{i}@ex.com",
            cn[country_indices[idx]],
            base + timedelta(days=int(days_offset[idx])),
            bool(active_probs[idx] > 0.1),
            round(float(spendings[idx]), 2),
        ))
    return rows


def generate_products_chunk(start, end, NCT, base):
    size = end - start
    rng = np.random.default_rng(start)
    cat_ids = rng.integers(1, NCT + 1, size)
    costs = rng.uniform(1, 400, size)
    price_multipliers = rng.uniform(1.1, 4, size)
    weights = rng.uniform(0.1, 20, size)
    active_probs = rng.random(size)
    stock_qtys = rng.integers(0, 1001, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        cost = round(float(costs[idx]), 2)
        price = round(cost * float(price_multipliers[idx]), 2)
        rows.append((
            i,
            int(cat_ids[idx]),
            f"SKU-{i:06d}",
            f"Prod {i}",
            price,
            cost,
            round(float(weights[idx]), 3),
            bool(active_probs[idx] > 0.05),
            int(stock_qtys[idx]),
        ))
    return rows


def generate_orders_chunk(start, end, NC, st, ch, base):
    size = end - start
    rng = np.random.default_rng(start)
    cust_ids = rng.integers(1, NC + 1, size)
    days_offset = rng.integers(0, 2001, size)
    status_indices = rng.integers(0, len(st), size)
    channel_indices = rng.integers(0, len(ch), size)
    discount_probs = rng.random(size)
    discounts = rng.uniform(0, 30, size)
    shipping_costs = rng.uniform(0, 25, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        discount = round(float(discounts[idx]), 2) if discount_probs[idx] > 0.6 else 0.0
        rows.append((
            i,
            int(cust_ids[idx]),
            base + timedelta(days=int(days_offset[idx])),
            st[status_indices[idx]],
            ch[channel_indices[idx]],
            discount,
            round(float(shipping_costs[idx]), 2),
        ))
    return rows


def generate_order_items_chunk(start, end, NO, NP):
    size = end - start
    rng = np.random.default_rng(start)
    order_ids = rng.integers(1, NO + 1, size)
    product_ids = rng.integers(1, NP + 1, size)
    quantities = rng.integers(1, 6, size)
    unit_prices = rng.uniform(5, 500, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(order_ids[idx]),
            int(product_ids[idx]),
            int(quantities[idx]),
            round(float(unit_prices[idx]), 2),
        ))
    return rows


def generate_reviews_chunk(start, end, NP, NC, base):
    size = end - start
    rng = np.random.default_rng(start)
    product_ids = rng.integers(1, NP + 1, size)
    customer_ids = rng.integers(1, NC + 1, size)
    ratings = rng.integers(1, 6, size)
    days_offset = rng.integers(0, 2001, size)
    votes = rng.integers(0, 201, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(product_ids[idx]),
            int(customer_ids[idx]),
            int(ratings[idx]),
            base + timedelta(days=int(days_offset[idx])),
            int(votes[idx]),
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1550.0
    NC = max(10, int(2000 * sf_adj))
    NCT = max(5, int(20 * sf_adj))
    NP = max(20, int(500 * sf_adj))
    NO = max(30, int(8000 * sf_adj))
    NI = max(50, int(24000 * sf_adj))
    NR = max(20, int(6000 * sf_adj))

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS reviews; DROP TABLE IF EXISTS order_items;
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
        customer_id INTEGER, rating TINYINT, review_date DATE, helpful_votes INTEGER);
    """)

    base = date(2018, 1, 1)
    cn = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "MX"]
    st = ["completed", "pending", "shipped", "cancelled", "refunded"]
    ch = ["web", "mobile", "in-store", "marketplace"]
    cats = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports",
        "Beauty",
        "Toys",
        "Food",
        "Garden",
        "Automotive",
        "Health",
        "Office",
        "Jewelry",
        "Music",
        "Movies",
        "Games",
        "Travel",
        "Pets",
        "Tools",
        "Baby",
    ]

    cpu_count = get_worker_count()
    progress = GenerationProgress("p01_ecommerce", 6)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("categories")
        batched_insert(
            con,
            "categories",
            ["category_id", "name", "parent_id", "display_rank"],
            run_parallel(executor, generate_categories_chunk, NCT, cats),
        )
        progress.advance("customers")
        batched_insert(
            con,
            "customers",
            [
                "customer_id",
                "full_name",
                "email",
                "country",
                "signup_date",
                "is_active",
                "lifetime_spend",
            ],
            run_parallel(executor, generate_customers_chunk, NC, cn, base),
        )
        progress.advance("products")
        batched_insert(
            con,
            "products",
            [
                "product_id",
                "category_id",
                "sku",
                "name",
                "price",
                "cost",
                "weight_kg",
                "is_active",
                "stock_qty",
            ],
            run_parallel(executor, generate_products_chunk, NP, NCT, base),
        )
        progress.advance("orders")
        batched_insert(
            con,
            "orders",
            [
                "order_id",
                "customer_id",
                "order_date",
                "status",
                "channel",
                "discount_pct",
                "shipping_cost",
            ],
            run_parallel(executor, generate_orders_chunk, NO, NC, st, ch, base),
        )
        progress.advance("order_items")
        batched_insert(
            con,
            "order_items",
            ["item_id", "order_id", "product_id", "quantity", "unit_price"],
            run_parallel(executor, generate_order_items_chunk, NI, NO, NP),
        )
        progress.advance("reviews")
        batched_insert(
            con,
            "reviews",
            [
                "review_id",
                "product_id",
                "customer_id",
                "rating",
                "review_date",
                "helpful_votes",
            ],
            run_parallel(executor, generate_reviews_chunk, NR, NP, NC, base),
        )

    con.close()
    print_generation_summary(
        "p01_ecommerce",
        sf,
        {
            "categories": NCT,
            "customers": NC,
            "products": NP,
            "orders": NO,
            "order_items": NI,
            "reviews": NR,
        },
    )


if __name__ == "__main__":
    main()
