import duckdb, random, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_categories_chunk(start, end, cats):
    return [
        (i, cats[(i - 1) % len(cats)], random.randint(1, i - 1) if i > 4 else None, i)
        for i in range(start, end)
    ]


def generate_customers_chunk(start, end, cn, base):
    return [
        (
            i,
            f"Cust {i}",
            f"u{i}@ex.com",
            random.choice(cn),
            base + timedelta(days=random.randint(0, 2000)),
            random.random() > 0.1,
            round(random.uniform(0, 15000), 2),
        )
        for i in range(start, end)
    ]


def generate_products_chunk(start, end, NCT, base):
    return [
        (
            i,
            random.randint(1, NCT),
            f"SKU-{i:06d}",
            f"Prod {i}",
            round((c := round(random.uniform(1, 400), 2)) * random.uniform(1.1, 4), 2),
            c,
            round(random.uniform(0.1, 20), 3),
            random.random() > 0.05,
            random.randint(0, 1000),
        )
        for i in range(start, end)
    ]


def generate_orders_chunk(start, end, NC, st, ch, base):
    return [
        (
            i,
            random.randint(1, NC),
            base + timedelta(days=random.randint(0, 2000)),
            random.choice(st),
            random.choice(ch),
            round(random.uniform(0, 30), 2) if random.random() > 0.6 else 0,
            round(random.uniform(0, 25), 2),
        )
        for i in range(start, end)
    ]


def generate_order_items_chunk(start, end, NO, NP):
    return [
        (
            i,
            random.randint(1, NO),
            random.randint(1, NP),
            random.randint(1, 5),
            round(random.uniform(5, 500), 2),
        )
        for i in range(start, end)
    ]


def generate_reviews_chunk(start, end, NP, NC, base):
    return [
        (
            i,
            random.randint(1, NP),
            random.randint(1, NC),
            random.randint(1, 5),
            base + timedelta(days=random.randint(0, 2000)),
            random.randint(0, 200),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 50.0
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

    cpu_count = os.cpu_count()
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "categories",
            ["category_id", "name", "parent_id", "display_rank"],
            run_parallel(executor, generate_categories_chunk, NCT, cats),
        )
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
        batched_insert(
            con,
            "order_items",
            ["item_id", "order_id", "product_id", "quantity", "unit_price"],
            run_parallel(executor, generate_order_items_chunk, NI, NO, NP),
        )
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
    print(f"p01 done: sf={sf} customers={NC} orders={NO} items={NI}")


if __name__ == "__main__":
    main()
