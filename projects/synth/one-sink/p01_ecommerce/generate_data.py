import pyarrow as pa
import duckdb, random, sys, os
from datetime import date, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
sf *= 50.0
NC = max(10, int(2000 * sf))
NCT = max(5, int(20 * sf))
NP = max(20, int(500 * sf))
NO = max(30, int(8000 * sf))
NI = max(50, int(24000 * sf))
NR = max(20, int(6000 * sf))

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")


def batched_insert(table_name, columns, rows):
    rows = list(rows)
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")


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

batched_insert("categories", ['category_id', 'name', 'parent_id', 'display_rank'], [
        (i, cats[(i - 1) % len(cats)], random.randint(1, i - 1) if i > 4 else None, i)
        for i in range(1, NCT + 1)
    ],
)
batched_insert("customers", ['customer_id', 'full_name', 'email', 'country', 'signup_date', 'is_active', 'lifetime_spend'], [
        (
            i,
            f"Cust {i}",
            f"u{i}@ex.com",
            random.choice(cn),
            base + timedelta(days=random.randint(0, 2000)),
            random.random() > 0.1,
            round(random.uniform(0, 15000), 2),
        )
        for i in range(1, NC + 1)
    ],
)
batched_insert("products", ['product_id', 'category_id', 'sku', 'name', 'price', 'cost', 'weight_kg', 'is_active', 'stock_qty'], [
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
        for i in range(1, NP + 1)
    ],
)
batched_insert("orders", ['order_id', 'customer_id', 'order_date', 'status', 'channel', 'discount_pct', 'shipping_cost'], [
        (
            i,
            random.randint(1, NC),
            base + timedelta(days=random.randint(0, 2000)),
            random.choice(st),
            random.choice(ch),
            round(random.uniform(0, 30), 2) if random.random() > 0.6 else 0,
            round(random.uniform(0, 25), 2),
        )
        for i in range(1, NO + 1)
    ],
)
batched_insert("order_items", ['item_id', 'order_id', 'product_id', 'quantity', 'unit_price'], [
        (
            i,
            random.randint(1, NO),
            random.randint(1, NP),
            random.randint(1, 5),
            round(random.uniform(5, 500), 2),
        )
        for i in range(1, NI + 1)
    ],
)
batched_insert("reviews", ['review_id', 'product_id', 'customer_id', 'rating', 'review_date', 'helpful_votes'], [
        (
            i,
            random.randint(1, NP),
            random.randint(1, NC),
            random.randint(1, 5),
            base + timedelta(days=random.randint(0, 2000)),
            random.randint(0, 200),
        )
        for i in range(1, NR + 1)
    ],
)
con.close()
print(f"p01 done: sf={sf} customers={NC} orders={NO} items={NI}")
