import csv, duckdb, random, sys, os, tempfile
from datetime import date, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NC, NCT, NP, NO, NI, NR = (
    max(a, int(b * sf))
    for a, b in [(10, 2000), (5, 20), (20, 500), (30, 8000), (50, 24000), (20, 6000)]
)
os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")

def batched_insert(sql, rows):
    rows = list(rows)
    if not rows:
        return
    table_name = sql.split()[2]
    with tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False) as tmp:
        csv.writer(tmp).writerows(rows)
        temp_path = tmp.name
    try:
        con.execute(f"COPY {table_name} FROM '{temp_path}' (FORMAT CSV)")
    finally:
        os.unlink(temp_path)

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
con.execute("BEGIN")
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
    "Auto",
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
batched_insert(
    "INSERT INTO categories VALUES(?,?,?,?)",
    [
        (i, cats[(i - 1) % len(cats)], random.randint(1, i - 1) if i > 4 else None, i)
        for i in range(1, NCT + 1)
    ],
)
batched_insert(
    "INSERT INTO customers VALUES(?,?,?,?,?,?,?)",
    [
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
batched_insert(
    "INSERT INTO products VALUES(?,?,?,?,?,?,?,?,?)",
    [
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
batched_insert(
    "INSERT INTO orders VALUES(?,?,?,?,?,?,?)",
    [
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
batched_insert(
    "INSERT INTO order_items VALUES(?,?,?,?,?)",
    [
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
batched_insert(
    "INSERT INTO reviews VALUES(?,?,?,?,?,?)",
    [
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
con.commit()
con.close()
print(f"p01 done sf={sf} customers={NC} orders={NO} items={NI}")
