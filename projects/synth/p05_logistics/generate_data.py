import duckdb, random, sys, os
from datetime import date, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
NSUP = max(5, int(100 * sf))
NWH = max(3, int(20 * sf))
NSH = max(20, int(5000 * sf))
NIN = max(10, int(1000 * sf))
NPO = max(10, int(2000 * sf))

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")
con.execute("""
DROP TABLE IF EXISTS purchase_orders; DROP TABLE IF EXISTS inventory;
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
    status VARCHAR);
""")

base = date(2021, 1, 1)
cats = ["raw_materials", "components", "packaging", "finished_goods", "consumables"]
regions = ["NA", "EU", "APAC", "ME", "SA"]
statuses = ["delivered", "in_transit", "delayed", "cancelled", "lost"]
po_statuses = ["open", "partial", "complete", "cancelled"]
skus = [f"SKU-{i:05d}" for i in range(1, 51)]

con.executemany(
    "INSERT INTO suppliers VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            f"Supplier {i}",
            random.choice(["CN", "IN", "DE", "US", "MX", "BR"]),
            round(random.uniform(0.5, 1.0), 2),
            random.randint(3, 60),
            random.choice(cats),
            random.random() > 0.5,
        )
        for i in range(1, NSUP + 1)
    ],
)
con.executemany(
    "INSERT INTO warehouses VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            f"WH-{i}",
            random.choice(["US", "DE", "SG", "BR", "AU"]),
            random.choice(regions),
            random.randint(1000, 50000),
            random.random() > 0.05,
        )
        for i in range(1, NWH + 1)
    ],
)
con.executemany(
    "INSERT INTO shipments VALUES(?,?,?,?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NSUP),
            random.randint(1, NWH),
            random.choice(skus),
            random.randint(10, 10000),
            round(random.uniform(1, 500), 2),
            sd := base + timedelta(days=random.randint(0, 1000)),
            sd + timedelta(days=random.randint(3, 45)),
            random.choice(statuses),
            round(random.uniform(50, 5000), 2),
        )
        for i in range(1, NSH + 1)
    ],
)
con.executemany(
    "INSERT INTO inventory VALUES(?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NWH),
            random.choice(skus),
            random.randint(0, 10000),
            random.randint(0, 500),
            random.randint(100, 1000),
            base + timedelta(days=random.randint(800, 1000)),
        )
        for i in range(1, NIN + 1)
    ],
)
con.executemany(
    "INSERT INTO purchase_orders VALUES(?,?,?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NSUP),
            random.choice(skus),
            random.randint(100, 5000),
            round(random.uniform(1, 500), 2),
            od := base + timedelta(days=random.randint(0, 900)),
            od + timedelta(days=random.randint(7, 60)),
            random.randint(0, 5000),
            random.choice(po_statuses),
        )
        for i in range(1, NPO + 1)
    ],
)
con.close()
print(f"p05 done: suppliers={NSUP} shipments={NSH} pos={NPO}")
