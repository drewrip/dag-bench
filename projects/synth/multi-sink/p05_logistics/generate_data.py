import pyarrow as pa
import duckdb, random, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor

def generate_suppliers_chunk(start, end, cats):
    return [
        (
            i,
            f"Supplier {i}",
            random.choice(["CN", "IN", "DE", "US", "MX"]),
            round(random.uniform(0.5, 1), 2),
            random.randint(3, 60),
            random.choice(cats),
            random.random() > 0.5,
        )
        for i in range(start, end)
    ]

def generate_warehouses_chunk(start, end, regions):
    return [
        (
            i,
            f"WH-{i}",
            random.choice(["US", "DE", "SG", "BR", "AU"]),
            random.choice(regions),
            random.randint(1000, 50000),
            random.random() > 0.05,
        )
        for i in range(start, end)
    ]

def generate_shipments_chunk(start, end, NSUP, NWH, skus, base, statuses):
    return [
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
        for i in range(start, end)
    ]

def generate_inventory_chunk(start, end, NWH, skus, base):
    return [
        (
            i,
            random.randint(1, NWH),
            random.choice(skus),
            random.randint(0, 10000),
            random.randint(0, 500),
            random.randint(100, 1000),
            base + timedelta(days=random.randint(800, 1000)),
        )
        for i in range(start, end)
    ]

def generate_purchase_orders_chunk(start, end, NSUP, skus, base, pos):
    return [
        (
            i,
            random.randint(1, NSUP),
            random.choice(skus),
            random.randint(100, 5000),
            round(random.uniform(1, 500), 2),
            od := base + timedelta(days=random.randint(0, 900)),
            od + timedelta(days=random.randint(7, 60)),
            random.randint(0, 5000),
            random.choice(pos),
        )
        for i in range(start, end)
    ]

def batched_insert(con, table_name, columns, rows):
    if not rows:
        return
    arrow_table = pa.Table.from_arrays([pa.array(c) for c in zip(*rows)], names=columns)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")

def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf *= 100
    NSUP, NWH, NSH, NIN, NPO = (
        max(a, int(b * sf))
        for a, b in [(5, 100), (3, 20), (20, 5000), (10, 1000), (10, 2000)]
    )
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS purchase_orders; DROP TABLE IF EXISTS inventory;
    DROP TABLE IF EXISTS shipments; DROP TABLE IF EXISTS warehouses; DROP TABLE IF EXISTS suppliers;
    CREATE TABLE suppliers(supplier_id INTEGER PRIMARY KEY,name VARCHAR,country VARCHAR,
      reliability_score DECIMAL(4,2),lead_time_days INTEGER,category VARCHAR,is_preferred BOOLEAN);
    CREATE TABLE warehouses(wh_id INTEGER PRIMARY KEY,name VARCHAR,country VARCHAR,
      region VARCHAR,capacity_m3 INTEGER,is_active BOOLEAN);
    CREATE TABLE shipments(shipment_id INTEGER PRIMARY KEY,supplier_id INTEGER,wh_id INTEGER,
      sku VARCHAR,quantity INTEGER,unit_cost DECIMAL(10,2),shipped_date DATE,received_date DATE,
      status VARCHAR,freight_cost DECIMAL(10,2));
    CREATE TABLE inventory(inv_id INTEGER PRIMARY KEY,wh_id INTEGER,sku VARCHAR,
      qty_on_hand INTEGER,qty_reserved INTEGER,reorder_point INTEGER,snapshot_date DATE);
    CREATE TABLE purchase_orders(po_id INTEGER PRIMARY KEY,supplier_id INTEGER,sku VARCHAR,
      ordered_qty INTEGER,unit_price DECIMAL(10,2),order_date DATE,expected_date DATE,
      received_qty INTEGER,status VARCHAR);
    """)
    base = date(2021, 1, 1)
    cats = ["raw_materials", "components", "packaging", "finished_goods", "consumables"]
    regions = ["NA", "EU", "APAC", "ME", "SA"]
    statuses = ["delivered", "in_transit", "delayed", "cancelled", "lost"]
    pos = ["open", "partial", "complete", "cancelled"]
    skus = [f"SKU-{i:05d}" for i in range(1, 51)]

    cpu_count = min(4, os.cpu_count() or 1)

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        def run_parallel(gen_func, total, *args):
            chunk_size = max(1, total // cpu_count)
            futures = []
            for i in range(0, total, chunk_size):
                futures.append(executor.submit(gen_func, i + 1, min(i + chunk_size + 1, total + 1), *args))
            rows = []
            for f in futures:
                rows.extend(f.result())
            return rows

        batched_insert(con, "suppliers", ['supplier_id', 'name', 'country', 'reliability_score', 'lead_time_days', 'category', 'is_preferred'], 
                       run_parallel(generate_suppliers_chunk, NSUP, cats))
        
        batched_insert(con, "warehouses", ['wh_id', 'name', 'country', 'region', 'capacity_m3', 'is_active'],
                       run_parallel(generate_warehouses_chunk, NWH, regions))
        
        batched_insert(con, "shipments", ['shipment_id', 'supplier_id', 'wh_id', 'sku', 'quantity', 'unit_cost', 'shipped_date', 'received_date', 'status', 'freight_cost'],
                       run_parallel(generate_shipments_chunk, NSH, NSUP, NWH, skus, base, statuses))
        
        batched_insert(con, "inventory", ['inv_id', 'wh_id', 'sku', 'qty_on_hand', 'qty_reserved', 'reorder_point', 'snapshot_date'],
                       run_parallel(generate_inventory_chunk, NIN, NWH, skus, base))
        
        batched_insert(con, "purchase_orders", ['po_id', 'supplier_id', 'sku', 'ordered_qty', 'unit_price', 'order_date', 'expected_date', 'received_qty', 'status'],
                       run_parallel(generate_purchase_orders_chunk, NPO, NSUP, skus, base, pos))

    con.close()
    print(f"p05 done suppliers={NSUP} shipments={NSH}")

if __name__ == "__main__":
    main()
