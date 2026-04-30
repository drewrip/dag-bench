import duckdb, numpy as np, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_suppliers_chunk(start, end, cats):
    size = end - start
    rng = np.random.default_rng(start)
    countries = ["CN", "IN", "DE", "US", "MX", "BR"]
    country_indices = rng.integers(0, len(countries), size)
    scores = rng.uniform(0.5, 1.0, size)
    lead_times = rng.integers(3, 61, size)
    cat_indices = rng.integers(0, len(cats), size)
    preferred_probs = rng.random(size)
    
    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Supplier {i}",
            countries[country_indices[idx]],
            round(float(scores[idx]), 2),
            int(lead_times[idx]),
            cats[cat_indices[idx]],
            bool(preferred_probs[idx] > 0.5),
        ))
    return rows

def generate_warehouses_chunk(start, end, regions):
    size = end - start
    rng = np.random.default_rng(start)
    countries = ["US", "DE", "SG", "BR", "AU"]
    country_indices = rng.integers(0, len(countries), size)
    region_indices = rng.integers(0, len(regions), size)
    capacities = rng.integers(1000, 50001, size)
    active_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"WH-{i}",
            countries[country_indices[idx]],
            regions[region_indices[idx]],
            int(capacities[idx]),
            bool(active_probs[idx] > 0.05),
        ))
    return rows

def generate_shipments_chunk(start, end, NSUP, NWH, skus, base, statuses):
    size = end - start
    rng = np.random.default_rng(start)
    supplier_ids = rng.integers(1, NSUP + 1, size)
    wh_ids = rng.integers(1, NWH + 1, size)
    sku_indices = rng.integers(0, len(skus), size)
    quantities = rng.integers(10, 10001, size)
    unit_costs = rng.uniform(1, 500, size)
    days_offset = rng.integers(0, 1001, size)
    transit_days = rng.integers(3, 46, size)
    status_indices = rng.integers(0, len(statuses), size)
    freight_costs = rng.uniform(50, 5000, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        sd = base + timedelta(days=int(days_offset[idx]))
        rows.append((
            i,
            int(supplier_ids[idx]),
            int(wh_ids[idx]),
            skus[sku_indices[idx]],
            int(quantities[idx]),
            round(float(unit_costs[idx]), 2),
            sd,
            sd + timedelta(days=int(transit_days[idx])),
            statuses[status_indices[idx]],
            round(float(freight_costs[idx]), 2),
        ))
    return rows

def generate_inventory_chunk(start, end, NWH, skus, base):
    size = end - start
    rng = np.random.default_rng(start)
    wh_ids = rng.integers(1, NWH + 1, size)
    sku_indices = rng.integers(0, len(skus), size)
    qtys_on_hand = rng.integers(0, 10001, size)
    qtys_reserved = rng.integers(0, 501, size)
    reorder_points = rng.integers(100, 1001, size)
    days_offset = rng.integers(800, 1001, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(wh_ids[idx]),
            skus[sku_indices[idx]],
            int(qtys_on_hand[idx]),
            int(qtys_reserved[idx]),
            int(reorder_points[idx]),
            base + timedelta(days=int(days_offset[idx])),
        ))
    return rows

def generate_purchase_orders_chunk(start, end, NSUP, skus, base, po_statuses):
    size = end - start
    rng = np.random.default_rng(start)
    supplier_ids = rng.integers(1, NSUP + 1, size)
    sku_indices = rng.integers(0, len(skus), size)
    ordered_qtys = rng.integers(100, 5001, size)
    unit_prices = rng.uniform(1, 500, size)
    days_offset = rng.integers(0, 901, size)
    expected_days = rng.integers(7, 61, size)
    received_qtys = rng.integers(0, 5001, size)
    status_indices = rng.integers(0, len(po_statuses), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        od = base + timedelta(days=int(days_offset[idx]))
        rows.append((
            i,
            int(supplier_ids[idx]),
            skus[sku_indices[idx]],
            int(ordered_qty := int(ordered_qtys[idx])),
            round(float(unit_prices[idx]), 2),
            od,
            od + timedelta(days=int(expected_days[idx])),
            int(received_qtys[idx]),
            po_statuses[status_indices[idx]],
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1000.0
    NSUP = max(5, int(100 * sf_adj))
    NWH = max(3, int(20 * sf_adj))
    NSH = max(20, int(5000 * sf_adj))
    NIN = max(10, int(1000 * sf_adj))
    NPO = max(10, int(2000 * sf_adj))


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

    cpu_count = os.cpu_count()
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "suppliers", ['supplier_id', 'name', 'country', 'reliability_score', 'lead_time_days', 'category', 'is_preferred'],
                       run_parallel(executor, generate_suppliers_chunk, NSUP, cats))
        batched_insert(con, "warehouses", ['wh_id', 'name', 'country', 'region', 'capacity_m3', 'is_active'],
                       run_parallel(executor, generate_warehouses_chunk, NWH, regions))
        batched_insert(con, "shipments", ['shipment_id', 'supplier_id', 'wh_id', 'sku', 'quantity', 'unit_cost', 'shipped_date', 'received_date', 'status', 'freight_cost'],
                       run_parallel(executor, generate_shipments_chunk, NSH, NSUP, NWH, skus, base, statuses))
        batched_insert(con, "inventory", ['inv_id', 'wh_id', 'sku', 'qty_on_hand', 'qty_reserved', 'reorder_point', 'snapshot_date'],
                       run_parallel(executor, generate_inventory_chunk, NIN, NWH, skus, base))
        batched_insert(con, "purchase_orders", ['po_id', 'supplier_id', 'sku', 'ordered_qty', 'unit_price', 'order_date', 'expected_date', 'received_qty', 'status'],
                       run_parallel(executor, generate_purchase_orders_chunk, NPO, NSUP, skus, base, po_statuses))


    con.close()
    print(f"p05 done: suppliers={NSUP} shipments={NSH} pos={NPO}")

if __name__ == "__main__":
    main()
