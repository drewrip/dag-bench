import duckdb, numpy as np, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_suppliers_chunk(start, end, cats):
    size = end - start
    rng = np.random.default_rng(start)
    countries = ["CN", "IN", "DE", "US", "MX", "BR"]
    country_indices = rng.integers(0, len(countries), size)
    scores = rng.uniform(0.5, 1.0, size)
    lead_times = rng.integers(3, 61, size)
    cat_indices = rng.integers(0, len(cats), size)
    preferred_probs = rng.random(size)
    
    supplier_ids = range(start, end)
    supplier_names = [f"Supplier {i}" for i in supplier_ids]
    selected_countries = np.take(countries, country_indices).tolist()
    scores_rounded = np.round(scores, 2).tolist()
    selected_lead_times = lead_times.tolist()
    selected_cats = np.take(cats, cat_indices).tolist()
    is_preferred = (preferred_probs > 0.5).tolist()
    
    return list(zip(supplier_ids, supplier_names, selected_countries, scores_rounded, selected_lead_times, selected_cats, is_preferred))


def generate_warehouses_chunk(start, end, regions):
    size = end - start
    rng = np.random.default_rng(start)
    countries = ["US", "DE", "SG", "BR", "AU"]
    country_indices = rng.integers(0, len(countries), size)
    region_indices = rng.integers(0, len(regions), size)
    capacities = rng.integers(1000, 50001, size)
    active_probs = rng.random(size)

    wh_ids = range(start, end)
    wh_names = [f"WH-{i}" for i in wh_ids]
    selected_countries = np.take(countries, country_indices).tolist()
    selected_regions = np.take(regions, region_indices).tolist()
    selected_capacities = capacities.tolist()
    is_active = (active_probs > 0.05).tolist()
    
    return list(zip(wh_ids, wh_names, selected_countries, selected_regions, selected_capacities, is_active))


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

    shipment_ids = range(start, end)
    selected_supplier_ids = supplier_ids.tolist()
    selected_wh_ids = wh_ids.tolist()
    selected_skus = np.take(skus, sku_indices).tolist()
    selected_quantities = quantities.tolist()
    unit_costs_rounded = np.round(unit_costs, 2).tolist()
    
    shipped_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]"))
    received_dates = shipped_dates + transit_days.astype("timedelta64[D]")
    
    selected_statuses = np.take(statuses, status_indices).tolist()
    freight_costs_rounded = np.round(freight_costs, 2).tolist()
    
    return list(zip(
        shipment_ids,
        selected_supplier_ids,
        selected_wh_ids,
        selected_skus,
        selected_quantities,
        unit_costs_rounded,
        shipped_dates.tolist(),
        received_dates.tolist(),
        selected_statuses,
        freight_costs_rounded
    ))


def generate_inventory_chunk(start, end, NWH, skus, base):
    size = end - start
    rng = np.random.default_rng(start)
    wh_ids = rng.integers(1, NWH + 1, size)
    sku_indices = rng.integers(0, len(skus), size)
    qtys_on_hand = rng.integers(0, 10001, size)
    qtys_reserved = rng.integers(0, 501, size)
    reorder_points = rng.integers(100, 1001, size)
    days_offset = rng.integers(800, 1001, size)

    inv_ids = range(start, end)
    selected_wh_ids = wh_ids.tolist()
    selected_skus = np.take(skus, sku_indices).tolist()
    selected_on_hand = qtys_on_hand.tolist()
    selected_reserved = qtys_reserved.tolist()
    selected_reorder = reorder_points.tolist()
    snapshot_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]")).tolist()
    
    return list(zip(inv_ids, selected_wh_ids, selected_skus, selected_on_hand, selected_reserved, selected_reorder, snapshot_dates))


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

    po_ids = range(start, end)
    selected_supplier_ids = supplier_ids.tolist()
    selected_skus = np.take(skus, sku_indices).tolist()
    selected_ordered_qtys = ordered_qtys.tolist()
    unit_prices_rounded = np.round(unit_prices, 2).tolist()
    
    order_dates = (np.datetime64(base) + days_offset.astype("timedelta64[D]"))
    expected_dates = order_dates + expected_days.astype("timedelta64[D]")
    
    selected_received_qtys = received_qtys.tolist()
    selected_statuses = np.take(po_statuses, status_indices).tolist()
    
    return list(zip(
        po_ids,
        selected_supplier_ids,
        selected_skus,
        selected_ordered_qtys,
        unit_prices_rounded,
        order_dates.tolist(),
        expected_dates.tolist(),
        selected_received_qtys,
        selected_statuses
    ))


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 6000.0
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

    cpu_count = get_worker_count()
    progress = GenerationProgress("p05_logistics", 5)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("suppliers")
        batched_insert(con, "suppliers", ['supplier_id', 'name', 'country', 'reliability_score', 'lead_time_days', 'category', 'is_preferred'],
                       run_parallel(executor, generate_suppliers_chunk, NSUP, cats))
        progress.advance("warehouses")
        batched_insert(con, "warehouses", ['wh_id', 'name', 'country', 'region', 'capacity_m3', 'is_active'],
                       run_parallel(executor, generate_warehouses_chunk, NWH, regions))
        progress.advance("shipments")
        batched_insert(con, "shipments", ['shipment_id', 'supplier_id', 'wh_id', 'sku', 'quantity', 'unit_cost', 'shipped_date', 'received_date', 'status', 'freight_cost'],
                       run_parallel(executor, generate_shipments_chunk, NSH, NSUP, NWH, skus, base, statuses))
        progress.advance("inventory")
        batched_insert(con, "inventory", ['inv_id', 'wh_id', 'sku', 'qty_on_hand', 'qty_reserved', 'reorder_point', 'snapshot_date'],
                       run_parallel(executor, generate_inventory_chunk, NIN, NWH, skus, base))
        progress.advance("purchase_orders")
        batched_insert(con, "purchase_orders", ['po_id', 'supplier_id', 'sku', 'ordered_qty', 'unit_price', 'order_date', 'expected_date', 'received_qty', 'status'],
                       run_parallel(executor, generate_purchase_orders_chunk, NPO, NSUP, skus, base, po_statuses))


    con.close()
    print_generation_summary(
        "p05_logistics",
        sf,
        {
            "suppliers": NSUP,
            "warehouses": NWH,
            "shipments": NSH,
            "inventory": NIN,
            "purchase_orders": NPO,
        },
    )

if __name__ == "__main__":
    main()
