import duckdb, sys, os
import numpy as np
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_suppliers_chunk(start, end, cats):
    rng = np.random.default_rng(start)
    size = end - start
    country_idx = rng.integers(0, 5, size)
    rel_rand = rng.uniform(0.5, 1, size)
    lead_rand = rng.integers(3, 61, size)
    cat_idx = rng.integers(0, len(cats), size)
    pref_rand = rng.random(size)
    countries = ["CN", "IN", "DE", "US", "MX"]
    return [
        (
            i,
            f"Supplier {i}",
            countries[country_idx[i - start]],
            round(float(rel_rand[i - start]), 2),
            int(lead_rand[i - start]),
            cats[cat_idx[i - start]],
            bool(pref_rand[i - start] > 0.5),
        )
        for i in range(start, end)
    ]


def generate_warehouses_chunk(start, end, regions):
    rng = np.random.default_rng(start)
    size = end - start
    country_idx = rng.integers(0, 5, size)
    reg_idx = rng.integers(0, len(regions), size)
    cap_rand = rng.integers(1000, 50001, size)
    active_rand = rng.random(size)
    countries = ["US", "DE", "SG", "BR", "AU"]
    return [
        (
            i,
            f"WH-{i}",
            countries[country_idx[i - start]],
            regions[reg_idx[i - start]],
            int(cap_rand[i - start]),
            bool(active_rand[i - start] > 0.05),
        )
        for i in range(start, end)
    ]


def generate_shipments_chunk(start, end, NSUP, NWH, skus, base, statuses):
    rng = np.random.default_rng(start)
    size = end - start
    sup_rand = rng.integers(1, NSUP + 1, size)
    wh_rand = rng.integers(1, NWH + 1, size)
    sku_idx = rng.integers(0, len(skus), size)
    qty_rand = rng.integers(10, 10001, size)
    cost_rand = rng.uniform(1, 500, size)
    sd_days = rng.integers(0, 1001, size)
    rec_days = rng.integers(3, 46, size)
    stat_idx = rng.integers(0, len(statuses), size)
    freight_rand = rng.uniform(50, 5001, size)
    return [
        (
            i,
            int(sup_rand[i - start]),
            int(wh_rand[i - start]),
            skus[sku_idx[i - start]],
            int(qty_rand[i - start]),
            round(float(cost_rand[i - start]), 2),
            (sd := base + timedelta(days=int(sd_days[i - start]))),
            sd + timedelta(days=int(rec_days[i - start])),
            statuses[stat_idx[i - start]],
            round(float(freight_rand[i - start]), 2),
        )
        for i in range(start, end)
    ]


def generate_inventory_chunk(start, end, NWH, skus, base):
    rng = np.random.default_rng(start)
    size = end - start
    wh_rand = rng.integers(1, NWH + 1, size)
    sku_idx = rng.integers(0, len(skus), size)
    qty_on_rand = rng.integers(0, 10001, size)
    qty_res_rand = rng.integers(0, 501, size)
    reorder_rand = rng.integers(100, 1001, size)
    snap_days = rng.integers(800, 1001, size)
    return [
        (
            i,
            int(wh_rand[i - start]),
            skus[sku_idx[i - start]],
            int(qty_on_rand[i - start]),
            int(qty_res_rand[i - start]),
            int(reorder_rand[i - start]),
            base + timedelta(days=int(snap_days[i - start])),
        )
        for i in range(start, end)
    ]


def generate_purchase_orders_chunk(start, end, NSUP, skus, base, pos):
    rng = np.random.default_rng(start)
    size = end - start
    sup_rand = rng.integers(1, NSUP + 1, size)
    sku_idx = rng.integers(0, len(skus), size)
    ord_qty_rand = rng.integers(100, 5001, size)
    price_rand = rng.uniform(1, 500, size)
    od_days = rng.integers(0, 901, size)
    exp_days = rng.integers(7, 61, size)
    rec_qty_rand = rng.integers(0, 5001, size)
    stat_idx = rng.integers(0, len(pos), size)
    return [
        (
            i,
            int(sup_rand[i - start]),
            skus[sku_idx[i - start]],
            int(ord_qty_rand[i - start]),
            round(float(price_rand[i - start]), 2),
            (od := base + timedelta(days=int(od_days[i - start]))),
            od + timedelta(days=int(exp_days[i - start])),
            int(rec_qty_rand[i - start]),
            pos[stat_idx[i - start]],
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    NSUP, NWH, NSH, NIN, NPO = (
        max(a, int(b * sf_adj))
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

    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "suppliers",
            [
                "supplier_id",
                "name",
                "country",
                "reliability_score",
                "lead_time_days",
                "category",
                "is_preferred",
            ],
            run_parallel(executor, generate_suppliers_chunk, NSUP, cats),
        )

        batched_insert(
            con,
            "warehouses",
            ["wh_id", "name", "country", "region", "capacity_m3", "is_active"],
            run_parallel(executor, generate_warehouses_chunk, NWH, regions),
        )

        batched_insert(
            con,
            "shipments",
            [
                "shipment_id",
                "supplier_id",
                "wh_id",
                "sku",
                "quantity",
                "unit_cost",
                "shipped_date",
                "received_date",
                "status",
                "freight_cost",
            ],
            run_parallel(
                executor, generate_shipments_chunk, NSH, NSUP, NWH, skus, base, statuses
            ),
        )

        batched_insert(
            con,
            "inventory",
            [
                "inv_id",
                "wh_id",
                "sku",
                "qty_on_hand",
                "qty_reserved",
                "reorder_point",
                "snapshot_date",
            ],
            run_parallel(executor, generate_inventory_chunk, NIN, NWH, skus, base),
        )

        batched_insert(
            con,
            "purchase_orders",
            [
                "po_id",
                "supplier_id",
                "sku",
                "ordered_qty",
                "unit_price",
                "order_date",
                "expected_date",
                "received_qty",
                "status",
            ],
            run_parallel(
                executor, generate_purchase_orders_chunk, NPO, NSUP, skus, base, pos
            ),
        )

    con.close()
    print(f"p05 done suppliers={NSUP} shipments={NSH}")


if __name__ == "__main__":
    main()
