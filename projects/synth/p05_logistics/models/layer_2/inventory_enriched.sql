select i.*, w.wh_name, w.region, w.capacity_m3,
    round(i.qty_on_hand * 100.0 / nullif(w.capacity_m3, 0), 2) as capacity_utilization_pct
from {{ ref('stg_inventory') }} i
join {{ ref('stg_warehouses') }} w using (wh_id)
