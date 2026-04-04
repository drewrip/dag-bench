select i.*, w.wh_name, w.region,
    round(i.qty_on_hand*100.0/nullif(w.capacity_m3,0),2) as capacity_pct
from {{ ref('stg_inventory') }} i
join {{ ref('stg_warehouses') }} w using (wh_id)
