select sku, wh_id, wh_name, region, qty_on_hand, qty_available,
    reorder_point, gap, suggested_order_qty, current_timestamp as report_ts
from {{ ref('reorder_alerts') }}
order by gap desc
