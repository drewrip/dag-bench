select i.sku, i.wh_id, i.wh_name, i.region,
    i.qty_on_hand, i.qty_available, i.reorder_point,
    i.reorder_point-i.qty_available as gap,
    round((i.reorder_point-i.qty_available)*1.5) as suggested_order_qty
from {{ ref('inventory_enriched') }} i
where i.needs_reorder
