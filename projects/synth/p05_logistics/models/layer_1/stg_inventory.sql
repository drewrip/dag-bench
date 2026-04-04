select inv_id, wh_id, sku, qty_on_hand, qty_reserved,
    qty_on_hand - qty_reserved as qty_available,
    reorder_point, snapshot_date,
    qty_on_hand <= reorder_point as needs_reorder
from {{ source('sc','inventory') }}
