select po_id, supplier_id, sku, ordered_qty, unit_price, order_date,
    expected_date, received_qty, status,
    cast(received_qty*unit_price as numeric(18,2)) as received_value,
    cast(ordered_qty*unit_price as numeric(18,2))  as ordered_value,
    {{ datediff("order_date", "expected_date", "day") }} as promised_lead
from {{ source('sc','purchase_orders') }}
where status <> 'cancelled'
