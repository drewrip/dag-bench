select po_id, supplier_id, sku, ordered_qty, unit_price, order_date,
    expected_date, received_qty, status,
    received_qty*unit_price as received_value,
    ordered_qty*unit_price  as ordered_value,
    date_diff('day',order_date,expected_date) as promised_lead
from {{ source('sc','purchase_orders') }}
