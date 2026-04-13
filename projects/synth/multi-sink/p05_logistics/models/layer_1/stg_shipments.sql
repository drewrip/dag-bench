select shipment_id, supplier_id, wh_id, sku, quantity, unit_cost,
    shipped_date, received_date, status, freight_cost,
    quantity*unit_cost as cargo_value,
    date_diff('day',shipped_date,received_date) as transit_days,
    status='delivered' as is_delivered,
    status in ('delayed','lost') as is_problematic
from {{ source('sc','shipments') }}
