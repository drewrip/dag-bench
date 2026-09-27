select shipment_id, supplier_id, wh_id, sku, quantity, unit_cost,
    shipped_date, received_date, status, freight_cost,
    cast(quantity*unit_cost as numeric(18,2)) as cargo_value,
    {{ datediff("shipped_date", "received_date", "day") }} as transit_days,
    status='delivered' as is_delivered,
    status in ('delayed','lost') as is_problematic
from {{ source('sc','shipments') }}
where shipped_date is not null
