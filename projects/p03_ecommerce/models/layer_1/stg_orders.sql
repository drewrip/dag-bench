select order_id, customer_id, order_date,
    lower(status) as status, lower(channel) as channel,
    cast(coalesce(discount_pct,0) as numeric(5,2)) as discount_pct,
    cast(coalesce(shipping_cost,0) as numeric(8,2)) as shipping_cost,
    cast(extract('year' from order_date) as bigint) as order_year,
    cast(extract('month' from order_date) as bigint) as order_month,
    cast(date_trunc('month',order_date) as date) as order_month_start,
    status in ('completed','shipped') as is_fulfilled,
    'internal' as source_system
from {{ source('raw','orders') }}
