select order_id, customer_id, order_date,
    lower(status) as status, lower(channel) as channel,
    coalesce(discount_pct,0) as discount_pct,
    coalesce(shipping_cost,0) as shipping_cost,
    extract('year' from order_date) as order_year,
    extract('month' from order_date) as order_month,
    date_trunc('month',order_date) as order_month_start,
    status in ('completed','shipped') as is_fulfilled
from {{ source('raw','orders') }}
