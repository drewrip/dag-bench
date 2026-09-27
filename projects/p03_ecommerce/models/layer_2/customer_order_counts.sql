select customer_id,
    count(distinct order_id) as total_orders,
    count(distinct order_id) filter (where is_fulfilled) as fulfilled_orders,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    cast(sum(shipping_cost) as numeric(18,2)) as total_shipping
from {{ ref('stg_orders') }}
group by customer_id
