select cs.customer_id, cs.country, cs.value_segment, cs.frequency_segment,
    cs.total_revenue, cs.total_gp, cs.avg_order_value, cs.total_orders
from {{ ref('customer_segments') }} cs
where cs.value_segment in ('VIP','High')
