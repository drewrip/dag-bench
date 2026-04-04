select customer_id, country, total_revenue, total_gp,
    total_orders, customer_lifespan_days,
    round(total_revenue/nullif(total_orders,0),2)  as avg_order_value,
    case
        when total_revenue >= 5000 then 'VIP'
        when total_revenue >= 1000 then 'High'
        when total_revenue >= 200  then 'Mid'
        else 'Low'
    end as value_segment,
    case
        when total_orders >= 20 then 'Loyal'
        when total_orders >= 5  then 'Repeat'
        else 'One-time'
    end as frequency_segment
from {{ ref('customer_ltv') }}
