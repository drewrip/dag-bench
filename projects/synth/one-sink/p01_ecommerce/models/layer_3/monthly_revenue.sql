select order_year, order_month, channel,
    count(distinct order_id)        as orders,
    count(distinct customer_id)     as unique_customers,
    round(sum(disc_revenue),2)      as revenue,
    round(sum(line_gross_profit),2) as gross_profit,
    round(avg(disc_revenue),4)      as avg_line_value
from {{ ref('order_line_facts') }}
where is_fulfilled
group by order_year, order_month, channel
