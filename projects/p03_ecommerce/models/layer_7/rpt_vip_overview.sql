select value_segment, frequency_segment, country,
    count(distinct customer_id) as customers,
    round(sum(total_revenue),2) as segment_revenue,
    round(avg(avg_order_value),2) as avg_aov,
    current_timestamp as generated_at
from {{ ref('vip_customers') }}
group by value_segment, frequency_segment, country
order by segment_revenue desc
