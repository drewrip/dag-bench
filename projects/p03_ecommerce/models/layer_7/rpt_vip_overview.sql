select value_segment, frequency_segment, country,
    count(distinct customer_id) as customers,
    round(cast(sum(total_revenue) as numeric(38,10)), 2) as segment_revenue,
    round(cast(avg(avg_order_value) as numeric(38,10)), 2) as avg_aov,
    current_timestamp as generated_at
from {{ ref('vip_customers') }}
group by value_segment, frequency_segment, country
order by segment_revenue desc nulls last, value_segment, frequency_segment, country
limit 20
