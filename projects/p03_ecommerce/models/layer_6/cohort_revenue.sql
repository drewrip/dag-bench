select extract('year' from c.signup_date)::INTEGER as signup_year,
    c.country, cs.value_segment,
    count(distinct cs.customer_id) as customers,
    round(cast(sum(cs.total_revenue) as numeric(38,10)), 2) as cohort_revenue,
    round(cast(avg(cs.avg_order_value) as numeric(38,10)), 2) as avg_aov
from {{ ref('customer_segments') }} cs
join {{ ref('stg_customers') }} c using (customer_id)
group by 1,2,3
