select
    extract('year' from c.signup_date)   as signup_year,
    c.country,
    cs.value_segment,
    count(distinct cs.customer_id)       as customers,
    round(sum(cs.total_revenue),2)       as cohort_revenue,
    round(avg(cs.avg_order_value),2)     as avg_aov,
    round(avg(cs.total_orders),2)        as avg_orders
from {{ ref('customer_segments') }} cs
join {{ ref('stg_customers') }} c using (customer_id)
group by 1,2,3
