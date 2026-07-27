select channel, order_year,
    max(ytd_revenue) as ytd_revenue,
    avg(mom_growth_pct) as avg_mom_growth,
    sum(orders) as total_orders,
    sum(unique_customers) as unique_customers
from (
    select *, max(order_year) over () as max_order_year
    from {{ ref('monthly_growth') }}
) mg
where order_year = max_order_year
group by channel, order_year
