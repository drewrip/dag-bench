select channel, order_year,
    cast(max(ytd_revenue) as numeric(18,2)) as ytd_revenue,
    cast(avg(mom_growth_pct) as numeric(38,6)) as avg_mom_growth,
    cast(sum(orders) as bigint) as total_orders,
    cast(sum(unique_customers) as bigint) as unique_customers
from (
    select *, max(order_year) over () as max_order_year
    from {{ ref('monthly_growth') }}
) mg
where order_year = max_order_year
group by channel, order_year
