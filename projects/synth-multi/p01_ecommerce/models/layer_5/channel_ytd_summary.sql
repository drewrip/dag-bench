select channel, order_year,
    max(ytd_revenue) as ytd_revenue,
    avg(mom_growth_pct) as avg_mom_growth,
    sum(orders) as total_orders,
    sum(unique_customers) as unique_customers
from {{ ref('monthly_growth') }}
group by channel, order_year
