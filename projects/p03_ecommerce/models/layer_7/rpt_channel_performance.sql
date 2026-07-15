select channel, order_year, ytd_revenue, avg_mom_growth,
    total_orders, unique_customers,
    round(ytd_revenue/nullif(unique_customers,0),2) as revenue_per_customer,
    current_timestamp as generated_at
from {{ ref('channel_ytd_summary') }}
order by order_year desc, ytd_revenue desc
