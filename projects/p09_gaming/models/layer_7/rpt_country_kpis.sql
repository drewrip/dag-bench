select country, players, total_revenue, arpu, avg_active_days, paying, conversion_pct,
    rank() over (order by total_revenue desc) as revenue_rank,
    current_timestamp as report_ts
from {{ ref('country_kpis') }}
order by total_revenue desc
