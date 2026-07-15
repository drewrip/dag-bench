select monetization_tier, engagement_tier, players, revenue,
    round(revenue*100.0/nullif(sum(revenue) over(),0),2) as revenue_share_pct,
    avg_sessions, avg_active_days, current_timestamp as report_ts
from {{ ref('monetization') }}
order by revenue desc
