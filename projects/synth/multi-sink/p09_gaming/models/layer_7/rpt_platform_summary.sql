select platform, monetization_tier, players, revenue, avg_active_days, avg_cr,
    round(revenue*100.0/sum(revenue) over(partition by platform),2) as tier_revenue_share,
    current_timestamp as report_ts
from {{ ref('platform_summary') }}
order by platform, revenue desc
