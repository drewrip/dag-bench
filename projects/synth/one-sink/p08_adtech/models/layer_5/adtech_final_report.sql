select channel, objective, campaigns, spend, impressions,
    clicks, conversions, revenue,
    avg_ctr, avg_cvr, avg_roas, roas_rank,
    round(spend*100.0/sum(spend) over(),2) as spend_share_pct,
    round(revenue*100.0/sum(revenue) over(),2) as revenue_share_pct,
    current_timestamp as report_ts
from {{ ref('channel_performance') }}
order by roas_rank
