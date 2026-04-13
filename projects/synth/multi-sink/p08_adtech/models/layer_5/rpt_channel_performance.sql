select channel, objective, campaigns, spend, impressions, clicks, conversions, revenue,
    avg_ctr, avg_cvr, channel_roas, roas_rank,
    round(spend*100.0/sum(spend) over(),2) as spend_share_pct,
    current_timestamp as report_ts
from {{ ref('channel_perf') }}
order by roas_rank
