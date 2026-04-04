select campaign_id, campaign_name, channel, advertiser, objective, budget,
    impressions, reach, spend, clicks, conversions, revenue,
    ctr, cvr, cpc, cpa, roas,
    round(revenue/nullif(budget,0),4) as budget_roi,
    current_timestamp as report_ts
from {{ ref('campaign_funnel') }}
order by roas desc nulls last
