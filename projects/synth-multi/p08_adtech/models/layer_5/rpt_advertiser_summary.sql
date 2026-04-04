select advertiser,
    count(distinct campaign_id) as campaigns,
    sum(spend) as total_spend, sum(revenue) as total_revenue,
    round(sum(revenue)/nullif(sum(spend),0),4) as overall_roas,
    sum(conversions) as total_conversions,
    round(sum(spend)/nullif(sum(conversions),0),2) as blended_cpa,
    current_timestamp as report_ts
from {{ ref('campaign_funnel') }}
group by advertiser
order by total_spend desc
