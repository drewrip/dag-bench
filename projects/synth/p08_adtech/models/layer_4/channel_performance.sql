select channel, objective,
    count(distinct campaign_id) as campaigns,
    sum(total_spend)            as spend,
    sum(total_impressions)      as impressions,
    sum(total_clicks)           as clicks,
    sum(total_conversions)      as conversions,
    sum(total_revenue)          as revenue,
    round(avg(ctr_pct),4)       as avg_ctr,
    round(avg(cvr_pct),4)       as avg_cvr,
    round(avg(roas),4)          as avg_roas,
    round(sum(total_revenue)/nullif(sum(total_spend),0),4) as channel_roas,
    rank() over (order by sum(total_revenue)/nullif(sum(total_spend),0) desc) as roas_rank
from {{ ref('campaign_funnel') }}
group by channel, objective
