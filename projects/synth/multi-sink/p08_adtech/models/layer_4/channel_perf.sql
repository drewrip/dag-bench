select channel, objective, count(distinct campaign_id) as campaigns,
    sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks,
    sum(conversions) as conversions, sum(revenue) as revenue,
    round(avg(ctr),4) as avg_ctr, round(avg(cvr),4) as avg_cvr,
    round(sum(revenue)/nullif(sum(spend),0),4) as channel_roas,
    rank() over (order by sum(revenue)/nullif(sum(spend),0) desc) as roas_rank
from {{ ref('campaign_funnel') }}
group by channel, objective
