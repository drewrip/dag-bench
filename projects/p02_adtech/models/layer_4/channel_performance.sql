select channel, objective,
    count(distinct campaign_id) as campaigns,
    cast(sum(total_spend) as numeric(18,6))                               as spend,
    cast(sum(total_impressions) as bigint)                         as impressions,
    cast(sum(total_clicks) as bigint)                              as clicks,
    cast(sum(total_conversions) as bigint)                         as conversions,
    cast(sum(total_revenue) as numeric(18,2))                             as revenue,
    cast(round(avg(ctr_pct), 4) as numeric(10,4)) as avg_ctr,
    cast(round(avg(cvr_pct), 4) as numeric(10,4)) as avg_cvr,
    cast(round(avg(roas), 4) as numeric(38,4))    as avg_roas,
    cast(round(sum(total_revenue)/nullif(sum(total_spend),0), 4) as numeric(38,4)) as channel_roas,
    rank() over (order by sum(total_revenue)/nullif(sum(total_spend),0) desc nulls last) as roas_rank
from {{ ref('campaign_funnel') }}
group by channel, objective
having count(distinct campaign_id) >= 2
