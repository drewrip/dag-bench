select channel, objective,
    count(distinct campaign_id) as campaigns,
    sum(total_spend)                               as spend,
    sum(total_impressions)                         as impressions,
    sum(total_clicks)                              as clicks,
    sum(total_conversions)                         as conversions,
    sum(total_revenue)                             as revenue,
    round(cast(avg(ctr_pct) as numeric(38,10)), 4) as avg_ctr,
    round(cast(avg(cvr_pct) as numeric(38,10)), 4) as avg_cvr,
    round(cast(avg(roas) as numeric(38,10)), 4)    as avg_roas,
    round(cast(sum(total_revenue)/nullif(sum(total_spend),0) as numeric(38,10)), 4) as channel_roas,
    rank() over (order by sum(total_revenue)/nullif(sum(total_spend),0) desc nulls last) as roas_rank
from {{ ref('campaign_funnel') }}
group by channel, objective
having count(distinct campaign_id) >= 2
