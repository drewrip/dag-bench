select campaign_id, imp_day, device, geo,
    count(*) as impressions, count(distinct user_id) as unique_users,
    sum(cost_usd) as spend, sum(cost_usd)*1000.0/nullif(count(*),0) as cpm
from {{ ref('stg_impressions') }}
group by campaign_id, imp_day, device, geo
