-- Analysts are only tracking mobile/US performance for the first 6 months of the launch cohort.
select campaign_id, imp_day, device, geo,
    count(*)                as impressions,
    count(distinct user_id) as unique_users,
    cast(sum(cost_usd) as numeric(18,6))           as spend,
    cast(sum(cost_usd)*1000.0/nullif(count(*),0) as numeric(38,6)) as cpm
from {{ ref('stg_impressions') }}
where device = 'mobile'
  and geo = 'US'
  and imp_day >= date '2023-01-01' and imp_day < date '2023-07-01'
group by campaign_id, imp_day, device, geo
