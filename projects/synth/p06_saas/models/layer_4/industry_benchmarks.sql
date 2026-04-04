select industry,
    count(distinct account_id)  as account_count,
    round(avg(arr),2)           as avg_arr,
    round(avg(composite_health),2) as avg_health,
    round(avg(active_days),1)   as avg_active_days,
    round(avg(features_used),1) as avg_features_used,
    sum(arr)                    as total_arr
from {{ ref('account_health_composite') }}
group by industry
