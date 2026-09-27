select account_id, name, industry, country, arr, created_date, csm_id, health_score,
    cast(arr/12.0 as numeric(38,6)) as implied_mrr, {{ datediff("created_date", "current_date", "day") }} as age_days
from {{ source('saas','accounts') }}
where name not ilike '%test%'
