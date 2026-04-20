select account_id, name, industry, country, arr, created_date, csm_id, health_score,
    arr/12.0 as implied_mrr, {{ datediff("created_date", "current_date", "day") }} as age_days
from {{ source('saas','accounts') }}
