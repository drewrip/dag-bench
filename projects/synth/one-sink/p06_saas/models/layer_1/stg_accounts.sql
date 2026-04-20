select account_id, name, industry, country, arr,
    created_date, csm_id, health_score,
    arr / 12.0 as mrr_equiv,
    {{ datediff("created_date", "current_date", "day") }} as account_age_days
from {{ source('saas','accounts') }}
