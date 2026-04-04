select account_id, name, industry, country, arr,
    created_date, csm_id, health_score,
    arr / 12.0 as mrr_equiv,
    date_diff('day', created_date, current_date) as account_age_days
from {{ source('saas','accounts') }}
