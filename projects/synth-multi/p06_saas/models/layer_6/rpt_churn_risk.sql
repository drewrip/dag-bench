select account_id, name, industry, country, arr, risk_band, composite_health, risk_rank,
    current_timestamp as report_ts
from {{ ref('churn_risk') }}
order by risk_rank
