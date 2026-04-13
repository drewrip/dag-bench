select account_id, name, industry, country, arr,
    composite_health, active_days, features_used,
    total_tickets, avg_csat,
    case
        when composite_health <  30 then 'CRITICAL'
        when composite_health <  50 then 'AT_RISK'
        when composite_health <  70 then 'NEUTRAL'
        else 'HEALTHY'
    end as churn_risk_band,
    rank() over (order by composite_health asc) as churn_risk_rank
from {{ ref('account_health_composite') }}
