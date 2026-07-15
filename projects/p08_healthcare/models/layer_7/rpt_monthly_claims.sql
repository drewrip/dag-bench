select svc_month, claim_type, plan_type, claims, billed, paid, denials,
    round(denials*100.0/nullif(claims,0),2) as denial_rate_pct,
    current_timestamp as report_ts
from {{ ref('monthly_trend') }}
order by svc_month, claim_type
