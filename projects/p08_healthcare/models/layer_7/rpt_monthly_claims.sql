select svc_month, claim_type, plan_type, claims, billed, paid, denials,
    round(denials*100.0/nullif(claims,0),2) as denial_rate_pct,
    current_timestamp as report_ts
from (
    select *, max(svc_month) over () as max_svc_month
    from {{ ref('monthly_trend') }}
) mt
where svc_month >= max_svc_month - interval '12 months'
order by svc_month, claim_type
