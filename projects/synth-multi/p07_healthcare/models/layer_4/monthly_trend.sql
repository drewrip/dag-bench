select svc_month, claim_type, plan_type,
    count(*) as claims, sum(total_billed) as billed, sum(total_paid) as paid,
    count(*) filter (where is_denied) as denials
from {{ ref('claims_enriched') }}
group by svc_month, claim_type, plan_type
