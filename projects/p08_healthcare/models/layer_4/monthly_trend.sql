select svc_month, claim_type, plan_type,
    count(*) as claims, cast(sum(total_billed) as numeric(18,2)) as billed, cast(sum(total_paid) as numeric(18,2)) as paid,
    count(*) filter (where is_denied) as denials
from {{ ref('claims_enriched') }}
group by svc_month, claim_type, plan_type
having count(*) > 5
