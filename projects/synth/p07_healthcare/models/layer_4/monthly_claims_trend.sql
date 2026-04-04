select service_month_start, claim_type, plan_type,
    count(*)              as claims,
    sum(total_billed)     as total_billed,
    sum(total_paid)       as total_paid,
    round(avg(total_billed),2) as avg_claim_size,
    count(*) filter (where is_denied) as denials
from {{ ref('claims_enriched') }}
group by service_month_start, claim_type, plan_type
