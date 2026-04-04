select claim_id,
    count(*)            as line_count,
    sum(billed_line_total) as total_line_billed,
    sum(paid_amount)    as total_line_paid,
    count(distinct cpt_code) as unique_cpts,
    max(unit_cost)      as max_unit_cost
from {{ ref('stg_claim_lines') }}
group by claim_id
