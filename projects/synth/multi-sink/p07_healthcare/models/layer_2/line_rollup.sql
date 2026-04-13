select claim_id, count(*) as lines, sum(billed_total) as billed, sum(paid_amount) as paid,
    count(distinct cpt_code) as unique_cpts, max(unit_cost) as max_unit_cost
from {{ ref('stg_claim_lines') }}
group by claim_id
