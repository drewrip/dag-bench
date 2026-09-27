select claim_id, count(*) as lines, cast(sum(billed_total) as numeric(18,2)) as billed, cast(sum(paid_amount) as numeric(18,2)) as paid,
    count(distinct cpt_code) as unique_cpts, cast(max(unit_cost) as numeric(10,2)) as max_unit_cost
from {{ ref('stg_claim_lines') }}
group by claim_id
