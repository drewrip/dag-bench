select line_id, claim_id, cpt_code, quantity, unit_cost, allowed_amount, paid_amount,
    cast(quantity*unit_cost as numeric(18,2)) as billed_total, cast(allowed_amount-paid_amount as numeric(11,2)) as patient_responsibility
from {{ source('hc','claim_lines') }}
