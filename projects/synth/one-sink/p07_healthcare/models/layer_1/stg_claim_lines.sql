select line_id, claim_id, cpt_code, quantity, unit_cost,
    allowed_amount, paid_amount,
    quantity * unit_cost    as billed_line_total,
    allowed_amount - paid_amount as line_patient_responsibility
from {{ source('hc','claim_lines') }}
