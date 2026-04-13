select patient_id, count(distinct claim_id) as total_claims,
    sum(total_billed) as total_billed, sum(total_paid) as total_paid,
    count(*) filter (where is_denied) as denied, max(service_date) as last_service
from {{ ref('stg_claims') }}
group by patient_id
