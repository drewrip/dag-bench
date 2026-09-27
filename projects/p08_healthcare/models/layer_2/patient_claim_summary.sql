select patient_id, count(distinct claim_id) as total_claims,
    cast(sum(total_billed) as numeric(18,2)) as total_billed, cast(sum(total_paid) as numeric(18,2)) as total_paid,
    count(*) filter (where is_denied) as denied, max(service_date) as last_service
from {{ ref('stg_claims') }}
group by patient_id
