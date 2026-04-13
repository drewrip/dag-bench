select claim_id, patient_id, provider_id, service_date, claim_type,
    total_billed, total_allowed, total_paid, status, denial_reason,
    total_billed - total_allowed               as billed_vs_allowed_gap,
    total_allowed - total_paid                 as allowed_vs_paid_gap,
    total_paid > 0                             as has_payment,
    status = 'denied'                          as is_denied,
    extract('year'  from service_date)         as service_year,
    extract('month' from service_date)         as service_month,
    date_trunc('month', service_date)          as service_month_start
from {{ source('hc','claims') }}
