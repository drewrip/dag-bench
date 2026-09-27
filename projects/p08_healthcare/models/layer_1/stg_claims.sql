select claim_id, patient_id, provider_id, service_date, claim_type,
    total_billed, total_allowed, total_paid, status, denial_reason,
    cast(total_billed-total_allowed as numeric(13,2)) as billed_gap, cast(total_allowed-total_paid as numeric(13,2)) as allowed_gap,
    total_paid>0 as has_payment, status='denied' as is_denied,
    cast(extract('year' from service_date) as bigint) as svc_year,
    cast(date_trunc('month',service_date) as date) as svc_month
from {{ source('hc','claims') }}
