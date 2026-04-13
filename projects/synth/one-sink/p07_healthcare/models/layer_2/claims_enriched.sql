select c.*, p.gender, p.plan_type, p.state as patient_state,
    p.age_years, p.age_group,
    pr.specialty, pr.is_in_network, pr.provider_name
from {{ ref('stg_claims') }} c
join {{ ref('stg_patients') }} p using (patient_id)
join {{ ref('stg_providers') }} pr using (provider_id)
