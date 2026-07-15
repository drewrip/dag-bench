select patient_id, age_group, plan_type, gender, total_paid, total_claims,
    chronic_count, has_chronic, cost_decile, is_high_cost,
    current_timestamp as report_ts
from {{ ref('high_cost_patients') }}
where cost_decile<=2
order by total_paid desc
