select plan_type,
    count(distinct patient_id)              as members,
    sum(total_paid)                         as plan_paid,
    round(avg(total_paid),2)                as avg_paid_per_claim,
    count(*) filter (where is_high_cost)    as high_cost_members,
    round(avg(cost_decile),2)               as avg_cost_decile
from {{ ref('high_cost_patients') }}
group by plan_type
