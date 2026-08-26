select plan_type, count(distinct patient_id) as members,
    sum(total_paid) as paid, round(cast(avg(total_paid) as numeric(38,10)), 2) as avg_paid,
    count(*) filter (where is_high_cost) as high_cost_members,
    round(cast(avg(cost_decile) as numeric(38,10)), 2) as avg_decile
from {{ ref('high_cost_patients') }}
group by plan_type
