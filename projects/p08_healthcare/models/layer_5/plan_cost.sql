select plan_type, count(distinct patient_id) as members,
    cast(sum(total_paid) as numeric(18,2)) as paid, cast(round(avg(total_paid), 2) as numeric(18,2)) as avg_paid,
    count(*) filter (where is_high_cost) as high_cost_members,
    cast(round(avg(cost_decile), 2) as numeric(10,2)) as avg_decile
from {{ ref('high_cost_patients') }}
group by plan_type
