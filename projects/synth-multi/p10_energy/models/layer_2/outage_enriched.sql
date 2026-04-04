select o.*, s.region, s.capacity_mw,
    o.cml/60.0 as customer_hours_lost, o.duration_min/60.0 as duration_hrs
from {{ ref('stg_outages') }} o
join {{ ref('stg_substations') }} s using (sub_id)
