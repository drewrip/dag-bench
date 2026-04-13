select o.*, s.region, s.capacity_mw, s.voltage_kv,
    o.customer_minutes_lost / 60.0 as customer_hours_lost,
    o.duration_min / 60.0          as duration_hours
from {{ ref('stg_outages') }} o
join {{ ref('stg_substations') }} s using (sub_id)
