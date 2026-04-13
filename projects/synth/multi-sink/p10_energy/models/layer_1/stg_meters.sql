select m.meter_id, m.sub_id, m.customer_id, m.meter_type, m.tariff_class,
    m.install_date, m.is_smart, m.rated_capacity_kw,
    s.name as sub_name, s.region, s.capacity_mw, s.voltage_kv
from {{ source('grid','meters') }} m
join {{ source('grid','substations') }} s using (sub_id)
