select r.meter_id, r.read_day,
    sum(r.kwh) filter (where r.is_valid_actual)  as daily_kwh,
    max(r.kwh)                                   as peak_reading,
    count(*) filter (where r.is_estimated)       as estimated_readings,
    avg(r.voltage_v)                             as avg_voltage,
    avg(r.power_factor)                          as avg_power_factor,
    m.meter_type, m.tariff_class, m.region, m.sub_id
from {{ ref('stg_readings') }} r
join {{ ref('stg_meters') }} m using (meter_id)
group by r.meter_id, r.read_day, m.meter_type, m.tariff_class, m.region, m.sub_id
