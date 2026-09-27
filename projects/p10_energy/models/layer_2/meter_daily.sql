select r.meter_id, r.read_day, m.meter_type, m.tariff_class, m.region, m.sub_id,
    cast(sum(r.kwh) filter (where r.is_valid) as numeric(18,4)) as daily_kwh,
    cast(max(r.kwh) as numeric(12,4)) as peak_reading,
    count(*) filter (where r.is_estimated) as estimated_readings,
    avg(r.voltage_v) as avg_voltage, avg(r.power_factor) as avg_pf,
    cast(sum(r.kwh) filter (where r.is_valid)
        - lag(sum(r.kwh) filter (where r.is_valid)) over (partition by r.meter_id order by r.read_day) as numeric(18,4)) as daily_kwh_delta
from {{ ref('stg_readings') }} r
join {{ ref('stg_meters') }} m using (meter_id)
group by r.meter_id, r.read_day, m.meter_type, m.tariff_class, m.region, m.sub_id
