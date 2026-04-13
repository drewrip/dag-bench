select meter_id, read_hour,
    sum(kwh) filter (where is_valid) as valid_kwh,
    count(*) filter (where is_valid) as valid_readings,
    count(*) filter (where is_estimated) as estimated,
    avg(voltage_v) filter (where is_valid) as avg_voltage,
    avg(power_factor) filter (where is_valid) as avg_pf
from {{ ref('stg_readings') }}
group by meter_id, read_hour
