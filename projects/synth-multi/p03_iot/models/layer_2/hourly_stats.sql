select device_id, ts_hour,
    count(*) as reading_count,
    count(*) filter (where is_valid) as valid_count,
    round(avg(temperature_c) filter (where is_valid),3) as avg_temp,
    round(min(temperature_c) filter (where is_valid),3) as min_temp,
    round(max(temperature_c) filter (where is_valid),3) as max_temp,
    round(avg(humidity_pct)  filter (where is_valid),3) as avg_humidity,
    round(avg(pressure_hpa)  filter (where is_valid),3) as avg_pressure,
    min(battery_pct) as min_battery,
    count(*) filter (where error_flag) as errors
from {{ ref('stg_readings') }}
group by device_id, ts_hour
