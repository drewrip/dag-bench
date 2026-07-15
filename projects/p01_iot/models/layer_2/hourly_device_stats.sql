select device_id, ts_hour,
    count(*)                              as reading_count,
    count(*) filter (where is_valid)      as valid_readings,
    round(CAST((avg(temperature_c) filter (where is_valid)) AS NUMERIC), 3) as avg_temp,
    round(CAST((min(temperature_c) filter (where is_valid)) AS NUMERIC), 3) as min_temp,
    round(CAST((max(temperature_c) filter (where is_valid)) AS NUMERIC), 3) as max_temp,
    round(CAST((avg(humidity_pct)  filter (where is_valid)) AS NUMERIC), 3) as avg_humidity,
    round(CAST((avg(pressure_hpa)  filter (where is_valid)) AS NUMERIC), 3) as avg_pressure,
    min(battery_pct)                      as min_battery,
    count(*) filter (where error_flag)    as error_count,
    round(CAST((count(*) filter (where error_flag)*100.0
          /nullif(count(*),0)) AS NUMERIC), 2)          as error_rate_pct
from {{ ref('stg_readings') }}
group by device_id, ts_hour
