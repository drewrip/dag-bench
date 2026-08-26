with hourly as (
    select device_id, ts_hour,
        count(*)                         as reading_count,
        count(*) filter (where is_valid) as valid_readings,
        round(cast(avg(temperature_c) filter (where is_valid) as numeric(38,10)), 3) as avg_temp,
        round(cast(min(temperature_c) filter (where is_valid) as numeric(38,10)), 3) as min_temp,
        round(cast(max(temperature_c) filter (where is_valid) as numeric(38,10)), 3) as max_temp,
        round(cast(avg(humidity_pct)  filter (where is_valid) as numeric(38,10)), 3) as avg_humidity,
        round(cast(avg(pressure_hpa)  filter (where is_valid) as numeric(38,10)), 3) as avg_pressure,
        min(battery_pct)                   as min_battery,
        count(*) filter (where error_flag) as error_count,
        round(cast(count(*) filter (where error_flag)*100.0
              /nullif(count(*),0) as numeric(38,10)), 2)          as error_rate_pct
    from {{ ref('stg_readings') }}
    group by device_id, ts_hour
)
select *,
    lag(avg_temp) over (partition by device_id order by ts_hour) as prev_hour_avg_temp
from hourly
