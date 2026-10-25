with hourly as (
    select r.device_id, d.device_type, date_trunc('hour', r.ts) as ts_hour,
        count(*)                           as reading_count,
        avg(r.temperature_c)               as avg_temp,
        min(r.battery_pct)                 as min_battery,
        count(*) filter (where r.error_flag) as error_count
    from {{ source('iot', 'readings') }} r
    join {{ source('iot', 'devices') }} d on r.device_id = d.device_id
    group by r.device_id, d.device_type, date_trunc('hour', r.ts)
)
select device_id, device_type, ts_hour, reading_count, min_battery, error_count,
    cast(round(cast(avg_temp as numeric(18,5)), 3) as numeric(10,3)) as avg_temp
from hourly
where device_id between 1000 and 1099
