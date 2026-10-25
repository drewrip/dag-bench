with hourly as (
    select r.device_id, d.device_type, date_trunc('hour', r.ts) as ts_hour,
        count(*)                as reading_count,
        avg(r.humidity_pct)     as avg_humidity,
        avg(r.pressure_hpa)     as avg_pressure,
        max(r.rssi_dbm)         as max_rssi_dbm
    from {{ source('iot', 'readings') }} r
    join {{ source('iot', 'devices') }} d on r.device_id = d.device_id
    group by r.device_id, d.device_type, date_trunc('hour', r.ts)
)
select device_id, device_type, ts_hour, reading_count, max_rssi_dbm,
    cast(round(cast(avg_humidity as numeric(18,5)), 3) as numeric(10,3)) as avg_humidity,
    cast(round(cast(avg_pressure as numeric(18,5)), 3) as numeric(10,3)) as avg_pressure
from hourly
where ts_hour >= timestamp '2023-04-10 00:00:00'
  and ts_hour <  timestamp '2023-04-17 00:00:00'
  and device_type <> 'gateway'
