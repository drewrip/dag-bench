{{ config(materialized='view') }}
-- A wide, non-reducing enrichment of every reading. Each consumer only needs a
-- tiny, selectively filtered slice, so materializing this view is a bad choice.
select r.reading_id, r.device_id, r.ts, r.temperature_c, r.humidity_pct,
    r.pressure_hpa, r.battery_pct, r.rssi_dbm, r.error_flag,
    d.site_id, d.device_type, d.model, d.firmware, d.installed_date, d.is_active,
    date_trunc('hour', r.ts)               as ts_hour,
    cast(date_trunc('day', r.ts) as date)  as ts_day,
    r.temperature_c * 9.0 / 5.0 + 32.0     as temp_f,
    d.model || '-' || d.firmware           as device_label,
    r.battery_pct < 15                     as is_low_battery
from {{ source('iot', 'readings') }} r
join {{ source('iot', 'devices') }} d on r.device_id = d.device_id
