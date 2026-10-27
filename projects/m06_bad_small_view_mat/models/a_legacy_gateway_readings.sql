{{ config(materialized='view') }}
-- A very selective join: only the readings of legacy (v1 firmware) gateways, ~1/160 of all
-- readings. The device side of the join is tiny, so an inlined consumer streams readings
-- through a hash probe, and its own filter on a readings column is pushed into the scan.
-- Materializing this view means writing every column any consumer could need.
select r.reading_id, r.device_id, r.ts, r.temperature_c, r.humidity_pct,
    r.pressure_hpa, r.battery_pct, r.rssi_dbm, r.error_flag,
    d.site_id, d.model, d.firmware, d.installed_date, d.is_active,
    date_trunc('hour', r.ts)               as ts_hour,
    cast(date_trunc('day', r.ts) as date)  as ts_day,
    r.temperature_c * 9.0 / 5.0 + 32.0     as temp_f,
    r.battery_pct < 15                     as is_low_battery
from {{ source('iot', 'readings') }} r
join {{ source('iot', 'devices') }} d
  on r.device_id = d.device_id
 and d.device_type = 'gateway'
 and d.firmware like 'v1.%'
