{{ config(materialized='view') }}
-- An expensive (distinct aggregates + sort-based percentiles over every reading) but small
-- (one row per site-week, ~250x fewer rows than readings) profile. Consumers filter only on
-- aggregate outputs, which cannot be pushed below the group by, so each inlined consumer
-- repeats the full aggregate.
select d.site_id,
    cast(date_trunc('week', r.ts) as date)                                       as ts_week,
    count(*)                                                                     as reading_count,
    count(distinct r.device_id)                                                  as active_devices,
    count(distinct date_trunc('hour', r.ts))                                     as active_hours,
    count(*) filter (where r.error_flag)                                         as error_count,
    min(r.battery_pct)                                                           as min_battery,
    cast(round(cast(avg(r.temperature_c) as numeric(18,5)), 3) as numeric(10,3)) as avg_temp,
    cast(round(cast(percentile_cont(0.5) within group (order by r.temperature_c) as numeric(18,5)), 3) as numeric(10,3)) as median_temp,
    cast(round(cast(percentile_cont(0.95) within group (order by r.temperature_c) as numeric(18,5)), 3) as numeric(10,3)) as p95_temp,
    cast(round(cast(stddev_samp(r.temperature_c) as numeric(18,5)), 3) as numeric(10,3)) as stddev_temp,
    cast(round(cast(stddev_samp(r.humidity_pct) as numeric(18,5)), 3) as numeric(10,3))  as stddev_humidity,
    cast(round(cast(stddev_samp(r.pressure_hpa) as numeric(18,5)), 3) as numeric(10,3))  as stddev_pressure
from {{ source('iot', 'readings') }} r
join {{ source('iot', 'devices') }} d on r.device_id = d.device_id
group by d.site_id, cast(date_trunc('week', r.ts) as date)
