{{ config(materialized='view') }}
-- An expensive (sort-based window over every reading + distinct aggregate) but tiny
-- (one row per device) profile. The device attributes are functionally dependent on
-- device_id; repeating them in the partition / group keys lets each consumer's filter
-- on them be pushed below the window and aggregate, so no executed consumer plan
-- contains this view's subplan unchanged.
with ordered as (
    select r.device_id, d.device_type, d.model, d.firmware, d.installed_date, d.is_active,
        r.ts, r.temperature_c, r.humidity_pct, r.pressure_hpa, r.error_flag,
        lag(r.ts) over (
            partition by r.device_id, d.device_type, d.model, d.firmware, d.installed_date, d.is_active
            order by r.ts, r.reading_id
        ) as prev_ts
    from {{ source('iot', 'readings') }} r
    join {{ source('iot', 'devices') }} d on r.device_id = d.device_id
)
select device_id, device_type, model, firmware, installed_date, is_active,
    count(*)                                                                    as reading_count,
    count(distinct date_trunc('hour', ts))                                      as active_hours,
    count(*) filter (where error_flag)                                          as error_count,
    cast(round(cast(avg(temperature_c) as numeric(18,5)), 3) as numeric(10,3))  as avg_temp,
    cast(round(cast(stddev_samp(temperature_c) as numeric(18,5)), 3) as numeric(10,3)) as stddev_temp,
    cast(round(cast(min(temperature_c) as numeric(18,5)), 3) as numeric(10,3))  as min_temp,
    cast(round(cast(max(temperature_c) as numeric(18,5)), 3) as numeric(10,3))  as max_temp,
    cast(round(cast(avg(humidity_pct) as numeric(18,5)), 3) as numeric(10,3))   as avg_humidity,
    cast(round(cast(stddev_samp(humidity_pct) as numeric(18,5)), 3) as numeric(10,3)) as stddev_humidity,
    cast(round(cast(avg(pressure_hpa) as numeric(18,5)), 3) as numeric(10,3))   as avg_pressure,
    cast(round(cast(stddev_samp(pressure_hpa) as numeric(18,5)), 3) as numeric(10,3)) as stddev_pressure,
    cast(max(extract(epoch from ts - prev_ts)) as bigint)                       as max_gap_seconds,
    cast(round(cast(avg(extract(epoch from ts - prev_ts)) as numeric(18,5)), 1) as numeric(12,1)) as avg_gap_seconds
from ordered
group by device_id, device_type, model, firmware, installed_date, is_active
