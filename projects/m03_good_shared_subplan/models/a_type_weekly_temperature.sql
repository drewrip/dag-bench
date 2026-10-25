with weekly as (
    select d.device_type,
        cast(date_trunc('week', r.ts) as date) as week_start,
        count(*)                               as reading_count,
        avg(r.temperature_c)                   as avg_temp
    from {{ source('iot', 'readings') }} r
    join (
        select device_id, device_type
        from {{ source('iot', 'devices') }}
        where is_active
    ) d on r.device_id = d.device_id
    where not r.error_flag
    group by d.device_type, cast(date_trunc('week', r.ts) as date)
)
select device_type, week_start, reading_count,
    cast(round(cast(avg_temp as numeric(18,5)), 3) as numeric(10,3)) as avg_temp,
    cast(round(cast(avg_temp - lag(avg_temp) over (partition by device_type order by week_start)
        as numeric(18,5)), 3) as numeric(10,3))                     as wow_temp_change
from weekly
