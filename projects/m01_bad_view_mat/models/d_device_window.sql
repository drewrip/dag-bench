select device_id,
    max(device_type)                                                    as device_type,
    max(device_label)                                                   as device_label,
    count(*)                                                            as reading_count,
    min(ts)                                                             as first_ts,
    max(ts)                                                             as last_ts,
    cast(round(cast(avg(temperature_c) as numeric(18,5)), 3) as numeric(10,3)) as avg_temp,
    cast(round(cast(avg(humidity_pct) as numeric(18,5)), 3) as numeric(10,3))  as avg_humidity,
    cast(round(cast(avg(pressure_hpa) as numeric(18,5)), 3) as numeric(10,3))  as avg_pressure,
    count(*) filter (where error_flag)                                  as error_count
from {{ ref('a_readings_enriched') }}
where device_id between 100 and 199
group by device_id
