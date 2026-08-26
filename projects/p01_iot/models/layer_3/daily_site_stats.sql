select d.site_id, d.site_name, d.region, h.ts_hour::DATE as stat_date,
    count(distinct h.device_id)                           as active_devices,
    round(cast(avg(h.avg_temp) as numeric(38,10)), 3)     as site_avg_temp,
    round(cast(min(h.min_temp) as numeric(38,10)), 3)     as site_min_temp,
    round(cast(max(h.max_temp) as numeric(38,10)), 3)     as site_max_temp,
    round(cast(avg(h.avg_humidity) as numeric(38,10)), 3) as site_avg_humidity,
    sum(h.error_count)                                    as total_errors,
    sum(h.valid_readings)                                 as total_valid_readings
from {{ ref('hourly_device_stats') }} h
join {{ ref('stg_devices') }} d using (device_id)
group by d.site_id, d.site_name, d.region, h.ts_hour::DATE
