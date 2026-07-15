select d.site_id, d.site_name, d.region, h.ts_hour::DATE as stat_date,
    count(distinct h.device_id)           as active_devices,
    round(avg(h.avg_temp),3)              as site_avg_temp,
    round(min(h.min_temp),3)              as site_min_temp,
    round(max(h.max_temp),3)              as site_max_temp,
    round(avg(h.avg_humidity),3)          as site_avg_humidity,
    sum(h.error_count)                    as total_errors,
    sum(h.valid_readings)                 as total_valid_readings
from {{ ref('hourly_device_stats') }} h
join {{ ref('stg_devices') }} d using (device_id)
group by d.site_id, d.site_name, d.region, h.ts_hour::DATE
