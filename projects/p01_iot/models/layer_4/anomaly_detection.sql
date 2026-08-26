with stats as (
    select device_id,
        avg(avg_temp)    as mean_temp,
        stddev(avg_temp) as std_temp
    from {{ ref('hourly_device_stats') }}
    where avg_temp is not null
    group by device_id
)
select h.device_id, h.ts_hour, h.avg_temp, s.mean_temp, s.std_temp,
    round(abs(h.avg_temp - s.mean_temp)/nullif(s.std_temp,0), 3) as z_score,
    abs(h.avg_temp - s.mean_temp) > 3*nullif(s.std_temp,0)       as is_anomaly
from {{ ref('hourly_device_stats') }} h
join stats s using (device_id)
