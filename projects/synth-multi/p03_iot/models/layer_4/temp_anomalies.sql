with stats as (
    select device_id, avg(avg_temp) as mean_t, stddev(avg_temp) as std_t
    from {{ ref('hourly_stats') }} where avg_temp is not null group by device_id
)
select h.device_id, h.ts_hour, h.avg_temp, s.mean_t, s.std_t,
    round(abs(h.avg_temp-s.mean_t)/nullif(s.std_t,0),3) as z_score,
    abs(h.avg_temp-s.mean_t)>3*nullif(s.std_t,0) as is_anomaly
from {{ ref('hourly_stats') }} h
join stats s using (device_id)
