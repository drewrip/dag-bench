with stats as (
    select meter_id, avg(avg_voltage) as mean_v, stddev(avg_voltage) as std_v
    from {{ ref('hourly_consumption') }} where avg_voltage is not null group by meter_id
)
select h.meter_id, h.read_hour, h.avg_voltage, s.mean_v, s.std_v,
    round(CAST((abs(h.avg_voltage-s.mean_v)/nullif(s.std_v,0)) AS NUMERIC),3) as z_score,
    abs(h.avg_voltage-s.mean_v)>2.5*nullif(s.std_v,0) as is_anomaly
from {{ ref('hourly_consumption') }} h
join stats s using (meter_id) where h.avg_voltage is not null
