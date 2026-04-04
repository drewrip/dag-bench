select d.region, ta.ts_hour::DATE as anomaly_date,
    count(*) filter (where ta.is_anomaly) as anomaly_count,
    count(distinct ta.device_id) filter (where ta.is_anomaly) as affected_devices,
    round(max(ta.z_score),2) as max_z_score
from {{ ref('temp_anomalies') }} ta
join {{ ref('stg_devices') }} d using (device_id)
group by d.region, ta.ts_hour::DATE
