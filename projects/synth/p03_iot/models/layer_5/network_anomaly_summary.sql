select d.region, ad.ts_hour::DATE as anomaly_date,
    count(*)                           as anomaly_count,
    count(distinct ad.device_id)       as affected_devices,
    round(avg(ad.z_score),2)           as avg_z_score,
    round(max(ad.z_score),2)           as max_z_score
from {{ ref('anomaly_detection') }} ad
join {{ ref('stg_devices') }} d using (device_id)
where ad.is_anomaly
group by d.region, ad.ts_hour::DATE
