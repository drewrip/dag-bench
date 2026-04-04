select sr.region, sr.site_id, sr.site_name,
    sr.days_with_data, sr.overall_avg_temp,
    sr.error_rate_pct        as site_error_rate,
    nas.anomaly_count        as recent_anomalies,
    count(drr.device_id) filter (where drr.health_band='CRITICAL') as critical_devices,
    count(drr.device_id) filter (where drr.health_band='GOOD')     as healthy_devices,
    round(avg(drr.health_score),2)                                 as avg_device_health
from {{ ref('site_reliability') }} sr
left join (
    select region, sum(anomaly_count) as anomaly_count
    from {{ ref('network_anomaly_summary') }}
    group by region
) nas on nas.region = sr.region
left join {{ ref('device_risk_ranking') }} drr using (site_id)
group by sr.region, sr.site_id, sr.site_name, sr.days_with_data,
         sr.overall_avg_temp, sr.error_rate_pct, nas.anomaly_count
order by sr.region, sr.site_name
