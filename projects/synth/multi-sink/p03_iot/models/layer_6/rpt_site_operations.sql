select sr.region, sr.site_id, sr.site_name, sr.days_with_data,
    sr.overall_avg_temp, sr.error_rate_pct,
    count(drr.device_id) filter (where drr.health_band='CRITICAL') as critical_devices,
    count(drr.device_id) filter (where drr.health_band='GOOD') as good_devices,
    round(avg(drr.health_score),2) as avg_device_health,
    current_timestamp as report_ts
from {{ ref('site_reliability') }} sr
left join {{ ref('device_risk_ranking') }} drr using (site_id)
group by sr.region, sr.site_id, sr.site_name, sr.days_with_data, sr.overall_avg_temp, sr.error_rate_pct
order by sr.region, sr.site_name
