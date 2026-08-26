with site_reliability as (
    select * from {{ ref('site_reliability') }}
),

device_risk_ranking as (
    select * from {{ ref('device_risk_ranking') }}
),

network_anomaly_summary as (
    select * from {{ ref('network_anomaly_summary') }}
),

anomalies_by_region as (
    select region, sum(anomaly_count) as anomaly_count
    from network_anomaly_summary
    group by region
),

final as (
    select sr.region, sr.site_id, sr.site_name,
        sr.days_with_data, sr.overall_avg_temp,
        sr.error_rate_pct as site_error_rate,
        nas.anomaly_count as recent_anomalies,
        count(drr.device_id) filter (where drr.health_band='CRITICAL') as critical_devices,
        count(drr.device_id) filter (where drr.health_band='GOOD') as healthy_devices,
        round(cast(avg(drr.health_score) as numeric(38,10)), 2)    as avg_device_health
    from site_reliability sr
    left join anomalies_by_region nas on nas.region = sr.region
    left join device_risk_ranking drr using (site_id)
    group by sr.region, sr.site_id, sr.site_name, sr.days_with_data,
             sr.overall_avg_temp, sr.error_rate_pct, nas.anomaly_count
    order by sr.region, sr.site_name
)

select * from final
