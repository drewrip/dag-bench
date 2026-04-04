select region, health_band,
    count(*) as device_count,
    round(avg(health_score),2) as avg_health_score,
    round(avg(avg_min_battery),1) as avg_min_battery,
    sum(total_valid) as total_valid_readings,
    current_timestamp as report_ts
from {{ ref('device_risk_ranking') }}
group by region, health_band
order by region, health_band
