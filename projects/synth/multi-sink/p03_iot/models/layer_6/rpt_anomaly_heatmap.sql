select region, anomaly_date, anomaly_count, affected_devices, max_z_score,
    sum(anomaly_count) over (partition by region order by anomaly_date
        rows unbounded preceding) as cumulative_anomalies,
    current_timestamp as report_ts
from {{ ref('anomaly_summary') }}
order by region, anomaly_date
