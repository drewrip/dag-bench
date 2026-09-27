select region, anomaly_date, anomalies, affected_meters, avg_z,
    cast(sum(anomalies) over (partition by region order by anomaly_date
        rows unbounded preceding) as bigint) as cumulative_anomalies,
    current_timestamp as report_ts
from {{ ref('anomaly_summary') }}
order by region, anomaly_date
