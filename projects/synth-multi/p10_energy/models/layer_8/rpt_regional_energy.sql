select region, tariff_class, total_kwh, avg_pf, total_outages, total_cml,
    avg_daily_kwh, voltage_anomalies, current_timestamp as report_ts
from {{ ref('regional_summary') }}
order by region, tariff_class
