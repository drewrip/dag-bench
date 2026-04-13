select gk.region,
    gk.total_kwh, gk.avg_power_factor,
    gk.total_outages, gk.avg_availability_pct,
    tc.tariff_class,
    tc.avg_daily_kwh as tariff_avg_daily_kwh,
    vas.anomaly_count as voltage_anomalies
from {{ ref('grid_kpis') }} gk
left join {{ ref('tariff_consumption') }} tc on true
left join (
    select region, sum(anomaly_count) as anomaly_count
    from {{ ref('voltage_anomaly_summary') }}
    group by region
) vas on vas.region = gk.region
