select gk.region, gk.total_kwh, gk.avg_pf, gk.total_outages, gk.total_cml,
    ts.tariff_class, ts.avg_daily_kwh,
    coalesce(asumm.anomalies,0) as voltage_anomalies
from {{ ref('grid_kpis') }} gk
left join {{ ref('tariff_summary') }} ts on true
left join (select region, sum(anomalies) as anomalies
           from {{ ref('anomaly_summary') }} group by region) asumm on asumm.region=gk.region
