select region, tariff_class,
    round(total_kwh,0)            as total_kwh,
    round(avg_power_factor,4)     as avg_pf,
    total_outages,
    round(avg_availability_pct,4) as availability_pct,
    round(tariff_avg_daily_kwh,3) as avg_daily_kwh_per_meter,
    coalesce(voltage_anomalies,0) as voltage_anomalies,
    current_timestamp             as report_ts
from {{ ref('regional_energy_summary') }}
order by region, tariff_class
