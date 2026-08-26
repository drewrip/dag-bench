select tariff_class, meter_type, meters, avg_daily_kwh, total_kwh, avg_pf,
    round(cast(total_kwh*100.0/nullif(sum(total_kwh) over(partition by tariff_class),0) as numeric(38,10)), 2) as type_share_pct,
    current_timestamp as report_ts
from {{ ref('tariff_summary') }}
order by tariff_class, total_kwh desc nulls last
