select region, total_kwh, round(cast(avg_pf as numeric(38,10)), 4) as avg_pf, total_outages, total_cml,
    latest_month_kwh, current_timestamp as report_ts
from {{ ref('grid_kpis') }}
order by total_kwh desc nulls last
