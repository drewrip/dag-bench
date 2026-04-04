select region, total_kwh, round(avg_pf,4) as avg_pf, total_outages, total_cml,
    latest_month_kwh, current_timestamp as report_ts
from {{ ref('grid_kpis') }}
order by total_kwh desc
