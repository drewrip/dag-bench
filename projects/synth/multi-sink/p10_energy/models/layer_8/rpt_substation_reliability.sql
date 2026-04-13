select sub_id, sub_name, region, capacity_mw, period_kwh, avg_pf, outages, total_cml, days_with_data,
    round(period_kwh/nullif(capacity_mw,0),2) as kwh_per_mw, current_timestamp as report_ts
from {{ ref('substation_reliability') }}
order by region, sub_name
