select dept_name, division, location, active_hc, total_hc, female_count,
    round(female_count*100.0/nullif(active_hc,0),2) as female_pct,
    round(avg_tenure,2) as avg_tenure, current_timestamp as report_ts
from {{ ref('dept_headcount') }}
order by division, dept_name
