select dept_name, division, location, active_hc, total_hc, female_count,
    round(cast(female_count*100.0/nullif(active_hc,0) as numeric(38,10)), 2) as female_pct,
    round(cast(avg_tenure as numeric(38,10)), 2) as avg_tenure, current_timestamp as report_ts
from {{ ref('dept_headcount') }}
order by division, dept_name
