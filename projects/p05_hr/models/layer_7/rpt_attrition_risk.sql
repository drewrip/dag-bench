select dept_name, employees, high_risk, medium_risk,
    cast(round(high_risk*100.0/nullif(employees,0), 2) as numeric(10,2)) as pct_high_risk,
    cast(round(avg_risk_score, 2) as numeric(10,2)) as avg_risk, current_timestamp as report_ts
from {{ ref('dept_risk_summary') }}
order by avg_risk_score desc nulls last
