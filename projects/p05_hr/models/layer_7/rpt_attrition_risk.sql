select dept_name, employees, high_risk, medium_risk,
    round(cast(high_risk*100.0/nullif(employees,0) as numeric(38,10)), 2) as pct_high_risk,
    round(cast(avg_risk_score as numeric(38,10)), 2) as avg_risk, current_timestamp as report_ts
from {{ ref('dept_risk_summary') }}
order by avg_risk_score desc nulls last
