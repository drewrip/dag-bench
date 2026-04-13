select dept_name, employees, high_risk, medium_risk,
    round(high_risk*100.0/nullif(employees,0),2) as pct_high_risk,
    round(avg_risk_score,2) as avg_risk, current_timestamp as report_ts
from {{ ref('dept_risk_summary') }}
order by avg_risk_score desc
