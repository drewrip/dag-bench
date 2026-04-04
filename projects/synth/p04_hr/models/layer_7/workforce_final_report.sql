select dept_name, headcount, total_comp,
    round(avg_base_salary,2)    as avg_base_salary,
    round(avg_attrition_risk,2) as avg_attrition_risk,
    high_risk_employees,
    round(pay_equity_ratio,4)   as pay_equity_ratio,
    round(high_risk_employees*100.0/nullif(headcount,0),2) as pct_high_risk,
    current_timestamp           as report_ts
from {{ ref('hr_kpi_by_division') }}
order by total_comp desc
