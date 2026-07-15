select dept_name, headcount, payroll, round(avg_base,2) as avg_base,
    round(attrition_risk,2) as attrition_risk, high_risk_emps,
    round(pay_equity,4) as pay_equity, current_timestamp as report_ts
from {{ ref('division_kpis') }}
order by payroll desc
