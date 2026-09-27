select dept_name, headcount, payroll, cast(round(avg_base, 2) as numeric(18,2)) as avg_base,
    cast(round(attrition_risk, 2) as numeric(10,2)) as attrition_risk, high_risk_emps,
    cast(round(pay_equity, 4) as numeric(10,4)) as pay_equity, current_timestamp as report_ts
from {{ ref('division_kpis') }}
order by payroll desc nulls last
