select dept_name, headcount, payroll, round(cast(avg_base as numeric(38,10)), 2) as avg_base,
    round(cast(attrition_risk as numeric(38,10)), 2) as attrition_risk, high_risk_emps,
    round(cast(pay_equity as numeric(38,10)), 4) as pay_equity, current_timestamp as report_ts
from {{ ref('division_kpis') }}
order by payroll desc nulls last
