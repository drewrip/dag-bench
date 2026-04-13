select dss.dept_name,
    sum(dss.employee_count)   as headcount,
    sum(dss.total_comp_spend) as total_comp,
    avg(dss.avg_base)         as avg_base_salary,
    avg(drs.avg_risk_score)   as avg_attrition_risk,
    sum(drs.high_risk_count)  as high_risk_employees,
    gps.f_to_m_ratio          as pay_equity_ratio
from {{ ref('dept_salary_stats') }} dss
left join {{ ref('dept_risk_summary') }} drs on drs.dept_name = dss.dept_name
left join {{ ref('gender_pay_summary') }} gps on gps.dept_name = dss.dept_name
group by dss.dept_name, gps.f_to_m_ratio
