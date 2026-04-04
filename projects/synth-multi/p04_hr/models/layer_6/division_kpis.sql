select dss.dept_name, sum(dss.headcount) as headcount, sum(dss.total_comp_spend) as payroll,
    avg(dss.avg_base) as avg_base, avg(drs.avg_risk_score) as attrition_risk,
    sum(drs.high_risk) as high_risk_emps, gpg.f_m_ratio as pay_equity
from {{ ref('dept_salary_stats') }} dss
left join {{ ref('dept_risk_summary') }} drs on drs.dept_name=dss.dept_name
left join {{ ref('gender_pay_gaps') }} gpg on gpg.dept_name=dss.dept_name
group by dss.dept_name, gpg.f_m_ratio
