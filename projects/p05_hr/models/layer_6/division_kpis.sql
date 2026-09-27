select dss.dept_name, cast(sum(dss.headcount) as bigint) as headcount, cast(sum(dss.total_comp_spend) as numeric(18,2)) as payroll,
    cast(avg(dss.avg_base) as numeric(38,6)) as avg_base, cast(avg(drs.avg_risk_score) as numeric(38,6)) as attrition_risk,
    cast(sum(drs.high_risk) as bigint) as high_risk_emps, gpg.f_m_ratio as pay_equity
from {{ ref('dept_salary_stats') }} dss
left join {{ ref('dept_risk_summary') }} drs on drs.dept_name=dss.dept_name
left join {{ ref('gender_pay_gaps') }} gpg on gpg.dept_name=dss.dept_name
group by dss.dept_name, gpg.f_m_ratio
