select dept_name,
    max(case when gender='F' then avg_salary end) as female_avg,
    max(case when gender='M' then avg_salary end) as male_avg,
    round(max(case when gender='F' then avg_salary end)
        / nullif(max(case when gender='M' then avg_salary end),0), 4) as f_to_m_ratio
from {{ ref('pay_equity_analysis') }}
group by dept_name
