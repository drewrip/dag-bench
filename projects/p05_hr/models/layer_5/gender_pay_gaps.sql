select dept_name,
    max(case when gender='female' then avg_salary end) as female_avg,
    max(case when gender='male' then avg_salary end) as male_avg,
    round(cast(max(case when gender='female' then avg_salary end)
        /nullif(max(case when gender='male' then avg_salary end),0) as numeric(38,10)), 4) as f_m_ratio
from {{ ref('pay_equity') }}
group by dept_name
