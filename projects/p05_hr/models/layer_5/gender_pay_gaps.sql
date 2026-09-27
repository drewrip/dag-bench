select dept_name,
    cast(max(case when gender='female' then avg_salary end) as numeric(18,2)) as female_avg,
    cast(max(case when gender='male' then avg_salary end) as numeric(18,2)) as male_avg,
    cast(round(max(case when gender='female' then avg_salary end)
        /nullif(max(case when gender='male' then avg_salary end),0), 4) as numeric(10,4)) as f_m_ratio
from {{ ref('pay_equity') }}
group by dept_name
