select dept_name, division,
    count(distinct emp_id) as headcount,
    round(cast(avg(base_salary) as numeric(38,10)), 2) as avg_base,
    round(cast(stddev(base_salary) as numeric(38,10)), 2) as stddev_base,
    round(cast(sum(total_comp) as numeric(38,10)), 2) as total_comp_spend
from {{ ref('current_salary') }}
group by dept_name, division
