select dept_name, division,
    count(distinct emp_id) as headcount,
    cast(round(avg(base_salary), 2) as numeric(18,2)) as avg_base,
    cast(round(stddev(base_salary), 2) as numeric(18,2)) as stddev_base,
    cast(round(sum(total_comp), 2) as numeric(18,2)) as total_comp_spend
from {{ ref('current_salary') }}
group by dept_name, division
