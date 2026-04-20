select dept_name, division,
    count(distinct emp_id) as headcount,
    round(CAST(avg(base_salary) AS NUMERIC),2) as avg_base,
    round(CAST(stddev(base_salary) AS NUMERIC),2) as stddev_base,
    round(CAST(sum(total_comp) AS NUMERIC),2) as total_comp_spend
from {{ ref('current_salary') }}
group by dept_name, division
