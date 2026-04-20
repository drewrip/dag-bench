select dept_name, division,
    count(distinct emp_id)        as employee_count,
    round(CAST(avg(base_salary)AS NUMERIC),2)     as avg_base,
    round(CAST(min(base_salary)AS NUMERIC),2)     as min_base,
    round(CAST(max(base_salary)AS NUMERIC),2)     as max_base,
    round(CAST(stddev(base_salary)AS NUMERIC),2)  as stddev_base,
    round(CAST(sum(total_comp) AS NUMERIC),2)      as total_comp_spend
from {{ ref('employee_current_salary') }}
group by dept_name, division
