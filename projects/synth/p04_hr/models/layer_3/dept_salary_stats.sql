select dept_name, division,
    count(distinct emp_id)        as employee_count,
    round(avg(base_salary),2)     as avg_base,
    round(median(base_salary),2)  as median_base,
    round(min(base_salary),2)     as min_base,
    round(max(base_salary),2)     as max_base,
    round(stddev(base_salary),2)  as stddev_base,
    round(sum(total_comp),2)      as total_comp_spend
from {{ ref('employee_current_salary') }}
group by dept_name, division
