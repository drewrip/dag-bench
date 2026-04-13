select dept_name, division,
    count(distinct emp_id) as headcount,
    round(avg(base_salary),2) as avg_base,
    round(median(base_salary),2) as median_base,
    round(stddev(base_salary),2) as stddev_base,
    round(sum(total_comp),2) as total_comp_spend
from {{ ref('current_salary') }}
group by dept_name, division
