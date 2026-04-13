select dept_name, gender, count(*) as headcount,
    round(avg(base_salary),2) as avg_salary,
    round(avg(base_salary)/nullif(avg(avg(base_salary)) over (partition by dept_name),0),4) as pay_index
from {{ ref('employee_profile') }}
where is_active
group by dept_name, gender
