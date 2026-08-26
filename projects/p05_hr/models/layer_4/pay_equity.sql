select dept_name, gender, count(*) as headcount,
    round(cast(avg(base_salary) as numeric(38,10)), 2) as avg_salary,
    round(cast(avg(base_salary)/nullif(avg(avg(base_salary)) over (partition by dept_name),0) as numeric(38,10)), 4) as pay_index
from {{ ref('employee_profile') }}
where is_active
group by dept_name, gender
