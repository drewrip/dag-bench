select dept_name, gender,
    count(*)                      as headcount,
    round(CAST((avg(base_salary)) AS NUMERIC),2)     as avg_salary,
    round(CAST((avg(base_salary) / nullif(
        avg(avg(base_salary)) over (partition by dept_name), 0
    )) AS NUMERIC), 4)                         as relative_pay_index
from {{ ref('employee_full_profile') }}
where is_active
group by dept_name, gender
