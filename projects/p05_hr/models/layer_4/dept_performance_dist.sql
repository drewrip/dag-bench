select dept_name, perf_band, count(*) as emp_count,
    round(cast(avg(base_salary) as numeric(38,10)), 2) as avg_salary,
    round(cast(avg(total_leave_days) as numeric(38,10)), 1) as avg_leave_days
from {{ ref('employee_profile') }}
where is_active
group by dept_name, perf_band
