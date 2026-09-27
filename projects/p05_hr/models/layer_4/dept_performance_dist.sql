select dept_name, perf_band, count(*) as emp_count,
    cast(round(avg(base_salary), 2) as numeric(18,2)) as avg_salary,
    cast(round(avg(total_leave_days), 1) as numeric(10,1)) as avg_leave_days
from {{ ref('employee_profile') }}
where is_active
group by dept_name, perf_band
