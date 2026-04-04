select dept_name, perf_band, count(*) as emp_count,
    round(avg(base_salary),2) as avg_salary,
    round(avg(total_leave_days),1) as avg_leave_days
from {{ ref('employee_profile') }}
group by dept_name, perf_band
