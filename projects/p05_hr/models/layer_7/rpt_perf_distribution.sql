select dept_name, perf_band, emp_count, avg_salary, avg_leave_days,
    round(emp_count*100.0/sum(emp_count) over (partition by dept_name),2) as pct_of_dept,
    current_timestamp as report_ts
from {{ ref('dept_performance_dist') }}
order by dept_name, perf_band
