select emp_id, full_name, dept_name, division, total_comp, avg_perf_score, tenure_years,
    rank() over (partition by division order by total_comp desc) as rank_in_division
from {{ ref('employee_profile') }}
where is_active
