select cs.emp_id, cs.full_name, cs.dept_name, cs.division, cs.location,
    cs.gender, cs.employment_type, cs.tenure_years, cs.is_active,
    cs.base_salary, cs.total_comp,
    coalesce(ra.avg_score,0)      as avg_perf_score,
    coalesce(ra.review_count,0)   as review_count,
    coalesce(la.total_leave_days,0) as total_leave_days,
    coalesce(la.sick_days,0)      as sick_days,
    case
        when ra.avg_score >= 4.0 then 'High Performer'
        when ra.avg_score >= 2.5 then 'Meets Expectations'
        else 'Needs Improvement'
    end as performance_band
from {{ ref('employee_current_salary') }} cs
left join {{ ref('employee_review_agg') }}  ra using (emp_id)
left join {{ ref('employee_leave_agg') }}   la using (emp_id)
