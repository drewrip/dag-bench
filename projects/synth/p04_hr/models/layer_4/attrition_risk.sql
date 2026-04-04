select emp_id, full_name, dept_name, tenure_years, total_comp,
    avg_perf_score, sick_days, total_leave_days,
    -- simple additive risk model
    (case when avg_perf_score < 2.5 then 3 else 0 end
     + case when sick_days > 15 then 2 else 0 end
     + case when tenure_years < 1 then 2 else 0 end
     + case when tenure_years between 1 and 2 then 1 else 0 end
    )                             as risk_score,
    case
        when avg_perf_score < 2.5 or sick_days > 15 then 'HIGH'
        when tenure_years < 2 then 'MEDIUM'
        else 'LOW'
    end as attrition_risk
from {{ ref('employee_full_profile') }}
where is_active
