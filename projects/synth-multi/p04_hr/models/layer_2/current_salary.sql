select e.emp_id, e.full_name, e.dept_name, e.division, e.location,
    e.gender, e.employment_type, e.tenure_years, e.is_active,
    s.base_salary, s.bonus, s.total_comp
from {{ ref('stg_employees') }} e
join (select * from {{ ref('stg_salaries') }} where recency_rank=1) s using (emp_id)
