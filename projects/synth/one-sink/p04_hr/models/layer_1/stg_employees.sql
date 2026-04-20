select e.emp_id, e.dept_id, e.manager_id, e.first_name, e.last_name,
    e.first_name||' '||e.last_name as full_name,
    e.gender, e.hire_date, e.job_title, e.employment_type, e.is_active,
    {{ datediff("e.hire_date", "current_date", "year") }} as tenure_years,
    d.name as dept_name, d.division, d.location
from {{ source('hr','employees') }} e
join {{ source('hr','departments') }} d using (dept_id)
