select e.emp_id, e.dept_id, e.manager_id,
    (upper(substr(trim(e.first_name),1,1))||lower(substr(trim(e.first_name),2)))
        ||' '||
    (upper(substr(trim(e.last_name),1,1))||lower(substr(trim(e.last_name),2))) as full_name,
    e.gender, e.hire_date, e.job_title, e.employment_type, e.is_active,
    {{ datediff("e.hire_date", "current_date", "year") }} as tenure_years,
    d.name as dept_name, d.division, d.location
from {{ source('hr','employees') }} e
join {{ source('hr','departments') }} d using (dept_id)
