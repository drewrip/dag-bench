select salary_id, emp_id, effective_date, base_salary, bonus, currency,
    base_salary + coalesce(bonus,0) as total_comp,
    row_number() over (partition by emp_id order by effective_date desc) as recency_rank
from {{ source('hr','salaries') }}
