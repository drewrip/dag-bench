select emp_id, sum(leave_days) as total_leave_days,
    sum(leave_days) filter (where leave_type='sick')   as sick_days,
    sum(leave_days) filter (where leave_type='annual') as annual_days,
    count(*) filter (where not approved) as unapproved
from {{ ref('stg_leaves') }}
group by emp_id
