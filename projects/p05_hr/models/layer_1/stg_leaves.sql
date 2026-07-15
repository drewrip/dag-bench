select leave_id, emp_id, leave_type, start_date, end_date, approved,
    {{ datediff("start_date", "end_date", "day") }} as leave_days
from {{ source('hr','leave_requests') }}
