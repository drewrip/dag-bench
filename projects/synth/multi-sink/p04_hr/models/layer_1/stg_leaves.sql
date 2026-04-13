select leave_id, emp_id, leave_type, start_date, end_date, approved,
    date_diff('day',start_date,end_date) as leave_days
from {{ source('hr','leave_requests') }}
