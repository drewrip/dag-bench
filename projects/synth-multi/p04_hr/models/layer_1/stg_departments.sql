select dept_id, name as dept_name, division, location, budget, headcount_target
from {{ source('hr','departments') }}
