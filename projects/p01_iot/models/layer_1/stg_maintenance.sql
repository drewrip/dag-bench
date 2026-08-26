select log_id, device_id, log_ts, action, technician,
    cast(date_trunc('month', log_ts) as date) as log_month
from {{ source('iot','maintenance_logs') }}
