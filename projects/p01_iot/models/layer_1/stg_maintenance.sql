select log_id, device_id, log_ts, action, technician,
    date_trunc('month', log_ts) as log_month
from {{ source('iot','maintenance_logs') }}
