select outage_id, sub_id, start_ts, end_ts, cause, affected_meters, severity,
    date_diff('minute', start_ts, end_ts)  as duration_min,
    affected_meters * date_diff('minute', start_ts, end_ts) as customer_minutes_lost,
    severity in ('major','critical')       as is_major
from {{ source('grid','outage_events') }}
