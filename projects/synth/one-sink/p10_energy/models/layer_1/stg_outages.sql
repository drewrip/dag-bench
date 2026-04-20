select outage_id, sub_id, start_ts, end_ts, cause, affected_meters, severity,
    {{ datediff("start_ts", "end_ts", "minute") }}  as duration_min,
    affected_meters * {{ datediff("start_ts", "end_ts", "minute") }} as customer_minutes_lost,
    severity in ('major','critical')       as is_major
from {{ source('grid','outage_events') }}
