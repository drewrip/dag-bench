select event_id, account_id, user_id, event_type, event_ts,
    session_id, platform,
    date_trunc('day',  event_ts) as event_day,
    date_trunc('week', event_ts) as event_week,
    extract('hour' from event_ts) as hour_of_day
from {{ source('saas','events') }}
