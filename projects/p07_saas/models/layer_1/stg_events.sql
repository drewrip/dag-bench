select event_id, account_id, user_id, event_type, event_ts, session_id, platform,
    cast(date_trunc('day',event_ts) as date) as event_day, cast(date_trunc('week',event_ts) as date) as event_week
from {{ source('saas','events') }}
