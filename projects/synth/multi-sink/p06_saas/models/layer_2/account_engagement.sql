select account_id, count(distinct event_id) as total_events,
    count(distinct user_id) as unique_users, count(distinct session_id) as sessions,
    count(distinct event_day) as active_days, max(event_ts) as last_seen
from {{ ref('stg_events') }}
group by account_id
