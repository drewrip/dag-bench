select account_id,
    count(distinct event_id)          as total_events,
    count(distinct user_id)           as dau_avg,
    count(distinct session_id)        as total_sessions,
    count(distinct event_day)         as active_days,
    count(distinct event_type)        as feature_breadth,
    max(event_ts)                     as last_seen_ts
from {{ ref('stg_events') }}
group by account_id
