with events_bounded as (
    select event_id, account_id, user_id, session_id, event_ts, event_day,
        max(event_ts) over () as max_event_ts
    from {{ ref('stg_events') }}
),

events_recent as (
    select event_id, account_id, user_id, session_id, event_ts, event_day
    from events_bounded
    where event_ts >= max_event_ts - interval '90 days'
),

events_with_lag as (
    select *,
        lag(event_ts) over (partition by account_id order by event_ts) as prev_event_ts
    from events_recent
)

select account_id, count(distinct event_id) as total_events,
    count(distinct user_id) as unique_users, count(distinct session_id) as sessions,
    count(distinct event_day) as active_days, max(event_ts) as last_seen,
    round(avg({{ datediff('prev_event_ts', 'event_ts', 'day') }}),2) as avg_days_between_events
from events_with_lag
group by account_id
