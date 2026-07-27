with sessions_with_gap as (
    select *,
        lag(session_start) over (partition by player_id order by session_start) as prev_session_start
    from {{ ref('stg_sessions') }}
)

select player_id, count(distinct session_id) as sessions,
    sum(duration_sec) as total_sec, avg(duration_sec) as avg_sec,
    sum(coins_earned) as coins, count(distinct session_day) as active_days,
    max(session_start) as last_session,
    max(prev_session_start) as last_prev_session_start
from sessions_with_gap
group by player_id
