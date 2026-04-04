select player_id, count(distinct session_id) as sessions,
    sum(duration_sec) as total_sec, avg(duration_sec) as avg_sec,
    sum(coins_earned) as coins, count(distinct session_day) as active_days,
    max(session_start) as last_session
from {{ ref('stg_sessions') }}
group by player_id
