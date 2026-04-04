select player_id,
    count(distinct session_id)                    as total_sessions,
    sum(session_duration_sec)                     as total_playtime_sec,
    avg(session_duration_sec)                     as avg_session_sec,
    max(session_duration_sec)                     as longest_session_sec,
    sum(coins_earned)                             as total_coins_earned,
    count(distinct session_day)                   as active_days,
    max(session_start)                            as last_session_ts,
    sum(levels_attempted)                         as total_levels_attempted
from {{ ref('stg_sessions') }}
group by player_id
