select session_id, player_id, session_start, session_end, platform, version,
    levels_attempted, coins_earned,
    date_diff('second', session_start, session_end) as session_duration_sec,
    date_trunc('day', session_start)                as session_day,
    extract('hour' from session_start)              as start_hour
from {{ source('game','sessions') }}
