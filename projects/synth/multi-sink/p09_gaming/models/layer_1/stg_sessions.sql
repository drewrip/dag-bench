select session_id, player_id, session_start, session_end, platform, version, coins_earned,
    date_diff('second',session_start,session_end) as duration_sec,
    date_trunc('day',session_start) as session_day
from {{ source('game','sessions') }}
