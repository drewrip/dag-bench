select session_id, player_id, session_start, session_end, platform, version, coins_earned,
    {{ datediff("session_start", "session_end", "second") }} as duration_sec,
    {{ date_trunc("day", "session_start") }} as session_day
from {{ source('game','sessions') }}
