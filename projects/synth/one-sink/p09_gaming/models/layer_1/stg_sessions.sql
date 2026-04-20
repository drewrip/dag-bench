select session_id, player_id, session_start, session_end, platform, version,
    levels_attempted, coins_earned,
    {{ datediff("session_start", "session_end", "second") }} as session_duration_sec,
    {{ date_trunc("day", "session_start") }}                as session_day,
    EXTRACT('hour' FROM session_start)            as start_hour
from {{ source('game','sessions') }}
