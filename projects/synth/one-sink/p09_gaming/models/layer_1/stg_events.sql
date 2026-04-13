select event_id, session_id, player_id, event_type, event_ts,
    level_id, value,
    event_type in ('level_complete') as is_completion,
    event_type in ('level_fail','death') as is_failure
from {{ source('game','events') }}
