select event_id, session_id, player_id,
    lower(trim(event_type)) as event_type,
    event_ts, level_id, value,
    lower(trim(event_type))='level_complete' as is_completion,
    lower(trim(event_type)) in ('level_fail','death') as is_failure
from {{ source('game','events') }}
