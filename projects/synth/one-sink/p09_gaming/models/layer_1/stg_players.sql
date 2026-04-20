select player_id, username, country, platform, created_ts, age_group, is_paid_user,
    {{ datediff("created_ts::date", "current_date", "day") }} as account_age_days
from {{ source('game','players') }}
