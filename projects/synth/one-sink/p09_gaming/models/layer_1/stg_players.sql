select player_id, username, country, platform, created_ts, age_group, is_paid_user,
    date_diff('day', created_ts::date, current_date) as account_age_days
from {{ source('game','players') }}
