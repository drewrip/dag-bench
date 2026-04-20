select account_id, holder_name, account_type, country,
    credit_limit, opened_date, is_frozen,
    {{ datediff("opened_date", "current_date", "day") }} as account_age_days
from {{ source('src','accounts') }}
