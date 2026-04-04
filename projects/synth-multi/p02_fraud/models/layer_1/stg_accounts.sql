select account_id, holder_name, account_type, country, credit_limit, opened_date, is_frozen,
    date_diff('day',opened_date,current_date) as account_age_days
from {{ source('src','accounts') }}
