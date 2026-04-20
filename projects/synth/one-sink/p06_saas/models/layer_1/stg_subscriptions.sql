select sub_id, account_id, plan, seats, mrr, start_date, end_date,
    is_active, renewal_date,
    {{ datediff("start_date", "coalesce(end_date, current_date)", "day") }} as sub_days,
    mrr * 12 as arr
from {{ source('saas','subscriptions') }}
