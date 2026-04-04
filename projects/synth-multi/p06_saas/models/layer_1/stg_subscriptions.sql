select sub_id, account_id, plan, seats, mrr, start_date, end_date, is_active, mrr*12 as arr
from {{ source('saas','subscriptions') }}
