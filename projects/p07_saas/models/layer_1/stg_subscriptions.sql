select sub_id, account_id, plan, seats, mrr, start_date, end_date, is_active, cast(mrr*12 as numeric(12,2)) as arr
from {{ source('saas','subscriptions') }}
