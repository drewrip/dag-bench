select date_trunc('month', start_date) as month,
    plan,
    count(distinct account_id) as new_accounts,
    sum(mrr) as new_mrr,
    sum(arr) as new_arr
from {{ ref('stg_subscriptions') }}
group by date_trunc('month', start_date), plan
