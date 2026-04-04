select account_id, txn_day,
    count(*)                          as txns_per_day,
    sum(amount)                       as daily_spend,
    count(distinct merchant_id)       as distinct_merchants,
    count(*) filter (where is_flagged) as flagged_count,
    max(amount)                       as max_txn_amount,
    sum(amount) filter (where is_large) as large_txn_total
from {{ ref('stg_transactions') }}
group by account_id, txn_day
