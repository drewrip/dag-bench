select account_id, txn_day,
    count(*) as txns_per_day, cast(sum(amount) as numeric(18,2)) as daily_spend,
    count(distinct merchant_id) as distinct_merchants,
    count(*) filter (where is_flagged) as flagged_count,
    cast(max(amount) as numeric(12,2)) as max_txn
from {{ ref('stg_transactions') }}
group by account_id, txn_day
