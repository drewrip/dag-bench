select txn_id, account_id, merchant_id, amount, txn_ts, channel,
    currency, is_declined, is_flagged, response_code,
    date_trunc('hour', txn_ts)  as txn_hour,
    date_trunc('day',  txn_ts)  as txn_day,
    extract('hour' from txn_ts) as hour_of_day,
    extract('dow'  from txn_ts) as day_of_week,
    amount > 1000               as is_large
from {{ source('src','transactions') }}
