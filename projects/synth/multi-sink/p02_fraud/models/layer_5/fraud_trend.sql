select txn_day,
    sum(flagged) as daily_flagged, sum(confirmed_fraud) as daily_confirmed,
    sum(volume) as daily_volume,
    lag(sum(flagged)) over (order by txn_day) as prev_day_flagged,
    sum(sum(confirmed_fraud)) over (order by txn_day rows unbounded preceding) as cumulative_fraud
from {{ ref('daily_fraud_kpis') }}
group by txn_day
