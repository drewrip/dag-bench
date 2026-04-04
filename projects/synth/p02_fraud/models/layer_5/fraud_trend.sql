select txn_day,
    sum(flagged)         as daily_flagged,
    sum(confirmed_fraud) as daily_confirmed,
    sum(volume)          as daily_volume,
    round(avg(flagged*100.0/nullif(txns,0)),3) as avg_flag_rate,
    lag(sum(flagged)) over (order by txn_day)  as prev_day_flagged,
    sum(sum(confirmed_fraud)) over (order by txn_day
        rows unbounded preceding)              as cumulative_fraud
from {{ ref('daily_fraud_summary') }}
group by txn_day
