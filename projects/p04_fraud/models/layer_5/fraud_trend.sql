with bounds as (

    select max(txn_day) as max_txn_day from {{ ref('daily_fraud_kpis') }}

)

select txn_day,
    cast(sum(flagged) as bigint) as daily_flagged, cast(sum(confirmed_fraud) as bigint) as daily_confirmed,
    cast(sum(volume) as numeric(18,2)) as daily_volume,
    cast(lag(sum(flagged)) over (order by txn_day) as bigint) as prev_day_flagged,
    cast(sum(sum(confirmed_fraud)) over (order by txn_day rows unbounded preceding) as bigint) as cumulative_fraud
from {{ ref('daily_fraud_kpis') }}
cross join bounds
where txn_day >= bounds.max_txn_day - interval '90 days'
group by txn_day, bounds.max_txn_day
