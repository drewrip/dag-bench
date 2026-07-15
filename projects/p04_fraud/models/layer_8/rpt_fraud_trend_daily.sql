select txn_day, daily_flagged, daily_confirmed, daily_volume,
    prev_day_flagged,
    round((daily_flagged-prev_day_flagged)*100.0/nullif(prev_day_flagged,0),2) as flagged_chg_pct,
    cumulative_fraud,
    current_timestamp as report_ts
from {{ ref('fraud_trend') }}
order by txn_day
