select merchant_category, merchants, avg_flag_rate, total_flagged,
    total_volume, max_risk_quartile,
    round(total_flagged*100.0/nullif(sum(total_flagged) over(),0),2) as share_of_fraud_pct,
    current_timestamp as report_ts
from {{ ref('category_fraud_profile') }}
order by avg_flag_rate desc
