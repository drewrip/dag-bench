select merchant_category,
    count(distinct merchant_id) as merchants,
    round(cast(avg(flag_rate_pct) as numeric(38,10)), 3) as avg_flag_rate,
    sum(flagged_txns) as total_flagged,
    sum(total_volume) as total_volume,
    max(risk_quartile) as max_risk_quartile
from {{ ref('merchant_risk_rank') }}
group by merchant_category
