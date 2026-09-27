select merchant_category,
    count(distinct merchant_id) as merchants,
    cast(round(avg(flag_rate_pct), 3) as numeric(10,3)) as avg_flag_rate,
    cast(sum(flagged_txns) as bigint) as total_flagged,
    cast(sum(total_volume) as numeric(18,2)) as total_volume,
    max(risk_quartile) as max_risk_quartile
from {{ ref('merchant_risk_rank') }}
group by merchant_category
