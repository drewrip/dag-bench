select *,
    rank() over (partition by merchant_category order by flag_rate_pct desc) as rank_in_cat,
    ntile(4) over (order by flag_rate_pct desc) as risk_quartile
from {{ ref('merchant_fraud_stats') }}
