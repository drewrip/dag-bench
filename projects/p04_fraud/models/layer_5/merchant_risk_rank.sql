select *,
    rank() over (partition by merchant_category order by flag_rate_pct desc nulls last) as rank_in_cat,
    ntile(4) over (order by flag_rate_pct desc nulls last, merchant_id) as risk_quartile
from {{ ref('merchant_fraud_stats') }}
