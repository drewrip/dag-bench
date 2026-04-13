select merchant_id, merchant_name, merchant_category, risk_tier,
    flag_rate_pct, total_volume, rank_in_cat, risk_quartile,
    current_timestamp as report_ts
from {{ ref('merchant_risk_rank') }}
where risk_quartile=1
order by flag_rate_pct desc
