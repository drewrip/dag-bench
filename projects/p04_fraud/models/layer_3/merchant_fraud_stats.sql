select merchant_id, merchant_name, merchant_category, risk_tier,
    count(*) as total_txns, sum(amount) as total_volume,
    count(*) filter (where is_flagged) as flagged_txns,
    round(cast(count(*) filter (where is_flagged)*100.0/nullif(count(*),0) as numeric(38,10)), 3) as flag_rate_pct,
    round(cast(avg(amount) as numeric(38,10)), 2) as avg_txn, max(amount) as max_txn
from {{ ref('txn_enriched') }}
group by merchant_id, merchant_name, merchant_category, risk_tier
having count(*) >= 10
