select merchant_id, merchant_name, merchant_category, risk_tier,
    count(*) as total_txns, sum(amount) as total_volume,
    count(*) filter (where is_flagged) as flagged_txns,
    round(count(*) filter (where is_flagged)*100.0/nullif(count(*),0),3) as flag_rate_pct,
    round(avg(amount),2) as avg_txn, max(amount) as max_txn
from {{ ref('txn_enriched') }}
group by merchant_id, merchant_name, merchant_category, risk_tier
