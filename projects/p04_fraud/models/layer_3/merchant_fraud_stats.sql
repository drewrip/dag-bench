select merchant_id, merchant_name, merchant_category, risk_tier,
    count(*) as total_txns, cast(sum(amount) as numeric(18,2)) as total_volume,
    count(*) filter (where is_flagged) as flagged_txns,
    cast(round(count(*) filter (where is_flagged)*100.0/nullif(count(*),0), 3) as numeric(10,3)) as flag_rate_pct,
    cast(round(avg(amount), 2) as numeric(18,2)) as avg_txn, cast(max(amount) as numeric(12,2)) as max_txn
from {{ ref('txn_enriched') }}
group by merchant_id, merchant_name, merchant_category, risk_tier
having count(*) >= 10
