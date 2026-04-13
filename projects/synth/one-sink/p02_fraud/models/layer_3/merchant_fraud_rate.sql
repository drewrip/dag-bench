select merchant_id, merchant_name, t.merchant_category, t.risk_tier,
    count(*)                                         as total_txns,
    sum(t.amount)                                      as total_volume,
    count(*) filter (where is_flagged)               as flagged_txns,
    count(*) filter (where is_confirmed_fraud)       as confirmed_fraud_txns,
    round(count(*) filter (where is_flagged)
          *100.0/nullif(count(*),0), 3)              as flag_rate_pct,
    round(avg(t.amount),2)                             as avg_txn_amount,
    max(t.amount)                                      as max_txn_amount
from {{ ref('txn_enriched') }} t
left join {{ ref('alert_txn_join') }} a using (txn_id)
group by merchant_id, merchant_name, t.merchant_category, t.risk_tier
