select al.alert_id, al.alert_type, al.severity, al.is_confirmed_fraud,
    al.resolved, t.txn_id, t.amount, t.account_id, t.channel,
    t.is_large, t.merchant_category, t.merchant_country,
    t.account_country, t.risk_tier
from {{ ref('stg_alerts') }} al
join {{ ref('txn_enriched') }} t using (txn_id)
