select al.alert_id, al.alert_type, al.severity, al.is_confirmed_fraud, al.resolved,
    t.txn_id, t.amount, t.account_id, t.account_country,
    t.merchant_category, t.risk_tier, t.is_large
from {{ ref('stg_alerts') }} al
join {{ ref('txn_enriched') }} t using (txn_id)
where exists (
    select 1 from {{ ref('stg_accounts') }} a
    where a.account_id = t.account_id and not a.is_frozen
)
