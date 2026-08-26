select te.txn_day, te.merchant_category,
    count(*) as txns,
    count(*) filter (where te.is_flagged) as flagged,
    count(*) filter (where ae.is_confirmed_fraud) as confirmed_fraud,
    round(cast(sum(te.amount) as numeric(38,10)), 2) as volume,
    round(cast(avg(te.amount) as numeric(38,10)), 2) as avg_amount
from {{ ref('txn_enriched') }} te
left join {{ ref('alert_enriched') }} ae using (txn_id)
group by te.txn_day, te.merchant_category
