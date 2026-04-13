select ar.account_id, ar.account_country, ar.risk_band, ar.risk_score,
    ar.total_txns, ar.flag_rate_pct,
    rp.total_spend, rp.avg_daily_spend
from {{ ref('account_risk_score') }} ar
join {{ ref('account_risk_profile') }} rp using (account_id)
where ar.risk_band = 'HIGH'
