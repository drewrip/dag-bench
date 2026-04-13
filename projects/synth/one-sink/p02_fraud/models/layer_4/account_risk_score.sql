select account_id, account_country, account_type,
    flag_rate_pct, avg_daily_txns, risky_merchant_txns, total_txns,
    -- composite risk score 0-100
    least(100, round(
        flag_rate_pct * 3
        + least(max_daily_txns, 50) * 0.5
        + risky_merchant_txns * 2
        + case when account_age_days < 90 then 10 else 0 end
    ,2)) as risk_score,
    case
        when flag_rate_pct > 10 or risky_merchant_txns > 5 then 'HIGH'
        when flag_rate_pct > 3  or risky_merchant_txns > 2 then 'MEDIUM'
        else 'LOW'
    end as risk_band
from {{ ref('account_risk_profile') }}
