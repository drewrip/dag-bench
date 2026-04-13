select account_id, account_country, risk_band, risk_score, flag_rate_pct,
    total_spend, total_txns,
    current_timestamp as report_ts
from {{ ref('high_risk_accounts') }}
order by risk_score desc
