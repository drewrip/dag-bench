select account_country,
    accounts, avg_risk_score, high_risk_accounts,
    latest_daily_flagged, cumulative_fraud, top_merchant_flag_rate,
    round(high_risk_accounts*100.0/nullif(accounts,0),2) as pct_high_risk,
    current_timestamp as report_ts
from {{ ref('fraud_executive_summary') }}
order by avg_risk_score desc
