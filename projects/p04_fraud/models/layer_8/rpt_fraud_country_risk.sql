select account_country, accounts, avg_risk_score, high_risk_count,
    round(high_risk_count*100.0/nullif(accounts,0),2) as pct_high_risk,
    current_timestamp as report_ts
from {{ ref('country_exposure') }}
order by avg_risk_score desc
