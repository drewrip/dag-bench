select account_country,
    count(distinct account_id)               as accounts,
    avg(risk_score)                          as avg_risk_score,
    count(*) filter (where risk_band='HIGH') as high_risk_accounts,
    sum(total_txns)                          as total_txns,
    sum(flag_rate_pct)/nullif(count(*),0)    as mean_flag_rate
from {{ ref('account_risk_score') }}
group by account_country
