select cr.account_country,
    cr.accounts, cr.avg_risk_score, cr.high_risk_accounts,
    ft.daily_flagged as latest_daily_flagged,
    ft.cumulative_fraud,
    mr.flag_rate_pct as top_merchant_flag_rate
from {{ ref('country_risk_matrix') }} cr
left join (select * from {{ ref('fraud_trend') }}
           order by txn_day desc limit 1) ft on true
left join (select account_country, flag_rate_pct
           from {{ ref('merchant_risk_rank') }}
           where rank_in_category = 1) mr
    on mr.account_country = cr.account_country
