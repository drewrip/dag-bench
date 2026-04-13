select ce.account_country, ce.accounts, ce.avg_risk_score, ce.high_risk_count,
    ft.cumulative_fraud,
    cfp.avg_flag_rate as category_avg_flag_rate
from {{ ref('country_exposure') }} ce
left join (select * from {{ ref('fraud_trend') }} order by txn_day desc limit 1) ft on true
left join (select merchant_category, avg_flag_rate from {{ ref('category_fraud_profile') }}
           order by avg_flag_rate desc limit 1) cfp on true
