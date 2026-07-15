select ar.account_country,
    count(distinct account_id) as accounts,
    avg(risk_score) as avg_risk_score,
    count(*) filter (where risk_band='HIGH') as high_risk_count,
    sum(total_txns) as total_txns
from {{ ref('account_risk_score') }} ar
join {{ ref('account_risk_profile') }} rp using (account_id)
group by ar.account_country
