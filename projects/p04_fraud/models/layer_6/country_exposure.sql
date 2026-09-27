select ar.account_country,
    count(distinct account_id) as accounts,
    cast(avg(risk_score) as numeric(38,6)) as avg_risk_score,
    count(*) filter (where risk_band='HIGH') as high_risk_count,
    cast(sum(total_txns) as bigint) as total_txns
from {{ ref('account_risk_score') }} ar
join {{ ref('account_risk_profile') }} rp using (account_id)
group by ar.account_country
