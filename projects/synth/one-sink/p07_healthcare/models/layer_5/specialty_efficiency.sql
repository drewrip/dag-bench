select specialty,
    count(distinct provider_id)             as providers,
    sum(total_claims)                       as claims,
    round(avg(denial_rate_pct),2)           as avg_denial_rate,
    round(avg(pay_rate),4)                  as avg_pay_rate,
    sum(total_paid)                         as total_paid,
    rank() over (order by avg(denial_rate_pct)) as efficiency_rank
from {{ ref('provider_performance') }}
group by specialty
