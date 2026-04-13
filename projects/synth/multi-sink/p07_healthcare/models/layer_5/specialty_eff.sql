select specialty, count(distinct provider_id) as providers,
    sum(claims) as claims, round(avg(denial_rate),2) as avg_denial_rate,
    round(avg(pay_rate),4) as avg_pay_rate, sum(paid) as total_paid,
    rank() over (order by avg(denial_rate)) as eff_rank
from {{ ref('provider_perf') }}
group by specialty
