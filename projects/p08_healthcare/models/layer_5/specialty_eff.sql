select specialty, count(distinct provider_id) as providers,
    sum(claims) as claims, round(cast(avg(denial_rate) as numeric(38,10)), 2) as avg_denial_rate,
    round(cast(avg(pay_rate) as numeric(38,10)), 4) as avg_pay_rate, sum(paid) as total_paid,
    rank() over (order by avg(denial_rate)) as eff_rank
from {{ ref('provider_perf') }}
group by specialty
order by eff_rank, specialty
limit 10
