select specialty, count(distinct provider_id) as providers,
    cast(sum(claims) as bigint) as claims, cast(round(avg(denial_rate), 2) as numeric(10,2)) as avg_denial_rate,
    cast(round(avg(pay_rate), 4) as numeric(10,4)) as avg_pay_rate, cast(sum(paid) as numeric(18,2)) as total_paid,
    rank() over (order by avg(denial_rate)) as eff_rank
from {{ ref('provider_perf') }}
group by specialty
order by eff_rank, specialty
limit 10
