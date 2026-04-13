select us.plan_type, us.members, us.plan_paid, us.pmpm, us.high_cost_pct,
    se.avg_denial_rate as platform_avg_denial_rate,
    se.avg_pay_rate,
    current_timestamp as report_ts
from {{ ref('utilization_summary') }} us
cross join (
    select round(avg(avg_denial_rate),2) as avg_denial_rate,
           round(avg(avg_pay_rate),4) as avg_pay_rate
    from {{ ref('specialty_efficiency') }}
) se
order by us.pmpm desc
