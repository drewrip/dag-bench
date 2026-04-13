select specialty, providers, claims, avg_denial_rate, avg_pay_rate, total_paid, eff_rank,
    current_timestamp as report_ts
from {{ ref('specialty_eff') }}
order by eff_rank
