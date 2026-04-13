select provider_id, provider_name, specialty, is_in_network,
    claims, billed, paid, denial_rate, pay_rate,
    rank() over (partition by specialty order by denial_rate) as specialty_rank,
    current_timestamp as report_ts
from {{ ref('provider_perf') }}
order by denial_rate
