select plan_type, members, paid, pmpm, high_cost_members, high_cost_pct,
    current_timestamp as report_ts
from {{ ref('utilization') }}
order by pmpm desc
