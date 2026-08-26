select region, inventory, available, reorder_alerts, avg_utilization,
    round(cast(available*100.0/nullif(inventory,0) as numeric(38,10)), 2) as availability_pct,
    current_timestamp as report_ts
from {{ ref('region_health') }}
order by reorder_alerts desc nulls last
