select category, risk_status, avg_score, suppliers, reorder_alerts,
    current_timestamp as report_ts
from {{ ref('supply_risk') }}
group by category, risk_status, avg_score, suppliers, reorder_alerts
order by avg_score
