select category, risk_status,
    count(distinct supplier_name)  as top_suppliers,
    round(avg(composite_score),2)  as avg_supplier_score,
    round(avg(delivery_rate_pct),2) as avg_delivery_rate,
    max(reorder_alerts)            as reorder_alerts,
    current_timestamp              as report_ts
from {{ ref('supply_chain_summary') }}
group by category, risk_status
order by avg_supplier_score desc
