select category, supplier_name, score, delivery_rate, avg_fill_rate, risk_status,
    current_timestamp as report_ts
from {{ ref('supply_chain_view') }}
order by category, score desc
