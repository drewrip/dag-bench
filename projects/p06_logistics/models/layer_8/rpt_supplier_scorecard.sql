select supplier_id, supplier_name, category, score, overall_rank,
    delivery_rate, avg_fill_rate, is_preferred, current_timestamp as report_ts
from {{ ref('supplier_scorecard') }}
order by overall_rank
