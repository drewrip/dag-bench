select category,
    count(distinct supplier_id)   as supplier_count,
    round(avg(composite_score),2) as avg_composite_score,
    max(composite_score)          as best_score,
    min(composite_score)          as worst_score
from {{ ref('supplier_scorecard') }}
group by category
