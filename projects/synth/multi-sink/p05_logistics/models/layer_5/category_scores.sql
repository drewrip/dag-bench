select category, count(distinct supplier_id) as suppliers,
    round(avg(score),2) as avg_score, max(score) as best, min(score) as worst
from {{ ref('supplier_scorecard') }}
group by category
