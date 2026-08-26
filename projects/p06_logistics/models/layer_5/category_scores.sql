select category, count(distinct supplier_id) as suppliers,
    round(cast(avg(score) as numeric(38,10)), 2) as avg_score, max(score) as best, min(score) as worst
from {{ ref('supplier_scorecard') }}
group by category
