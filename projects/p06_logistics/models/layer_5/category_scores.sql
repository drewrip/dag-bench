select category, count(distinct supplier_id) as suppliers,
    cast(round(avg(score), 2) as numeric(10,2)) as avg_score, cast(max(score) as numeric(10,2)) as best, cast(min(score) as numeric(10,2)) as worst
from {{ ref('supplier_scorecard') }}
group by category
