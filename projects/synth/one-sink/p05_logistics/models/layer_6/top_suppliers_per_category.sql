select *,
    row_number() over (partition by category order by composite_score desc) as rank_in_cat
from {{ ref('supplier_scorecard') }}
