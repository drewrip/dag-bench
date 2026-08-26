select *,
    row_number() over (partition by category order by score desc nulls last, supplier_id) as rank_in_cat
from {{ ref('supplier_scorecard') }}
