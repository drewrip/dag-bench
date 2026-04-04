select cd.category_id, cd.category_name, cd.category_revenue_rank,
    cd.top10_revenue, cd.avg_rating, cd.avg_pos_pct,
    current_timestamp as generated_at
from {{ ref('category_deep_dive') }} cd
order by cd.category_revenue_rank
