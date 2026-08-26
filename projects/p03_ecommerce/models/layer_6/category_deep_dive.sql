select tp.category_id, tp.category_name, tp.category_revenue_rank,
    count(*) as top10_products,
    round(cast(sum(tp.net_revenue) as numeric(38,10)), 2) as top10_revenue,
    round(cast(avg(tp.avg_rating) as numeric(38,10)), 2) as avg_rating,
    round(cast(avg(tp.pos_pct) as numeric(38,10)), 2) as avg_pos_pct
from {{ ref('top_products_by_category') }} tp
group by tp.category_id, tp.category_name, tp.category_revenue_rank
