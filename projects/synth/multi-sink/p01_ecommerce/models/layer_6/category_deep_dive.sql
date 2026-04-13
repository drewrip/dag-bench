select tp.category_id, tp.category_name, tp.category_revenue_rank,
    count(*) as top10_products,
    round(sum(tp.net_revenue),2) as top10_revenue,
    round(avg(tp.avg_rating),2) as avg_rating,
    round(avg(tp.pos_pct),2) as avg_pos_pct
from {{ ref('top_products_by_category') }} tp
group by tp.category_id, tp.category_name, tp.category_revenue_rank
