select lf.product_id, lf.category_id, lf.category_name,
    count(distinct lf.order_id) as orders_containing,
    sum(lf.quantity) as units_sold,
    round(cast(sum(lf.disc_revenue) as numeric(38,10)), 2) as net_revenue,
    round(cast(sum(lf.line_gross_profit) as numeric(38,10)), 2) as gross_profit,
    round(cast(avg(lf.margin_pct)*100 as numeric(38,10)), 2) as avg_margin_pct,
    r.review_count, r.avg_rating, r.pos_pct
from {{ ref('order_line_facts') }} lf
left join {{ ref('product_review_agg') }} r using (product_id)
group by lf.product_id,lf.category_id,lf.category_name,r.review_count,r.avg_rating,r.pos_pct
having sum(lf.quantity) > 0
