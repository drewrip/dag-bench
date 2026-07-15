with ranked as (
    select pp.*,
        row_number() over (partition by category_id order by net_revenue desc) as rank_in_cat
    from {{ ref('product_performance') }} pp
)
select r.product_id, r.category_id, r.category_name,
    r.net_revenue, r.gross_profit, r.units_sold,
    r.avg_rating, r.pos_pct, r.rank_in_cat,
    cr.revenue_rank as category_revenue_rank
from ranked r
join {{ ref('category_revenue_rank') }} cr using (category_id)
where r.rank_in_cat<=10
