with ranked as (
    select pp.*,
        row_number() over (
            partition by pp.category_id
            order by pp.net_revenue desc
        ) as rank_in_category
    from {{ ref('product_performance') }} pp
)
select r.product_id, r.category_id, r.category_name,
    r.net_revenue, r.gross_profit, r.units_sold,
    r.avg_rating, r.pos_pct, r.rank_in_category,
    cr.revenue_rank       as category_revenue_rank,
    cr.revenue_share_pct  as category_revenue_share
from ranked r
join {{ ref('category_revenue_rank') }} cr using (category_id)
where r.rank_in_category <= 10
