with cat_rev as (
    select pp.category_id, pp.category_name,
        sum(pp.net_revenue)   as revenue,
        sum(pp.gross_profit)  as gp,
        sum(pp.units_sold)    as units
    from {{ ref('product_performance') }} pp
    group by pp.category_id, pp.category_name
)
select *,
    rank() over (order by revenue desc)                 as revenue_rank,
    round(gp*100.0/nullif(revenue,0),2)                 as gp_pct,
    sum(revenue) over ()                                as total_revenue,
    round(revenue*100.0/nullif(sum(revenue) over(),0),2) as revenue_share_pct
from cat_rev
