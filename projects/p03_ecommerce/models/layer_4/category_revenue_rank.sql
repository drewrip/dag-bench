with cat_rev as (
    select category_id, category_name,
        sum(net_revenue) as revenue, sum(gross_profit) as gp, sum(units_sold) as units
    from {{ ref('product_performance') }}
    group by category_id, category_name
)
select *,
    rank() over (order by revenue desc) as revenue_rank,
    round(gp*100.0/nullif(revenue,0),2) as gp_pct,
    round(revenue*100.0/nullif(sum(revenue) over(),0),2) as revenue_share_pct
from cat_rev
