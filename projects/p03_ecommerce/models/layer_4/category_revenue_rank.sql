with cat_rev as (
    select category_id, category_name,
        cast(sum(net_revenue) as numeric(18,2)) as revenue, cast(sum(gross_profit) as numeric(18,2)) as gp, cast(sum(units_sold) as bigint) as units
    from {{ ref('product_performance') }}
    group by category_id, category_name
)
select *,
    rank() over (order by revenue desc nulls last) as revenue_rank,
    cast(round(gp*100.0/nullif(revenue,0), 2) as numeric(10,2)) as gp_pct,
    cast(round(revenue*100.0/nullif(sum(revenue) over(),0), 2) as numeric(10,2)) as revenue_share_pct
from cat_rev cr
where units > 0
