select category_id, category_name,
    count(*) as product_count,
    count(*) filter (where is_active) as active_count,
    round(avg(price),2) as avg_price,
    round(avg(margin_pct)*100,2) as avg_margin_pct,
    sum(stock_qty) as total_stock
from {{ ref('stg_products') }}
group by category_id, category_name
