select oi.item_id, oi.order_id, oi.product_id, oi.quantity,
    oi.unit_price, oi.line_total,
    o.customer_id, o.order_date, o.status, o.channel,
    o.discount_pct, o.shipping_cost, o.order_year, o.order_month,
    o.is_fulfilled, p.category_id, p.category_name, p.margin_pct,
    p.cost                                                   as product_cost,
    round(oi.quantity * p.cost, 2)                           as line_cost,
    round(oi.line_total*(1 - o.discount_pct/100.0), 2)       as disc_revenue,
    round(oi.line_total - oi.quantity*p.cost, 2)             as line_gross_profit
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }}   o using (order_id)
join {{ ref('stg_products') }} p using (product_id)
