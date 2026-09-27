select p.product_id, p.category_id, p.sku, p.name as product_name,
    p.price, p.cost, p.weight_kg, p.is_active, p.stock_qty,
    cast(round(p.price-p.cost, 2) as numeric(10,2)) as gross_margin,
    cast(round((p.price-p.cost)/nullif(p.price,0), 4) as numeric(10,4)) as margin_pct,
    c.name as category_name
from {{ source('raw','products') }} p
left join {{ source('raw','categories') }} c using (category_id)
