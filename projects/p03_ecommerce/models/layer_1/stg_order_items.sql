select item_id, order_id, product_id, quantity, unit_price,
    cast(quantity*unit_price as numeric(18,2)) as line_total
from {{ source('raw','order_items') }}
