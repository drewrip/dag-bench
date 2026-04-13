select player_id,
    count(*) filter (where is_valid)              as purchases,
    sum(price_usd) filter (where is_valid)        as total_revenue,
    avg(price_usd) filter (where is_valid)        as avg_purchase_value,
    count(distinct item_type)                     as item_types_bought,
    max(purchase_ts)                              as last_purchase_ts
from {{ ref('stg_purchases') }}
group by player_id
