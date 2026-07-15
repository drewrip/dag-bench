select player_id, count(*) filter (where is_valid) as purchases,
    sum(price_usd) filter (where is_valid) as revenue,
    avg(price_usd) filter (where is_valid) as avg_purchase,
    count(distinct item_type) as item_types
from {{ ref('stg_purchases') }}
group by player_id
