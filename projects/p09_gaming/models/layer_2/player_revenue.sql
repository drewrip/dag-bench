select player_id, count(*) filter (where is_valid) as purchases,
    cast(sum(price_usd) filter (where is_valid) as numeric(18,2)) as revenue,
    cast(avg(price_usd) filter (where is_valid) as numeric(38,6)) as avg_purchase,
    count(distinct item_type) as item_types
from {{ ref('stg_purchases') }}
group by player_id
