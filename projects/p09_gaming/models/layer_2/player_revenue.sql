select player_id, {{ dagbench_agg_filter('count', '*', 'is_valid') }} as purchases,
    cast({{ dagbench_agg_filter('sum', 'price_usd', 'is_valid') }} as numeric(18,2)) as revenue,
    cast({{ dagbench_agg_filter('avg', 'price_usd', 'is_valid') }} as numeric(38,6)) as avg_purchase,
    count(distinct item_type) as item_types
from {{ ref('stg_purchases') }}
group by player_id
