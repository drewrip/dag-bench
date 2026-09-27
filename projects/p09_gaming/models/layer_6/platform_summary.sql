select platform, monetization_tier, count(distinct player_id) as players,
    cast(sum(revenue) as numeric(18,2)) as revenue, cast(round(avg(active_days), 2) as numeric(10,2)) as avg_active_days,
    cast(round(avg(completion_rate), 2) as numeric(10,2)) as avg_cr
from {{ ref('player_segments') }}
group by platform, monetization_tier
