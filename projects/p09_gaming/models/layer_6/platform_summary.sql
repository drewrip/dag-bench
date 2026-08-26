select platform, monetization_tier, count(distinct player_id) as players,
    sum(revenue) as revenue, round(cast(avg(active_days) as numeric(38,10)), 2) as avg_active_days,
    round(cast(avg(completion_rate) as numeric(38,10)), 2) as avg_cr
from {{ ref('player_segments') }}
group by platform, monetization_tier
