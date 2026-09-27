select monetization_tier, engagement_tier, count(distinct player_id) as players,
    cast(sum(revenue) as numeric(18,2)) as revenue, cast(round(avg(sessions), 1) as numeric(10,1)) as avg_sessions,
    cast(round(avg(active_days), 1) as numeric(10,1)) as avg_active_days
from {{ ref('player_segments') }}
group by monetization_tier, engagement_tier
