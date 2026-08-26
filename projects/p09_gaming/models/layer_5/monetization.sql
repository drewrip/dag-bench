select monetization_tier, engagement_tier, count(distinct player_id) as players,
    sum(revenue) as revenue, round(cast(avg(sessions) as numeric(38,10)), 1) as avg_sessions,
    round(cast(avg(active_days) as numeric(38,10)), 1) as avg_active_days
from {{ ref('player_segments') }}
group by monetization_tier, engagement_tier
