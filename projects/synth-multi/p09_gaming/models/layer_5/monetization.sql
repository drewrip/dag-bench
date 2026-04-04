select monetization_tier, engagement_tier, count(distinct player_id) as players,
    sum(revenue) as revenue, round(avg(sessions),1) as avg_sessions,
    round(avg(active_days),1) as avg_active_days
from {{ ref('player_segments') }}
group by monetization_tier, engagement_tier
