select monetization_tier, engagement_tier,
    count(distinct player_id) as players,
    sum(total_revenue)        as segment_revenue,
    round(avg(total_sessions),1) as avg_sessions,
    round(avg(active_days),1)    as avg_active_days,
    round(avg(completion_rate),2) as avg_completion_rate
from {{ ref('player_segments') }}
group by monetization_tier, engagement_tier
