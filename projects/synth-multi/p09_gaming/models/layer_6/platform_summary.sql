select platform, monetization_tier, count(distinct player_id) as players,
    sum(revenue) as revenue, round(avg(active_days),2) as avg_active_days,
    round(avg(completion_rate),2) as avg_cr
from {{ ref('player_segments') }}
group by platform, monetization_tier
