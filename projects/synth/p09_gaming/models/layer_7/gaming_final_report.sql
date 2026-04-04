select world, monetization_tier,
    world_completions, world_avg_cr,
    round(segment_revenue,2)  as segment_revenue,
    round(platform_revenue,2) as total_platform_revenue,
    round(segment_revenue*100.0/nullif(platform_revenue,0),2) as revenue_share_pct,
    current_timestamp         as report_ts
from {{ ref('game_health_summary') }}
order by world, monetization_tier
