select wp.world,
    wp.world_completions, wp.avg_completion_rate as world_avg_cr,
    ma.monetization_tier, ma.segment_revenue,
    ck.total_revenue as platform_revenue
from {{ ref('world_performance') }} wp
cross join (
    select monetization_tier, sum(segment_revenue) as segment_revenue
    from {{ ref('monetization_analysis') }}
    group by monetization_tier
) ma
cross join (
    select sum(total_revenue) as total_revenue
    from {{ ref('country_kpis') }}
) ck
