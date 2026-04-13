select player_id, country, platform, age_group,
    total_sessions, active_days, completion_rate, total_revenue,
    case
        when total_revenue > 50  then 'Whale'
        when total_revenue > 5   then 'Dolphin'
        when total_revenue > 0   then 'Minnow'
        else 'Free'
    end as monetization_tier,
    case
        when active_days >= 20 then 'Power'
        when active_days >= 7  then 'Regular'
        when active_days >= 2  then 'Casual'
        else 'Churned'
    end as engagement_tier,
    ntile(5) over (order by total_playtime_sec desc) as playtime_quintile
from {{ ref('player_profile') }}
