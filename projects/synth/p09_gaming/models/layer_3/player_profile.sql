select p.player_id, p.country, p.platform, p.age_group, p.is_paid_user,
    p.account_age_days,
    coalesce(ss.total_sessions,0)        as total_sessions,
    coalesce(ss.total_playtime_sec,0)    as total_playtime_sec,
    coalesce(ss.active_days,0)           as active_days,
    coalesce(es.completion_rate,0)       as completion_rate,
    coalesce(es.unique_levels_touched,0) as levels_touched,
    coalesce(pr.total_revenue,0)         as total_revenue,
    coalesce(pr.purchases,0)             as purchases,
    pr.total_revenue > 0                 as is_monetized
from {{ ref('stg_players') }} p
left join {{ ref('player_session_stats') }} ss using (player_id)
left join {{ ref('player_event_stats') }}   es using (player_id)
left join {{ ref('player_revenue') }}       pr using (player_id)
