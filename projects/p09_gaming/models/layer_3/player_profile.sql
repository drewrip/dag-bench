with players as (
    select * from {{ ref('stg_players') }}
),

session_stats as (
    select * from {{ ref('player_session_stats') }}
),

event_stats as (
    select * from {{ ref('player_event_stats') }}
),

revenue as (
    select * from {{ ref('player_revenue') }}
)

select p.player_id, p.country, p.platform, p.age_group, p.is_paid_user, p.account_age_days,
    coalesce(ss.sessions,0) as sessions, coalesce(ss.total_sec,0) as playtime_sec,
    coalesce(ss.active_days,0) as active_days,
    coalesce(es.completion_rate,0) as completion_rate,
    coalesce(es.levels_touched,0) as levels_touched,
    coalesce(pr.revenue,0) as revenue, coalesce(pr.purchases,0) as purchases,
    pr.revenue>0 as is_monetized
from players p
left join session_stats ss using (player_id)
left join event_stats es using (player_id)
left join revenue pr using (player_id)
