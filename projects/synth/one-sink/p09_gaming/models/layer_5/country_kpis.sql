select country,
    count(distinct player_id)               as player_count,
    sum(total_revenue)                      as total_revenue,
    round(avg(total_revenue),2)             as arpu,
    round(avg(active_days),2)               as avg_active_days,
    count(*) filter (where is_monetized)    as paying_players,
    round(count(*) filter (where is_monetized)
          *100.0/nullif(count(*),0),2)      as conversion_rate_pct
from {{ ref('player_profile') }}
group by country
