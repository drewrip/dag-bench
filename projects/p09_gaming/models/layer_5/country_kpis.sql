select country, count(distinct player_id) as players,
    sum(revenue) as total_revenue, round(cast(avg(revenue) as numeric(38,10)), 2) as arpu,
    round(cast(avg(active_days) as numeric(38,10)), 2) as avg_active_days,
    count(*) filter (where is_monetized) as paying,
    round(cast(count(*) filter (where is_monetized)*100.0/nullif(count(*),0) as numeric(38,10)), 2) as conversion_pct
from {{ ref('player_profile') }}
group by country
