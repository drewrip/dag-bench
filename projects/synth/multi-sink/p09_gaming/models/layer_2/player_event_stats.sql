select player_id, count(*) as events,
    count(*) filter (where is_completion) as completions,
    count(*) filter (where is_failure) as failures,
    count(distinct level_id) as levels_touched,
    round(count(*) filter (where is_completion)*100.0
          /nullif(count(*) filter (where is_completion or is_failure),0),2) as completion_rate
from {{ ref('stg_events') }}
group by player_id
