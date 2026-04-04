select e.level_id, lv.world, lv.difficulty, lv.par_time_sec,
    count(*) filter (where e.is_completion)       as completions,
    count(*) filter (where e.is_failure)          as failures,
    count(distinct e.player_id)                   as unique_players,
    round(count(*) filter (where e.is_completion)*100.0
          /nullif(count(*) filter (where e.is_completion or e.is_failure),0),2)
                                                  as completion_rate
from {{ ref('stg_events') }} e
join {{ ref('stg_levels') }} lv using (level_id)
group by e.level_id, lv.world, lv.difficulty, lv.par_time_sec
