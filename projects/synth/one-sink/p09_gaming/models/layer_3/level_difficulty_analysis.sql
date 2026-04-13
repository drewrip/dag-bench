select world, difficulty,
    count(distinct level_id)                       as level_count,
    round(avg(completion_rate),2)                  as avg_completion_rate,
    round(avg(unique_players),1)                   as avg_players_per_level,
    sum(completions)                               as total_completions,
    sum(failures)                                  as total_failures,
    round(avg(par_time_sec),0)                     as avg_par_time_sec
from {{ ref('level_completion_stats') }}
group by world, difficulty
