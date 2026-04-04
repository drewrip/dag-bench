select world,
    sum(total_completions)             as world_completions,
    sum(total_failures)                as world_failures,
    round(avg(avg_completion_rate),2)  as avg_completion_rate,
    sum(level_count)                   as level_count
from {{ ref('level_difficulty_analysis') }}
group by world
