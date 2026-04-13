select world, difficulty, count(distinct level_id) as levels,
    round(avg(completion_rate),2) as avg_cr, sum(completions) as total_completions,
    sum(failures) as total_failures
from {{ ref('level_stats') }}
group by world, difficulty
