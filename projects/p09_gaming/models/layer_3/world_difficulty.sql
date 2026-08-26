select world, difficulty, count(distinct level_id) as levels,
    round(cast(avg(completion_rate) as numeric(38,10)), 2) as avg_cr, sum(completions) as total_completions,
    sum(failures) as total_failures
from {{ ref('level_stats') }}
group by world, difficulty
