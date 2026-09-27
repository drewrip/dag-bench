select world, difficulty, count(distinct level_id) as levels,
    cast(round(avg(completion_rate), 2) as numeric(10,2)) as avg_cr, cast(sum(completions) as bigint) as total_completions,
    cast(sum(failures) as bigint) as total_failures
from {{ ref('level_stats') }}
group by world, difficulty
