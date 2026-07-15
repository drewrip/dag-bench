select world, sum(total_completions) as completions, sum(total_failures) as failures,
    round(avg(avg_cr),2) as avg_cr, sum(levels) as level_count
from {{ ref('world_difficulty') }}
group by world
