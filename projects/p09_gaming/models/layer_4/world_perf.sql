select world, sum(total_completions) as completions, sum(total_failures) as failures,
    round(cast(avg(avg_cr) as numeric(38,10)), 2) as avg_cr, sum(levels) as level_count
from {{ ref('world_difficulty') }}
group by world
