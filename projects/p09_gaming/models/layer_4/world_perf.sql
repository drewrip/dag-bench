select world, cast(sum(total_completions) as bigint) as completions, cast(sum(total_failures) as bigint) as failures,
    cast(round(avg(avg_cr), 2) as numeric(10,2)) as avg_cr, cast(sum(levels) as bigint) as level_count
from {{ ref('world_difficulty') }}
group by world
