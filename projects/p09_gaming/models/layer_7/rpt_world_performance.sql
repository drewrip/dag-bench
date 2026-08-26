select world, completions, failures, avg_cr, level_count,
    round(cast(completions*100.0/nullif(completions+failures,0) as numeric(38,10)), 2) as global_cr,
    rank() over (order by avg_cr desc nulls last) as difficulty_rank,
    current_timestamp as report_ts
from {{ ref('world_perf') }}
order by avg_cr desc nulls last
