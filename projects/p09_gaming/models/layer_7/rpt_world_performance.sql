select world, completions, failures, avg_cr, level_count,
    round(completions*100.0/nullif(completions+failures,0),2) as global_cr,
    rank() over (order by avg_cr desc) as difficulty_rank,
    current_timestamp as report_ts
from {{ ref('world_perf') }}
order by avg_cr desc
