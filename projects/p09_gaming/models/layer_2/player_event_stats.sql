select player_id, count(*) as events,
    {{ dagbench_agg_filter('count', '*', 'is_completion') }} as completions,
    {{ dagbench_agg_filter('count', '*', 'is_failure') }} as failures,
    count(distinct level_id) as levels_touched,
    cast(round({{ dagbench_agg_filter('count', '*', 'is_completion') }}*100.0
          /nullif({{ dagbench_agg_filter('count', '*', 'is_completion or is_failure') }},0), 2) as numeric(10,2)) as completion_rate
from {{ ref('stg_events') }}
group by player_id
