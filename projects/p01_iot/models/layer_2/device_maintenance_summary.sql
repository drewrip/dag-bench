select device_id,
    count(*)                              as total_actions,
    max(log_ts)                           as last_maintenance_ts,
    count(distinct action)                as distinct_action_types,
    {{ dagbench_agg_filter('count', '*', "action='battery_replacement'") }} as battery_replacements,
    {{ dagbench_agg_filter('count', '*', "action='repair'") }}          as repairs
from {{ ref('stg_maintenance') }}
group by device_id
having count(*) > 0
