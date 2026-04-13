select device_id, count(*) as total_actions, max(log_ts) as last_maintenance,
    count(*) filter (where action='replace_battery') as battery_replacements,
    count(*) filter (where action='repair') as repairs
from {{ ref('stg_maintenance') }}
group by device_id
