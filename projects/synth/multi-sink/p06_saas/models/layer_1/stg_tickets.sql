select ticket_id, account_id, created_ts, resolved_ts, priority, category,
    csat_score, is_resolved,
    {{ datediff("created_ts", "coalesce(resolved_ts,current_timestamp)", "hour") }} as ttr_hours,
    priority in ('high','critical') as is_urgent
from {{ source('saas','support_tickets') }}
