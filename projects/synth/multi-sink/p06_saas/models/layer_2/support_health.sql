select account_id, count(*) as total_tickets,
    count(*) filter (where is_urgent) as urgent_tickets,
    count(*) filter (where is_resolved) as resolved,
    round(CAST((avg(csat_score) filter (where csat_score is not null)) AS NUMERIC),2) as avg_csat,
    round(CAST((avg(ttr_hours) filter (where is_resolved)) AS NUMERIC),1) as avg_ttr
from {{ ref('stg_tickets') }}
group by account_id
