select account_id, count(*) as total_tickets,
    count(*) filter (where is_urgent) as urgent_tickets,
    count(*) filter (where is_resolved) as resolved,
    cast(round(avg(csat_score) filter (where csat_score is not null), 2) as numeric(10,2)) as avg_csat,
    cast(round(cast(avg(ttr_hours) filter (where is_resolved) as numeric(18,3)), 1) as numeric(10,1)) as avg_ttr
from {{ ref('stg_tickets') }}
group by account_id
having count(*) filter (where is_urgent) < count(*)
