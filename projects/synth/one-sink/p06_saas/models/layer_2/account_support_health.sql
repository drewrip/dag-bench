select account_id,
    count(*)                                    as total_tickets,
    count(*) filter (where is_urgent)           as urgent_tickets,
    count(*) filter (where is_resolved)         as resolved_tickets,
    round(avg(csat_score) filter (where csat_score is not null),2) as avg_csat,
    round(avg(ttr_hours) filter (where is_resolved),1) as avg_ttr_hours
from {{ ref('stg_tickets') }}
group by account_id
