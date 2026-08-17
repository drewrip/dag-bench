select
    a.id,
    a.name,
    a.amount,
    b.category,
    b.region,
    b.event_ts
from {{ ref('A') }} a
join {{ ref('B') }} b using (id)
