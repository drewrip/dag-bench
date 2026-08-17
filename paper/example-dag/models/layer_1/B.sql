select
    id,
    category,
    region,
    event_ts
from {{ source('raw', 'S2') }}
