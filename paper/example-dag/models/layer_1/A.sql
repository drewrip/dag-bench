select
    id,
    name,
    amount
from {{ source('raw', 'S1') }}
