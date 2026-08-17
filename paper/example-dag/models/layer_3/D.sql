select
    id,
    name,
    region,
    amount
from {{ ref('C') }}
where amount > 0
