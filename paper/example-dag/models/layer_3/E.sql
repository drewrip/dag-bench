select
    category,
    region,
    count(*)     as row_count,
    sum(amount)  as total_amount
from {{ ref('C') }}
group by category, region
