select industry, count(distinct account_id) as accounts,
    cast(round(avg(arr), 2) as numeric(18,2)) as avg_arr, cast(round(avg(composite_health), 2) as numeric(10,2)) as avg_health,
    cast(sum(arr) as numeric(18,2)) as total_arr
from {{ ref('account_health') }}
group by industry
