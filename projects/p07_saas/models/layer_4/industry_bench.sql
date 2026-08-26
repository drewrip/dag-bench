select industry, count(distinct account_id) as accounts,
    round(cast(avg(arr) as numeric(38,10)), 2) as avg_arr, round(cast(avg(composite_health) as numeric(38,10)), 2) as avg_health,
    sum(arr) as total_arr
from {{ ref('account_health') }}
group by industry
