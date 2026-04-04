select industry, count(distinct account_id) as accounts,
    round(avg(arr),2) as avg_arr, round(avg(composite_health),2) as avg_health,
    sum(arr) as total_arr
from {{ ref('account_health') }}
group by industry
