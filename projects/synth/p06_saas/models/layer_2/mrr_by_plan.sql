select plan, is_active,
    count(distinct account_id) as accounts,
    sum(mrr)                   as total_mrr,
    round(avg(mrr),2)          as avg_mrr,
    sum(seats)                 as total_seats,
    round(avg(seats),1)        as avg_seats
from {{ ref('stg_subscriptions') }}
group by plan, is_active
