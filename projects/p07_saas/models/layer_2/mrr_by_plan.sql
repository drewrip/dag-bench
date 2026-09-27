select plan, is_active, count(distinct account_id) as accounts,
    cast(sum(mrr) as numeric(18,2)) as total_mrr, cast(round(avg(mrr), 2) as numeric(18,2)) as avg_mrr, sum(seats) as seats
from {{ ref('stg_subscriptions') }}
group by plan, is_active
