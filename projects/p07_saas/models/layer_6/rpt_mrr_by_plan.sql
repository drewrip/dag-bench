select plan, is_active, accounts, total_mrr, avg_mrr, seats,
    round(cast(total_mrr*100.0/nullif(sum(total_mrr) over(partition by is_active),0) as numeric(38,10)), 2) as mrr_share_pct,
    current_timestamp as report_ts
from {{ ref('mrr_by_plan') }}
order by is_active desc nulls last, total_mrr desc nulls last
