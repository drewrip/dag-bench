select plan, is_active, accounts, total_mrr, avg_mrr, seats,
    round(total_mrr*100.0/nullif(sum(total_mrr) over(partition by is_active),0),2) as mrr_share_pct,
    current_timestamp as report_ts
from {{ ref('mrr_by_plan') }}
order by is_active desc, total_mrr desc
