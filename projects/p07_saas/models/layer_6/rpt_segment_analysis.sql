select risk_band, accounts, total_arr, avg_health,
    round(cast(total_arr*100.0/nullif(sum(total_arr) over(),0) as numeric(38,10)), 2) as arr_share_pct,
    industry_avg_arr, current_timestamp as report_ts
from {{ ref('segment_summary') }}
order by avg_health
