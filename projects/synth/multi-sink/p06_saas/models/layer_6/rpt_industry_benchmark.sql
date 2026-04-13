select industry, accounts, avg_arr, avg_health, total_arr,
    rank() over (order by total_arr desc) as arr_rank,
    current_timestamp as report_ts
from {{ ref('industry_bench') }}
order by total_arr desc
