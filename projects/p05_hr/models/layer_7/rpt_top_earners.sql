select emp_id, full_name, dept_name, division, total_comp,
    round(cast(avg_perf_score as numeric(38,10)), 2) as avg_perf, tenure_years, rank_in_division,
    current_timestamp as report_ts
from {{ ref('top_earners') }}
where rank_in_division<=5
order by division, rank_in_division
