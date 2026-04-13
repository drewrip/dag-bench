with total_mrr as (
    select sum(total_mrr) as platform_mrr
    from {{ ref('mrr_by_plan') }}
    where is_active
)
select plg.churn_risk_band,
    plg.accounts, plg.total_arr,
    round(plg.total_arr*100.0/sum(plg.total_arr) over(),2) as arr_share_pct,
    plg.avg_health, plg.avg_active_days,
    tm.platform_mrr,
    current_timestamp as report_ts
from {{ ref('product_led_growth') }} plg
cross join total_mrr tm
order by plg.avg_health
