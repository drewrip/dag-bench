select crs.churn_risk_band,
    count(distinct crs.account_id) as accounts,
    round(avg(crs.arr),2)          as avg_arr,
    sum(crs.arr)                   as total_arr,
    round(avg(crs.composite_health),2) as avg_health,
    round(avg(crs.active_days),1)  as avg_active_days,
    ib.avg_health                  as industry_avg_health
from {{ ref('churn_risk_scoring') }} crs
left join {{ ref('industry_benchmarks') }} ib using (industry)
group by crs.churn_risk_band, ib.avg_health
