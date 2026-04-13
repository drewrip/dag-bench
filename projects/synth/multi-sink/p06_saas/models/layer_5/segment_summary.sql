select cr.risk_band, count(distinct cr.account_id) as accounts,
    round(avg(cr.arr),2) as avg_arr, sum(cr.arr) as total_arr,
    round(avg(cr.composite_health),2) as avg_health,
    ib.avg_arr as industry_avg_arr
from {{ ref('churn_risk') }} cr
left join {{ ref('industry_bench') }} ib using (industry)
group by cr.risk_band, ib.avg_arr
