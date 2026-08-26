select *,
    lag(revenue) over (partition by channel order by order_year, order_month) as prev_month_rev,
    round(cast((revenue-lag(revenue) over (partition by channel order by order_year, order_month))
          *100.0/nullif(lag(revenue) over (partition by channel order by order_year, order_month),0) as numeric(38,10)), 2)
        as mom_growth_pct,
    sum(revenue) over (partition by channel,order_year order by order_month
                       rows unbounded preceding) as ytd_revenue
from {{ ref('monthly_channel_revenue') }}
