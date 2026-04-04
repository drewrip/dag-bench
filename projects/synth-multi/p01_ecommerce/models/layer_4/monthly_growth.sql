select *,
    lag(revenue) over (partition by channel order by order_month) as prev_month_rev,
    round((revenue-lag(revenue) over (partition by channel order by order_month))
          *100.0/nullif(lag(revenue) over (partition by channel order by order_month),0),2)
        as mom_growth_pct,
    sum(revenue) over (partition by channel,order_year order by order_month
                       rows unbounded preceding) as ytd_revenue
from {{ ref('monthly_channel_revenue') }}
