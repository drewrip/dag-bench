select *,
    cast(lag(revenue) over (partition by channel order by order_year, order_month) as numeric(18,2)) as prev_month_rev,
    cast(round((revenue-lag(revenue) over (partition by channel order by order_year, order_month))
          *100.0/nullif(lag(revenue) over (partition by channel order by order_year, order_month),0), 2) as numeric(38,2))
        as mom_growth_pct,
    cast(sum(revenue) over (partition by channel,order_year order by order_month
                       rows unbounded preceding) as numeric(18,2)) as ytd_revenue
from {{ ref('monthly_channel_revenue') }}
