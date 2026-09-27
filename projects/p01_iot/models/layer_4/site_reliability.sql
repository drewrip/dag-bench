select site_id, site_name, region,
    count(distinct stat_date)                            as days_with_data,
    cast(round(avg(site_avg_temp), 2) as numeric(10,2)) as overall_avg_temp,
    cast(round(avg(total_valid_readings*1.0
              /nullif(active_devices,0)), 1) as numeric(18,1)) as avg_readings_per_device,
    cast(sum(total_errors) as bigint)                                           as total_errors,
    cast(round(sum(total_errors)*100.0
          /nullif(sum(total_valid_readings),0), 3) as numeric(10,3))   as error_rate_pct
from {{ ref('daily_site_stats') }}
group by site_id, site_name, region
