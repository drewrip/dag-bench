select site_id, site_name, region,
    count(distinct stat_date)                            as days_with_data,
    round(cast(avg(site_avg_temp) as numeric(38,10)), 2) as overall_avg_temp,
    round(cast(avg(total_valid_readings*1.0
              /nullif(active_devices,0)) as numeric(38,10)), 1) as avg_readings_per_device,
    sum(total_errors)                                           as total_errors,
    round(cast(sum(total_errors)*100.0
          /nullif(sum(total_valid_readings),0) as numeric(38,10)), 3)   as error_rate_pct
from {{ ref('daily_site_stats') }}
group by site_id, site_name, region
