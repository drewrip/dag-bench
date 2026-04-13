select site_id, site_name, region,
    count(distinct stat_date)                      as days_with_data,
    round(avg(site_avg_temp),2)                    as overall_avg_temp,
    round(avg(total_valid_readings*1.0
              /nullif(active_devices,0)),1)         as avg_readings_per_device,
    sum(total_errors)                              as total_errors,
    round(sum(total_errors)*100.0
          /nullif(sum(total_valid_readings),0),3)   as error_rate_pct
from {{ ref('daily_site_stats') }}
group by site_id, site_name, region
