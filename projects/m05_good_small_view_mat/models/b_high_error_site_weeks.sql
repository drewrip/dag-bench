-- Every consumer reports the full profile, so no aggregate of A can be pruned from an
-- inlined plan.
select site_id, ts_week, reading_count, active_devices, active_hours, error_count, min_battery,
    avg_temp, median_temp, p95_temp, stddev_temp, stddev_humidity, stddev_pressure,
    cast(round(error_count * 100.0 / reading_count, 3) as numeric(10,3)) as error_rate_pct
from {{ ref('a_site_weekly_profile') }}
where reading_count >= 50
  and error_count * 100.0 / reading_count > 5
