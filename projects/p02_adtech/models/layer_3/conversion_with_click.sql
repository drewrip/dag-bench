-- Only interested in completed purchases from the first 6 months of the launch cohort,
-- not leads/signups/installs/subscriptions.
select cv.conv_id, cv.campaign_id, cv.user_id, cv.conv_ts,
    cv.conv_type, cv.revenue, cv.conv_day,
    cl.click_day, cl.device, cl.geo,
    {{ datediff("cl.click_ts", "cv.conv_ts", "hour") }} as hours_to_convert
from {{ ref('stg_conversions') }} cv
left join {{ ref('click_attribution') }} cl using (click_id)
where cv.conv_type = 'purchase'
  and cv.conv_day >= date '2023-01-01' and cv.conv_day < date '2023-07-01'
