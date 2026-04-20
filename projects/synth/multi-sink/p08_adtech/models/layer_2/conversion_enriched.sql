select cv.conv_id, cv.campaign_id, cv.user_id, cv.conv_ts, cv.conv_type,
    cv.revenue, cv.conv_day,
    cl.click_day, cl.device, cl.geo,
    {{ datediff("cl.click_ts", "cv.conv_ts", "hour") }} as hours_to_convert
from {{ ref('stg_conversions') }} cv
left join {{ ref('click_enriched') }} cl using (click_id)
