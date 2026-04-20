select cl.click_id, cl.campaign_id, cl.user_id, cl.click_day, cl.device,
    i.geo, i.placement, i.cost_usd, cl.click_ts,
    {{ datediff("i.imp_ts", "cl.click_ts", "second") }} as ttc_sec
from {{ ref('stg_clicks') }} cl
join {{ ref('stg_impressions') }} i using (imp_id)
