-- Scoped to the US market for the first 6 months of the launch cohort.
select cl.click_id, cl.campaign_id, cl.user_id, cl.click_day, cl.device,
    i.geo, i.placement, i.cost_usd as impression_cost, cl.click_ts,
    {{ datediff("i.imp_ts", "cl.click_ts", "second") }} as time_to_click_sec
from {{ ref('stg_clicks') }} cl
join {{ ref('stg_impressions') }} i using (imp_id)
where i.geo = 'US'
  and cl.click_day >= date '2023-01-01' and cl.click_day < date '2023-07-01'
  and exists (
    select 1 from {{ ref('stg_conversions') }} cv where cv.click_id = cl.click_id
  )
